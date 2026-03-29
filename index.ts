/**
 * Telegram Video Streaming Service
 *
 * Features:
 * - HTTP 206 Partial Content + Range support
 * - DC-aware file fetching (fixes "stored in DC 4" error)
 * - Fixed 128KB aligned chunks (prevents LIMIT_INVALID)
 * - Per-IP rate limiting + concurrent stream limiting
 * - Telegram flood wait / retry logic
 * - File info cache with filename
 * - Demo stream fallback
 * - CORS support
 * - Graceful cancel on browser seek/close
 */

import { serve } from "bun";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions";
import { Api } from "telegram";
import BigInteger from "big-integer";

// ─── Configuration ────────────────────────────────────────────────────────────

const PORT = parseInt(process.env.PORT || "3030");
const MAX_RETRIES = 3;
const RETRY_DELAY = 1000;

const API_ID = parseInt(process.env.TELEGRAM_API_ID || "0");
const API_HASH = process.env.TELEGRAM_API_HASH || "";
const SESSION_STRING = process.env.TELEGRAM_SESSION || "";
const CHANNEL_ID = process.env.TELEGRAM_CHANNEL_ID || "";

if (!API_ID || !API_HASH) {
  console.warn("⚠️  TELEGRAM_API_ID / TELEGRAM_API_HASH not set → demo mode");
}

// ─── Telegram Chunk Rules ─────────────────────────────────────────────────────
// limit must be power-of-2 (4KB–1MB), offset must be multiple of limit.
// Fixed 128KB: floor(pos/131072)*131072 is always valid.
const CHUNK_SIZE = 131072; // 128KB
const MAX_RESPONSE_SIZE = 4 * 1024 * 1024; // 4MB per HTTP response
const DEMO_FILE_SIZE = 10 * 1024 * 1024; // 10MB demo

// ─── Rate Limiting ────────────────────────────────────────────────────────────

const RATE_LIMIT = {
  maxConcurrentStreams: 10,
  requestsPerMinute: 120,   // raised: video players send many range requests
  telegramCallsPerSec: 5,
};

let activeStreams = 0;
const ipRequestLog = new Map<string, number[]>();
let telegramTokens = RATE_LIMIT.telegramCallsPerSec;
let lastTokenRefill = Date.now();

async function acquireTelegramToken(): Promise<void> {
  while (true) {
    const now = Date.now();
    const elapsed = (now - lastTokenRefill) / 1000;
    if (elapsed >= 1) {
      telegramTokens = RATE_LIMIT.telegramCallsPerSec;
      lastTokenRefill = now;
    }
    if (telegramTokens > 0) { telegramTokens--; return; }
    await sleep(100);
  }
}

function checkIpRateLimit(ip: string): boolean {
  const now = Date.now();
  const windowMs = 60_000;
  const times = (ipRequestLog.get(ip) || []).filter(t => now - t < windowMs);
  times.push(now);
  ipRequestLog.set(ip, times);

  // Cleanup stale IPs
  if (ipRequestLog.size > 2000) {
    for (const [k, v] of ipRequestLog)
      if (v.every(t => now - t >= windowMs)) ipRequestLog.delete(k);
  }
  return times.length <= RATE_LIMIT.requestsPerMinute;
}

// ─── Types ────────────────────────────────────────────────────────────────────

interface FileInfo {
  id: bigint;
  accessHash: bigint;
  fileReference: Buffer;
  dcId: number;
  size: number;
  mimeType: string;
  fileName: string;   // ← NEW: original filename from Telegram
  timestamp: number;
}

// ─── Cache ────────────────────────────────────────────────────────────────────

const fileInfoCache = new Map<string, FileInfo>();
const CACHE_TTL = 10 * 60 * 1000;

// ─── Telegram Client Pool ─────────────────────────────────────────────────────
// We need ONE client per DC because Telegram rejects cross-DC file requests.
// When a file is on DC 4, we must use a DC-4 connection to fetch it.

const dcClients = new Map<number, TelegramClient>();
let primaryClient: TelegramClient | null = null;
let connectionPromise: Promise<void> | null = null;

async function getPrimaryClient(): Promise<TelegramClient> {
  if (primaryClient?.connected) return primaryClient;

  if (!connectionPromise) {
    connectionPromise = (async () => {
      const session = new StringSession(SESSION_STRING);
      primaryClient = new TelegramClient(session, API_ID, API_HASH, {
        connectionRetries: 5,
        timeout: 30000,
        useWSS: false,
      });
      await primaryClient.connect();
      console.log("✅ Primary Telegram client connected");
    })().catch(err => {
      connectionPromise = null;
      primaryClient = null;
      throw err;
    });
  }

  await connectionPromise;
  return primaryClient!;
}

/**
 * Get (or create) a Telegram client connected to a specific DC.
 *
 * This is the KEY FIX for "stored in DC 4" error.
 * Telegram requires the request to come from the same DC as the file.
 * gramjs provides borrowDC() / _borrowedSenderCallback for this.
 */
async function getClientForDc(dcId: number): Promise<TelegramClient> {
  const primary = await getPrimaryClient();

  // If file is on the same DC as our primary connection, use primary
  // gramjs auto-detects our DC; we'll borrow a sender for other DCs
  if (dcClients.has(dcId)) {
    return dcClients.get(dcId)!;
  }

  // Create a borrowed connection to the target DC via primary client's network
  // gramjs handles auth key export/import internally
  const borrowed = await (primary as any)._borrowedSenderPool?._getSender(dcId)
    .catch(() => null);

  if (borrowed) {
    // Wrap borrowed sender — but easiest reliable approach is separate client
  }

  // Most reliable: create a separate TelegramClient for this DC
  // using exportAuthorization → importAuthorization
  try {
    const session = new StringSession("");
    const dcClient = new TelegramClient(session, API_ID, API_HASH, {
      connectionRetries: 3,
      timeout: 30000,
      useWSS: false,
      dcId,           // Force connect to specific DC
    });

    await dcClient.connect();

    // Export auth from primary and import to DC client
    const exportedAuth = await primary.invoke(
      new Api.auth.ExportAuthorization({ dcId })
    );

    await dcClient.invoke(
      new Api.auth.ImportAuthorization({
        id: exportedAuth.id,
        bytes: exportedAuth.bytes,
      })
    );

    dcClients.set(dcId, dcClient);
    console.log(`✅ DC${dcId} client ready`);
    return dcClient;
  } catch (err) {
    console.error(`❌ Failed to create DC${dcId} client:`, err);
    // Fallback to primary (may work for some DCs)
    return primary;
  }
}

// ─── File Info ────────────────────────────────────────────────────────────────

/**
 * Extract filename from Telegram document attributes.
 * Videos have DocumentAttributeFilename or DocumentAttributeVideo.
 */
function extractFileName(
  attributes: Api.TypeDocumentAttribute[],
  mimeType: string,
  messageId: string
): string {
  // Try DocumentAttributeFilename first (most reliable)
  for (const attr of attributes) {
    if (attr instanceof Api.DocumentAttributeFilename && attr.fileName) {
      return attr.fileName;
    }
  }

  // Try to build from video attributes
  for (const attr of attributes) {
    if (attr instanceof Api.DocumentAttributeVideo) {
      const ext = mimeType.split("/")[1] || "mp4";
      return `video_${messageId}.${ext}`;
    }
    if (attr instanceof Api.DocumentAttributeAudio) {
      const ext = mimeType.split("/")[1] || "mp3";
      return `audio_${messageId}.${ext}`;
    }
  }

  // Fallback from MIME type
  const ext = mimeType.split("/")[1]?.split(";")[0] || "bin";
  return `file_${messageId}.${ext}`;
}

async function getFileInfo(
  messageId: string,
  channelId: string
): Promise<FileInfo> {
  const cacheKey = `${channelId}:${messageId}`;
  const cached = fileInfoCache.get(cacheKey);
  if (cached && Date.now() - cached.timestamp < CACHE_TTL) return cached;

  const client = await getPrimaryClient();
  const messages = await client.getMessages(channelId, {
    ids: [parseInt(messageId)],
  });

  if (!messages?.length || !messages[0]) {
    throw new Error(`Message ${messageId} not found`);
  }

  const msg = messages[0];

  if (!(msg.media instanceof Api.MessageMediaDocument)) {
    throw new Error(
      `Message ${messageId} is not a document (type: ${msg.media?.className})`
    );
  }

  const doc = msg.media.document;
  if (!(doc instanceof Api.Document)) {
    throw new Error("Invalid document");
  }

  // Parse size
  const rawSize = doc.size;
  let size: number;
  if (typeof rawSize === "bigint") size = Number(rawSize);
  else if (rawSize && typeof rawSize === "object" && "toJSNumber" in rawSize)
    size = (rawSize as any).toJSNumber();
  else size = Number(rawSize);

  const mimeType = doc.mimeType || "video/mp4";
  const fileName = extractFileName(doc.attributes, mimeType, messageId);

  const info: FileInfo = {
    id: doc.id as bigint,
    accessHash: doc.accessHash as bigint,
    fileReference: Buffer.from(doc.fileReference),
    dcId: doc.dcId,
    size,
    mimeType,
    fileName,
    timestamp: Date.now(),
  };

  fileInfoCache.set(cacheKey, info);
  console.log(
    `📁 Cached: msg=${messageId} | "${fileName}" | ` +
    `${formatBytes(size)} | dc=${info.dcId} | ${mimeType}`
  );
  return info;
}

// ─── Core: DC-Aware Block Fetch ───────────────────────────────────────────────

function getBlockOffset(pos: number): number {
  return Math.floor(pos / CHUNK_SIZE) * CHUNK_SIZE;
}

/**
 * Fetch one 128KB block using the correct DC client.
 *
 * DC fix:
 *   - getClientForDc(info.dcId) gives us a client authenticated to that DC
 *   - We pass that client to invoke() instead of primary client
 *   - No more "stored in DC X" errors
 */
async function fetchBlock(
  info: FileInfo,
  blockOffset: number
): Promise<Uint8Array> {
  await acquireTelegramToken();

  // KEY FIX: Use DC-specific client
  const client = await getClientForDc(info.dcId);
  let lastError: any;

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const result = await client.invoke(
        new Api.upload.GetFile({
          location: new Api.InputDocumentFileLocation({
            id: BigInteger(info.id.toString()),
            accessHash: BigInteger(info.accessHash.toString()),
            fileReference: info.fileReference,
            thumbSize: "",
          }),
          offset: BigInteger(blockOffset),
          limit: CHUNK_SIZE,
          precise: true,
        })
      );

      if (result instanceof Api.upload.File) {
        return new Uint8Array(result.bytes);
      }

      if (result instanceof Api.upload.FileCdnRedirect) {
        await acquireTelegramToken();
        const cdnResult = await client.invoke(
          new Api.upload.GetCdnFile({
            fileToken: result.fileToken,
            offset: BigInteger(blockOffset),
            limit: CHUNK_SIZE,
          })
        );
        if (cdnResult instanceof Api.upload.CdnFile) {
          return new Uint8Array(cdnResult.bytes);
        }
      }

      throw new Error("Unknown GetFile result type");
    } catch (err: any) {
      lastError = err;
      const msg = err?.message || err?.errorMessage || "";

      if (msg.includes("FLOOD")) {
        const wait = RETRY_DELAY * Math.pow(2, attempt);
        console.warn(`⏳ Flood wait ${wait}ms (attempt ${attempt + 1})`);
        await sleep(wait);
        continue;
      }

      if (msg.includes("FILE_REFERENCE")) {
        console.warn("🔄 File reference expired, clearing cache...");
        for (const [k, v] of fileInfoCache)
          if (v.id === info.id) fileInfoCache.delete(k);
        throw new Error("FILE_REFERENCE_EXPIRED: Retry request");
      }

      // DC redirect inside error — should not happen now but handle gracefully
      if (msg.includes("stored in DC")) {
        const dcMatch = msg.match(/DC\s*(\d+)/i);
        if (dcMatch) {
          const targetDc = parseInt(dcMatch[1]);
          console.warn(`🔄 Redirecting to DC${targetDc}...`);
          // Update cached info with correct DC
          info.dcId = targetDc;
          const dcClient = await getClientForDc(targetDc);
          const retryResult = await dcClient.invoke(
            new Api.upload.GetFile({
              location: new Api.InputDocumentFileLocation({
                id: BigInteger(info.id.toString()),
                accessHash: BigInteger(info.accessHash.toString()),
                fileReference: info.fileReference,
                thumbSize: "",
              }),
              offset: BigInteger(blockOffset),
              limit: CHUNK_SIZE,
              precise: true,
            })
          );
          if (retryResult instanceof Api.upload.File) {
            return new Uint8Array(retryResult.bytes);
          }
        }
      }

      throw err;
    }
  }

  throw lastError;
}

// ─── Streaming ────────────────────────────────────────────────────────────────

function createTelegramStream(
  info: FileInfo,
  start: number,
  end: number
): ReadableStream {
  let cancelled = false;

  return new ReadableStream({
    async start(controller) {
      activeStreams++;
      let currentBlock = getBlockOffset(start);
      let bytesSent = 0;
      const totalNeeded = end - start + 1;

      console.log(
        `🌊 Stream start: [${start}-${end}] = ${formatBytes(totalNeeded)} | ` +
        `block=${currentBlock} | active=${activeStreams}`
      );

      try {
        while (bytesSent < totalNeeded) {
          if (cancelled) { console.log("  🛑 Cancelled before fetch"); return; }

          const block = await fetchBlock(info, currentBlock);

          if (block.length === 0) { console.warn("  ⚠️  Empty block"); break; }

          if (cancelled) { console.log("  🛑 Cancelled after fetch"); return; }

          const absStart = currentBlock;
          const absEnd = currentBlock + block.length - 1;
          const oStart = Math.max(absStart, start);
          const oEnd = Math.min(absEnd, end);

          if (oStart <= oEnd) {
            const slice = block.slice(oStart - absStart, oEnd - absStart + 1);
            try {
              controller.enqueue(slice);
              bytesSent += slice.length;
            } catch (e: any) {
              if (e?.code === "ERR_INVALID_STATE") {
                console.log("  🛑 Controller closed");
                return;
              }
              throw e;
            }
          }

          currentBlock += CHUNK_SIZE;
          if (block.length < CHUNK_SIZE) { console.log("  📄 EOF"); break; }
        }

        if (!cancelled) {
          controller.close();
          console.log(`  ✅ Done: ${formatBytes(bytesSent)}`);
        }
      } catch (err: any) {
        if (cancelled) { console.log("  🛑 Post-cancel error:", err.message); return; }
        console.error("  ❌ Stream error:", err.message);
        try { controller.error(err); } catch { /* already closed */ }
      } finally {
        activeStreams--;
      }
    },

    cancel(reason) {
      cancelled = true;
      console.log(`  🛑 Browser cancelled: ${reason || "seek/close"}`);
    },
  });
}

// ─── Demo Stream ──────────────────────────────────────────────────────────────

function handleDemoStream(request: Request): Response {
  const range = parseRangeHeader(
    request.headers.get("Range") || "",
    DEMO_FILE_SIZE
  );

  const start = range?.start ?? 0;
  const end = range?.end ?? DEMO_FILE_SIZE - 1;
  const status = range ? 206 : 200;

  const chunk = new Uint8Array(end - start + 1);
  for (let i = 0; i < chunk.length; i++) chunk[i] = (start + i) % 256;

  const headers: Record<string, string> = {
    "Content-Type": "video/mp4",
    "Content-Length": chunk.length.toString(),
    "Accept-Ranges": "bytes",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Expose-Headers":
      "Content-Range, Accept-Ranges, Content-Length, Content-Type",
  };
  if (range) headers["Content-Range"] = `bytes ${start}-${end}/${DEMO_FILE_SIZE}`;

  return new Response(chunk, { status, headers });
}

// ─── Route Handlers ───────────────────────────────────────────────────────────

async function handleStreamRequest(
  request: Request,
  messageId: string
): Promise<Response> {
  const url = new URL(request.url);
  const channelId = url.searchParams.get("channel") || CHANNEL_ID;

  if (!channelId) {
    return jsonError(400, "Missing channel ID. Pass ?channel=ID or set TELEGRAM_CHANNEL_ID");
  }

  if (activeStreams >= RATE_LIMIT.maxConcurrentStreams) {
    return jsonError(429, "Too many concurrent streams. Try again shortly.", {
      "Retry-After": "5",
    });
  }

  if (!API_ID || !API_HASH || !SESSION_STRING) {
    console.warn("⚠️  Telegram not configured → demo stream");
    return handleDemoStream(request);
  }

  try {
    const info = await getFileInfo(messageId, channelId);

    if (request.method === "HEAD") {
      return new Response(null, {
        status: 200,
        headers: {
          "Content-Type": info.mimeType,
          "Content-Length": info.size.toString(),
          "Accept-Ranges": "bytes",
          "Content-Disposition": `inline; filename="${info.fileName}"`,
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Expose-Headers":
            "Content-Range, Accept-Ranges, Content-Length, Content-Type, Content-Disposition",
        },
      });
    }

    const rangeHeader = request.headers.get("Range");
    let start: number, end: number;

    if (rangeHeader) {
      const range = parseRangeHeader(rangeHeader, info.size);
      if (!range) {
        return new Response(null, {
          status: 416,
          headers: {
            "Content-Range": `bytes */${info.size}`,
            "Access-Control-Allow-Origin": "*",
          },
        });
      }
      start = range.start;
      end = range.end;
    } else {
      start = 0;
      end = info.size - 1;
    }

    const cappedEnd = Math.min(end, start + MAX_RESPONSE_SIZE - 1);
    const contentLength = cappedEnd - start + 1;

    console.log(
      `\n🎬 msg=${messageId} | "${info.fileName}" | ` +
      `[${formatBytes(start)}-${formatBytes(cappedEnd)}] | ` +
      `${formatBytes(contentLength)} | file=${formatBytes(info.size)}`
    );

    const stream = createTelegramStream(info, start, cappedEnd);

    return new Response(stream, {
      status: 206,
      headers: {
        "Content-Type": info.mimeType,
        "Content-Length": contentLength.toString(),
        "Content-Range": `bytes ${start}-${cappedEnd}/${info.size}`,
        "Accept-Ranges": "bytes",
        "Content-Disposition": `inline; filename="${info.fileName}"`,
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Expose-Headers":
          "Content-Range, Accept-Ranges, Content-Length, Content-Type, Content-Disposition",
        "Cache-Control": "no-cache, no-store",
      },
    });
  } catch (err: any) {
    console.error("❌ Stream error:", err.message);
    return jsonError(500, err.message || "Internal Server Error");
  }
}

async function handleInfoRequest(
  messageId: string,
  channelId: string
): Promise<Response> {
  if (!API_ID || !API_HASH) {
    return jsonError(503, "Telegram not configured", {}, {
      hint: "Set TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_SESSION",
    });
  }

  try {
    const info = await getFileInfo(messageId, channelId);
    return new Response(
      JSON.stringify({
        messageId,
        channelId,
        fileName: info.fileName,          // ← filename included
        fileSize: info.size,
        fileSizeFormatted: formatBytes(info.size),
        mimeType: info.mimeType,
        dcId: info.dcId,
        streamUrl: `/stream/${messageId}${channelId ? `?channel=${channelId}` : ""}`,
      }),
      {
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
      }
    );
  } catch (err: any) {
    return jsonError(500, err.message || "Failed to get file info");
  }
}

// ─── Main Router ──────────────────────────────────────────────────────────────

async function handleRequest(request: Request): Promise<Response> {
  const url = new URL(request.url);

  const ip =
    request.headers.get("x-forwarded-for")?.split(",")[0].trim() ||
    request.headers.get("x-real-ip") ||
    "127.0.0.1";

  // CORS preflight
  if (request.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers": "Range, Content-Type, Accept",
        "Access-Control-Expose-Headers":
          "Content-Range, Accept-Ranges, Content-Length, Content-Type, Content-Disposition",
        "Access-Control-Max-Age": "86400",
      },
    });
  }

  // Rate limit (skip health)
  if (url.pathname !== "/health" && !checkIpRateLimit(ip)) {
    console.warn(`🚫 Rate limited: ${ip}`);
    return jsonError(429, "Too many requests. Please slow down.", {
      "Retry-After": "60",
    });
  }

  // Health
  if (url.pathname === "/health") {
    return new Response(
      JSON.stringify({
        status: "ok",
        telegram: primaryClient?.connected ?? false,
        activeStreams,
        cachedFiles: fileInfoCache.size,
        dcConnections: [...dcClients.keys()],
        rateLimit: RATE_LIMIT,
        chunkSize: formatBytes(CHUNK_SIZE),
        maxResponseSize: formatBytes(MAX_RESPONSE_SIZE),
        timestamp: new Date().toISOString(),
      }),
      { headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } }
    );
  }

  // /stream/:id
  const streamMatch = url.pathname.match(/^\/stream\/(\d+)$/);
  if (streamMatch) return handleStreamRequest(request, streamMatch[1]);

  // /info/:id
  const infoMatch = url.pathname.match(/^\/info\/(\d+)$/);
  if (infoMatch) {
    const channelId = url.searchParams.get("channel") || CHANNEL_ID;
    return handleInfoRequest(infoMatch[1], channelId);
  }

  // /demo-stream
  if (url.pathname === "/demo-stream") return handleDemoStream(request);

  return new Response(
    JSON.stringify({
      error: "Not Found",
      availableEndpoints: [
        "GET /stream/:messageId?channel=CHANNEL_ID",
        "GET /info/:messageId?channel=CHANNEL_ID",
        "GET /demo-stream",
        "GET /health",
      ],
    }),
    {
      status: 404,
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
    }
  );
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function parseRangeHeader(
  header: string,
  fileSize: number
): { start: number; end: number } | null {
  if (!header) return null;
  const match = header.match(/bytes=(\d*)-(\d*)/);
  if (!match) return null;

  let start: number, end: number;

  if (match[1] === "" && match[2] !== "") {
    const suffix = parseInt(match[2]);
    start = Math.max(0, fileSize - suffix);
    end = fileSize - 1;
  } else {
    start = match[1] !== "" ? parseInt(match[1]) : 0;
    end = match[2] !== "" ? parseInt(match[2]) : fileSize - 1;
  }

  end = Math.min(end, fileSize - 1);
  if (start < 0 || start > end || start >= fileSize) return null;
  return { start, end };
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 Bytes";
  const u = ["Bytes", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${parseFloat((bytes / Math.pow(1024, i)).toFixed(2))} ${u[i]}`;
}

function sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}

function jsonError(
  status: number,
  message: string,
  extraHeaders: Record<string, string> = {},
  extraBody: Record<string, unknown> = {}
): Response {
  return new Response(JSON.stringify({ error: message, ...extraBody }), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      ...extraHeaders,
    },
  });
}

// ─── Start ────────────────────────────────────────────────────────────────────

console.log(`\n🚀 Telegram Video Streaming Service`);
console.log(`   Port           : ${PORT}`);
console.log(`   Chunk          : ${formatBytes(CHUNK_SIZE)}`);
console.log(`   Max Response   : ${formatBytes(MAX_RESPONSE_SIZE)}`);
console.log(`   Max Streams    : ${RATE_LIMIT.maxConcurrentStreams}`);
console.log(`   Req/min per IP : ${RATE_LIMIT.requestsPerMinute}`);
console.log(`   TG calls/sec   : ${RATE_LIMIT.telegramCallsPerSec}`);
console.log(`   API ID         : ${API_ID || "⚠️  NOT SET"}`);
console.log(`   Channel        : ${CHANNEL_ID || "pass ?channel=..."}`);
console.log(`\n📡 Endpoints:`);
console.log(`   GET /stream/:messageId?channel=ID`);
console.log(`   GET /info/:messageId?channel=ID`);
console.log(`   GET /demo-stream`);
console.log(`   GET /health`);

serve({ port: PORT, fetch: handleRequest, idleTimeout: 120 });

console.log(`\n✅ Ready → http://localhost:${PORT}\n`);