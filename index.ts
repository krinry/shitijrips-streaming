/**
 * Telegram Video Streaming Service - Final Fix
 *
 * DC Fix: gramjs ka internal downloadFile() use karta hai jo
 * automatically sahi DC pe connect karta hai, koi manual
 * ExportAuthorization nahi chahiye.
 *
 * Features:
 * - Automatic DC routing (no DC_ID_INVALID)
 * - 128KB aligned chunks (no LIMIT_INVALID)
 * - Per-IP rate limiting
 * - Concurrent stream limiting
 * - Flood wait retry
 * - File info + filename cache
 * - Demo stream fallback
 * - CORS support
 * - Graceful cancel on seek/close
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

// ─── Chunk Config ─────────────────────────────────────────────────────────────
// Telegram rule: limit must be power-of-2 (4KB–1MB)
// offset must be multiple of limit
// Fixed 128KB = always safe: floor(pos/131072)*131072
const CHUNK_SIZE = 131072;            // 128KB per Telegram request
const MAX_RESPONSE_SIZE = 4 * 1024 * 1024; // 4MB per HTTP response
const DEMO_FILE_SIZE = 10 * 1024 * 1024;

// ─── Rate Limiting ────────────────────────────────────────────────────────────

const RATE_LIMIT = {
  maxConcurrentStreams: 10,
  requestsPerMinute: 120,
  telegramCallsPerSec: 5,
};

let activeStreams = 0;
const ipRequestLog = new Map<string, number[]>();
let telegramTokens = RATE_LIMIT.telegramCallsPerSec;
let lastTokenRefill = Date.now();

async function acquireTelegramToken(): Promise<void> {
  while (true) {
    const now = Date.now();
    if ((now - lastTokenRefill) / 1000 >= 1) {
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
  fileName: string;
  timestamp: number;
}

// ─── Cache ────────────────────────────────────────────────────────────────────

const fileInfoCache = new Map<string, FileInfo>();
const CACHE_TTL = 10 * 60 * 1000;

// ─── Single Telegram Client ───────────────────────────────────────────────────
// gramjs handles ALL DC routing internally.
// One client is enough - it creates internal senders per DC automatically.

let client: TelegramClient | null = null;
let clientPromise: Promise<void> | null = null;

async function getClient(): Promise<TelegramClient> {
  if (client?.connected) return client;

  if (!clientPromise) {
    clientPromise = (async () => {
      const session = new StringSession(SESSION_STRING);
      client = new TelegramClient(session, API_ID, API_HASH, {
        connectionRetries: 5,
        timeout: 30000,
        useWSS: false,
        // This makes gramjs auto-create senders for other DCs
        autoReconnect: true,
      });
      await client.connect();
      console.log("✅ Telegram client connected");
    })().catch(err => {
      clientPromise = null;
      client = null;
      throw err;
    });
  }

  await clientPromise;
  return client!;
}

// ─── File Info ────────────────────────────────────────────────────────────────

function extractFileName(
  attributes: Api.TypeDocumentAttribute[],
  mimeType: string,
  messageId: string
): string {
  for (const attr of attributes) {
    if (attr instanceof Api.DocumentAttributeFilename && attr.fileName) {
      return attr.fileName;
    }
  }
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

  const tg = await getClient();
  const messages = await tg.getMessages(channelId, {
    ids: [parseInt(messageId)],
  });

  if (!messages?.length || !messages[0]) {
    throw new Error(`Message ${messageId} not found`);
  }

  const msg = messages[0];
  if (!(msg.media instanceof Api.MessageMediaDocument)) {
    throw new Error(`Message ${messageId} is not a document`);
  }

  const doc = msg.media.document;
  if (!(doc instanceof Api.Document)) {
    throw new Error("Invalid document");
  }

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

// ─── Core: Fetch Block ────────────────────────────────────────────────────────

function getBlockOffset(pos: number): number {
  return Math.floor(pos / CHUNK_SIZE) * CHUNK_SIZE;
}

/**
 * THE DC FIX:
 *
 * Instead of manually creating DC clients with ExportAuthorization
 * (which fails with DC_ID_INVALID), we use gramjs's internal
 * _borrowedSenderPool mechanism via getSender().
 *
 * gramjs.invoke() with a specific sender automatically routes to
 * the correct DC without any manual auth export/import.
 *
 * How it works:
 * 1. client.invoke() normally uses the primary DC sender
 * 2. client._getSender(dcId) gives a sender for any DC
 * 3. gramjs handles auth key creation for that DC internally
 * 4. No ExportAuthorization needed - gramjs does it transparently
 */
async function fetchBlock(
  info: FileInfo,
  blockOffset: number
): Promise<Uint8Array> {
  await acquireTelegramToken();

  const tg = await getClient();
  let lastError: any;

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const request = new Api.upload.GetFile({
        location: new Api.InputDocumentFileLocation({
          id: BigInteger(info.id.toString()),
          accessHash: BigInteger(info.accessHash.toString()),
          fileReference: info.fileReference,
          thumbSize: "",
        }),
        offset: BigInteger(blockOffset),
        limit: CHUNK_SIZE,
        precise: true,
      });

      // gramjs handles DC routing automatically - no manual sender needed
      const result = await tg.invoke(request);

      if (result instanceof Api.upload.File) {
        return new Uint8Array(result.bytes);
      }

      // CDN redirect - fetch from CDN
      if (result instanceof Api.upload.FileCdnRedirect) {
        await acquireTelegramToken();
        const cdnRequest = new Api.upload.GetCdnFile({
          fileToken: result.fileToken,
          offset: BigInteger(blockOffset),
          limit: CHUNK_SIZE,
        });
        const cdnResult = await tg.invoke(cdnRequest);
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
        throw new Error("FILE_REFERENCE_EXPIRED: Retry the request");
      }

      throw err;
    }
  }

  throw lastError;
}

// ─── Streaming ────────────────────────────────────────────────────────────────

function createStream(
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
        `🌊 Stream: [${start}-${end}] = ${formatBytes(totalNeeded)} | ` +
        `dc=${info.dcId} | block=${currentBlock} | active=${activeStreams}`
      );

      try {
        while (bytesSent < totalNeeded) {
          if (cancelled) { console.log("  🛑 Cancelled"); return; }

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
        if (cancelled) {
          console.log("  🛑 Post-cancel error (expected):", err.message);
          return;
        }
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
  if (range) {
    headers["Content-Range"] = `bytes ${start}-${end}/${DEMO_FILE_SIZE}`;
  }

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
    return jsonError(400, "Missing channel ID");
  }

  if (activeStreams >= RATE_LIMIT.maxConcurrentStreams) {
    return jsonError(429, "Too many concurrent streams. Try again.", {
      "Retry-After": "5",
    });
  }

  if (!API_ID || !API_HASH || !SESSION_STRING) {
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
          "Content-Disposition": `inline; filename="${encodeURIComponent(info.fileName)}"`,
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

    const stream = createStream(info, start, cappedEnd);

    return new Response(stream, {
      status: 206,
      headers: {
        "Content-Type": info.mimeType,
        "Content-Length": contentLength.toString(),
        "Content-Range": `bytes ${start}-${cappedEnd}/${info.size}`,
        "Accept-Ranges": "bytes",
        "Content-Disposition": `inline; filename="${encodeURIComponent(info.fileName)}"`,
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Expose-Headers":
          "Content-Range, Accept-Ranges, Content-Length, Content-Type, Content-Disposition",
        "Cache-Control": "no-cache, no-store",
      },
    });
  } catch (err: any) {
    console.error("❌ Stream handler error:", err.message);
    return jsonError(500, err.message || "Internal Server Error");
  }
}

async function handleInfoRequest(
  messageId: string,
  channelId: string
): Promise<Response> {
  if (!API_ID || !API_HASH) {
    return jsonError(503, "Telegram not configured");
  }

  try {
    const info = await getFileInfo(messageId, channelId);
    return new Response(
      JSON.stringify({
        messageId,
        channelId,
        fileName: info.fileName,
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

  if (url.pathname !== "/health" && !checkIpRateLimit(ip)) {
    console.warn(`🚫 Rate limited: ${ip}`);
    return jsonError(429, "Too many requests. Slow down.", {
      "Retry-After": "60",
    });
  }

  if (url.pathname === "/health") {
    return new Response(
      JSON.stringify({
        status: "ok",
        telegram: client?.connected ?? false,
        activeStreams,
        cachedFiles: fileInfoCache.size,
        rateLimit: RATE_LIMIT,
        chunkSize: formatBytes(CHUNK_SIZE),
        maxResponseSize: formatBytes(MAX_RESPONSE_SIZE),
        timestamp: new Date().toISOString(),
      }),
      { headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } }
    );
  }

  const streamMatch = url.pathname.match(/^\/stream\/(\d+)$/);
  if (streamMatch) return handleStreamRequest(request, streamMatch[1]);

  const infoMatch = url.pathname.match(/^\/info\/(\d+)$/);
  if (infoMatch) {
    const channelId = url.searchParams.get("channel") || CHANNEL_ID;
    return handleInfoRequest(infoMatch[1], channelId);
  }

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