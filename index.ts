/**
 * Telegram Video Streaming Service
 *
 * Features:
 * - HTTP 206 Partial Content support with proper Range handling
 * - Fixed 128KB aligned chunks (prevents LIMIT_INVALID)
 * - Rate limiting: max concurrent streams + requests per minute
 * - Flood wait / retry logic
 * - File info cache (10 min TTL)
 * - Demo stream for testing without Telegram
 * - CORS support
 * - Graceful cancel when browser seeks/closes
 */

import { serve } from "bun";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions";
import { Api } from "telegram";
import BigInteger from "big-integer";

// ─── Configuration ────────────────────────────────────────────────────────────

const PORT = parseInt(process.env.PORT || "3030");
const MAX_RETRIES = 3;
const RETRY_DELAY = 1000; // ms, doubles each retry

const API_ID = parseInt(process.env.TELEGRAM_API_ID || "0");
const API_HASH = process.env.TELEGRAM_API_HASH || "";
const SESSION_STRING = process.env.TELEGRAM_SESSION || "";
const CHANNEL_ID = process.env.TELEGRAM_CHANNEL_ID || "";

// Validate on startup
if (!API_ID || !API_HASH) {
  console.warn("⚠️  TELEGRAM_API_ID and TELEGRAM_API_HASH not set");
  console.warn("   Demo mode will be used as fallback");
}

// ─── Telegram Chunk Rules ─────────────────────────────────────────────────────
//
// Telegram upload.GetFile strict rules:
//   1. limit MUST be a power of 2 (4KB to 1MB)
//   2. offset MUST be a multiple of limit
//
// Fix: use ONE fixed chunk size (128KB).
// offset = floor(bytePos / 131072) * 131072  → always valid!
//
const CHUNK_SIZE = 131072; // 128KB fixed — never changes

// Max bytes sent per HTTP response (browser requests next range after this)
const MAX_RESPONSE_SIZE = 4 * 1024 * 1024; // 4MB

// Demo file size for testing without Telegram
const DEMO_FILE_SIZE = 10 * 1024 * 1024; // 10MB

// ─── Rate Limiting ────────────────────────────────────────────────────────────

const RATE_LIMIT = {
  maxConcurrentStreams: 10,   // max parallel video streams
  requestsPerMinute: 60,      // max HTTP requests per IP per minute
  telegramCallsPerSec: 3,     // max Telegram API calls per second globally
};

// Track concurrent streams
let activeStreams = 0;

// Per-IP request tracking
const ipRequestLog = new Map<string, number[]>(); // ip → timestamps[]

// Global Telegram call rate (token bucket)
let telegramTokens = RATE_LIMIT.telegramCallsPerSec;
let lastTokenRefill = Date.now();

/**
 * Check and consume a Telegram API token.
 * Blocks until a token is available.
 */
async function acquireTelegramToken(): Promise<void> {
  while (true) {
    const now = Date.now();
    const elapsed = (now - lastTokenRefill) / 1000;

    // Refill tokens based on elapsed time
    if (elapsed >= 1) {
      telegramTokens = RATE_LIMIT.telegramCallsPerSec;
      lastTokenRefill = now;
    }

    if (telegramTokens > 0) {
      telegramTokens--;
      return;
    }

    // Wait for next refill
    await sleep(200);
  }
}

/**
 * Check if IP has exceeded per-minute request limit.
 * Returns true if allowed, false if rate limited.
 */
function checkIpRateLimit(ip: string): boolean {
  const now = Date.now();
  const windowMs = 60 * 1000; // 1 minute

  const timestamps = ipRequestLog.get(ip) || [];

  // Remove timestamps older than 1 minute
  const recent = timestamps.filter((t) => now - t < windowMs);
  recent.push(now);
  ipRequestLog.set(ip, recent);

  // Cleanup old IPs every 1000 requests to prevent memory leak
  if (ipRequestLog.size > 1000) {
    for (const [key, times] of ipRequestLog.entries()) {
      if (times.every((t) => now - t >= windowMs)) {
        ipRequestLog.delete(key);
      }
    }
  }

  return recent.length <= RATE_LIMIT.requestsPerMinute;
}

// ─── Types ────────────────────────────────────────────────────────────────────

interface FileInfo {
  size: number;
  dcId: number;
  accessHash: bigint;
  fileReference: Buffer;
  id: bigint;
  mimeType: string;
  timestamp: number;
}

// ─── File Info Cache ──────────────────────────────────────────────────────────

const fileInfoCache = new Map<string, FileInfo>();
const CACHE_TTL = 10 * 60 * 1000; // 10 minutes

// ─── Telegram Client ──────────────────────────────────────────────────────────

let telegramClient: TelegramClient | null = null;
let connectionPromise: Promise<void> | null = null;

async function getTelegramClient(): Promise<TelegramClient> {
  if (telegramClient?.connected) return telegramClient;

  if (!connectionPromise) {
    connectionPromise = (async () => {
      const session = new StringSession(SESSION_STRING);
      telegramClient = new TelegramClient(session, API_ID, API_HASH, {
        connectionRetries: 5,
        timeout: 30000,
        useWSS: false,
      });
      await telegramClient.connect();
      console.log("✅ Telegram client connected");
    })().catch((err) => {
      connectionPromise = null;
      telegramClient = null;
      throw err;
    });
  }

  await connectionPromise;
  return telegramClient!;
}

// ─── File Info (cached) ───────────────────────────────────────────────────────

async function getFileInfo(
  messageId: string,
  channelId: string
): Promise<FileInfo> {
  const cacheKey = `${channelId}:${messageId}`;
  const cached = fileInfoCache.get(cacheKey);

  if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
    return cached;
  }

  const client = await getTelegramClient();

  const messages = await client.getMessages(channelId, {
    ids: [parseInt(messageId)],
  });

  if (!messages?.length || !messages[0]) {
    throw new Error(`Message ${messageId} not found in channel ${channelId}`);
  }

  const msg = messages[0];

  if (!(msg.media instanceof Api.MessageMediaDocument)) {
    throw new Error(
      `Message ${messageId} is not a document (type: ${msg.media?.className})`
    );
  }

  const document = msg.media.document;
  if (!(document instanceof Api.Document)) {
    throw new Error("Invalid document in message");
  }

  // Parse size (handles native BigInt and gramjs BigInteger)
  const rawSize = document.size;
  let size: number;
  if (typeof rawSize === "bigint") {
    size = Number(rawSize);
  } else if (rawSize && typeof rawSize === "object" && "toJSNumber" in rawSize) {
    size = (rawSize as any).toJSNumber();
  } else {
    size = Number(rawSize);
  }

  const info: FileInfo = {
    id: document.id as bigint,
    accessHash: document.accessHash as bigint,
    fileReference: Buffer.from(document.fileReference),
    dcId: document.dcId,
    size,
    mimeType: document.mimeType || "video/mp4",
    timestamp: Date.now(),
  };

  fileInfoCache.set(cacheKey, info);
  console.log(
    `📁 Cached: msg=${messageId} | ${formatBytes(size)} | ` +
    `dc=${info.dcId} | ${info.mimeType}`
  );

  return info;
}

// ─── Core: Fetch One Aligned Block ───────────────────────────────────────────

/**
 * Convert any byte position to its aligned block offset.
 * offset = floor(pos / CHUNK_SIZE) * CHUNK_SIZE
 * This is ALWAYS a valid Telegram offset (multiple of CHUNK_SIZE).
 */
function getBlockOffset(bytePosition: number): number {
  return Math.floor(bytePosition / CHUNK_SIZE) * CHUNK_SIZE;
}

/**
 * Fetch one 128KB aligned block from Telegram.
 * Includes rate limiting + flood wait retry.
 */
async function fetchBlock(
  info: FileInfo,
  blockOffset: number
): Promise<Uint8Array> {
  // Acquire global Telegram rate limit token
  await acquireTelegramToken();

  const client = await getTelegramClient();
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
          limit: CHUNK_SIZE, // Always 128KB — always valid
          precise: true,
        })
      );

      if (result instanceof Api.upload.File) {
        return new Uint8Array(result.bytes);
      }

      // CDN redirect for large/popular files
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

      throw new Error("Unknown result from GetFile");
    } catch (err: any) {
      lastError = err;
      const msg = err?.message || err?.errorMessage || "";

      // Flood wait — back off and retry
      if (msg.includes("FLOOD")) {
        const wait = RETRY_DELAY * Math.pow(2, attempt);
        console.warn(
          `⏳ Flood wait ${wait}ms (attempt ${attempt + 1}/${MAX_RETRIES})`
        );
        await sleep(wait);
        continue;
      }

      // File reference expired — clear cache, caller should retry fresh
      if (msg.includes("FILE_REFERENCE")) {
        console.warn("🔄 File reference expired, clearing cache...");
        for (const [key, val] of fileInfoCache.entries()) {
          if (val.id === info.id) fileInfoCache.delete(key);
        }
        throw new Error("FILE_REFERENCE_EXPIRED: Retry the request");
      }

      // Other errors — don't retry
      throw err;
    }
  }

  throw lastError;
}

// ─── Streaming ────────────────────────────────────────────────────────────────

/**
 * Create a ReadableStream that fetches [start, end] bytes from Telegram.
 * Uses 128KB aligned blocks. Sends each block immediately to browser.
 * Stops cleanly when browser cancels (seek, tab close, etc.).
 */
function createTelegramStream(
  info: FileInfo,
  start: number,
  end: number
): ReadableStream {
  let cancelled = false;

  return new ReadableStream({
    async start(controller) {
      activeStreams++;
      let currentBlockOffset = getBlockOffset(start);
      let bytesSent = 0;
      const totalNeeded = end - start + 1;

      console.log(
        `🌊 Stream start: [${start}-${end}] = ${formatBytes(totalNeeded)} | ` +
        `block=${currentBlockOffset} | active=${activeStreams}`
      );

      try {
        while (bytesSent < totalNeeded) {
          // Stop if browser disconnected
          if (cancelled) {
            console.log("  🛑 Cancelled before fetch");
            return;
          }

          // Fetch aligned 128KB block
          const block = await fetchBlock(info, currentBlockOffset);

          if (block.length === 0) {
            console.warn("  ⚠️  Empty block received");
            break;
          }

          // Check again after async fetch (browser may have seeked)
          if (cancelled) {
            console.log("  🛑 Cancelled after fetch, discarding block");
            return;
          }

          // Calculate overlap: what part of this block overlaps [start, end]?
          const blockAbsStart = currentBlockOffset;
          const blockAbsEnd = currentBlockOffset + block.length - 1;
          const overlapStart = Math.max(blockAbsStart, start);
          const overlapEnd = Math.min(blockAbsEnd, end);

          if (overlapStart <= overlapEnd) {
            const slice = block.slice(
              overlapStart - blockAbsStart,
              overlapEnd - blockAbsStart + 1
            );

            try {
              controller.enqueue(slice);
              bytesSent += slice.length;
            } catch (e: any) {
              // Controller closed between check and enqueue (race condition)
              if (e?.code === "ERR_INVALID_STATE") {
                console.log("  🛑 Controller closed during enqueue");
                return;
              }
              throw e;
            }
          }

          currentBlockOffset += CHUNK_SIZE;

          // EOF: Telegram returned fewer bytes than requested
          if (block.length < CHUNK_SIZE) {
            console.log("  📄 EOF reached");
            break;
          }
        }

        if (!cancelled) {
          controller.close();
          console.log(`  ✅ Stream done: ${formatBytes(bytesSent)}`);
        }
      } catch (err: any) {
        if (cancelled) {
          console.log("  🛑 Error after cancel (expected):", err.message);
          return;
        }
        console.error("  ❌ Stream error:", err.message);
        try {
          controller.error(err);
        } catch {
          // Already closed
        }
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

/**
 * Demo stream for testing without Telegram credentials.
 * Returns a byte-pattern stream with proper Range support.
 */
function handleDemoStream(request: Request): Response {
  const rangeHeader = request.headers.get("Range");
  const range = parseRangeHeader(rangeHeader || "", DEMO_FILE_SIZE);

  let start: number, end: number, status: number;

  if (range) {
    start = range.start;
    end = range.end;
    status = 206;
  } else {
    start = 0;
    end = DEMO_FILE_SIZE - 1;
    status = 200;
  }

  // Generate pattern data
  const size = end - start + 1;
  const chunk = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    chunk[i] = (start + i) % 256;
  }

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
    return jsonError(400, "Missing channel ID. Pass ?channel=ID or set TELEGRAM_CHANNEL_ID");
  }

  // Check concurrent stream limit
  if (activeStreams >= RATE_LIMIT.maxConcurrentStreams) {
    console.warn(`⚠️  Max concurrent streams reached (${activeStreams})`);
    return jsonError(429, "Too many concurrent streams. Try again later.", {
      "Retry-After": "5",
    });
  }

  // Demo fallback if Telegram not configured
  if (!API_ID || !API_HASH || !SESSION_STRING) {
    console.warn("⚠️  Telegram not configured, using demo stream");
    return handleDemoStream(request);
  }

  try {
    const info = await getFileInfo(messageId, channelId);

    // HEAD request — return file metadata only
    if (request.method === "HEAD") {
      return new Response(null, {
        status: 200,
        headers: {
          "Content-Type": info.mimeType,
          "Content-Length": info.size.toString(),
          "Accept-Ranges": "bytes",
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Expose-Headers":
            "Content-Range, Accept-Ranges, Content-Length, Content-Type",
        },
      });
    }

    // Parse Range header
    const rangeHeader = request.headers.get("Range");
    let start: number;
    let end: number;

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
      // No Range header — start from beginning
      start = 0;
      end = info.size - 1;
    }

    // Cap response to MAX_RESPONSE_SIZE so browser gets data fast
    // Browser will automatically request next range after receiving this
    const cappedEnd = Math.min(end, start + MAX_RESPONSE_SIZE - 1);
    const contentLength = cappedEnd - start + 1;

    console.log(
      `\n🎬 msg=${messageId} | ` +
      `[${formatBytes(start)}-${formatBytes(cappedEnd)}] | ` +
      `${formatBytes(contentLength)} | ` +
      `file=${formatBytes(info.size)}`
    );

    const stream = createTelegramStream(info, start, cappedEnd);

    return new Response(stream, {
      status: 206,
      headers: {
        "Content-Type": info.mimeType,
        "Content-Length": contentLength.toString(),
        "Content-Range": `bytes ${start}-${cappedEnd}/${info.size}`,
        "Accept-Ranges": "bytes",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Expose-Headers":
          "Content-Range, Accept-Ranges, Content-Length, Content-Type",
        "Cache-Control": "no-cache, no-store",
      },
    });
  } catch (err: any) {
    console.error("❌ Stream handler error:", err.message);

    // Fallback to demo on error
    if (!request.headers.get("Range")) {
      return jsonError(500, err.message || "Internal Server Error");
    }

    // For range requests return error (browser will retry)
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
        fileSize: info.size,
        fileSizeFormatted: formatBytes(info.size),
        mimeType: info.mimeType,
        dcId: info.dcId,
        streamUrl: `/stream/${messageId}`,
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

  // Extract client IP for rate limiting
  const ip =
    request.headers.get("x-forwarded-for")?.split(",")[0].trim() ||
    request.headers.get("x-real-ip") ||
    "unknown";

  // CORS preflight
  if (request.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers": "Range, Content-Type, Accept",
        "Access-Control-Expose-Headers":
          "Content-Range, Accept-Ranges, Content-Length, Content-Type",
        "Access-Control-Max-Age": "86400",
      },
    });
  }

  // Per-IP rate limit (skip for health checks)
  if (url.pathname !== "/health" && !checkIpRateLimit(ip)) {
    console.warn(`🚫 Rate limited IP: ${ip}`);
    return jsonError(429, "Too many requests. Slow down.", { "Retry-After": "60" });
  }

  // Health check
  if (url.pathname === "/health") {
    return new Response(
      JSON.stringify({
        status: "ok",
        telegram: telegramClient?.connected ?? false,
        activeStreams,
        cachedFiles: fileInfoCache.size,
        rateLimit: {
          maxConcurrentStreams: RATE_LIMIT.maxConcurrentStreams,
          requestsPerMinute: RATE_LIMIT.requestsPerMinute,
          telegramCallsPerSec: RATE_LIMIT.telegramCallsPerSec,
        },
        chunkSize: formatBytes(CHUNK_SIZE),
        maxResponseSize: formatBytes(MAX_RESPONSE_SIZE),
        timestamp: new Date().toISOString(),
      }),
      {
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
      }
    );
  }

  // Stream: /stream/:messageId
  const streamMatch = url.pathname.match(/^\/stream\/(\d+)$/);
  if (streamMatch) {
    return handleStreamRequest(request, streamMatch[1]);
  }

  // Info: /info/:messageId
  const infoMatch = url.pathname.match(/^\/info\/(\d+)$/);
  if (infoMatch) {
    const channelId = url.searchParams.get("channel") || CHANNEL_ID;
    return handleInfoRequest(infoMatch[1], channelId);
  }

  // Demo stream for testing without Telegram
  if (url.pathname === "/demo-stream") {
    return handleDemoStream(request);
  }

  // 404
  return new Response(
    JSON.stringify({
      error: "Not Found",
      path: url.pathname,
      availableEndpoints: [
        "GET /stream/:messageId?channel=CHANNEL_ID",
        "GET /info/:messageId?channel=CHANNEL_ID",
        "GET /demo-stream",
        "GET /health",
      ],
    }),
    {
      status: 404,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
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

  let start: number;
  let end: number;

  if (match[1] === "" && match[2] !== "") {
    // bytes=-500 → last 500 bytes
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
  const units = ["Bytes", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${parseFloat((bytes / Math.pow(1024, i)).toFixed(2))} ${units[i]}`;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function jsonError(
  status: number,
  message: string,
  extraHeaders: Record<string, string> = {},
  extraBody: Record<string, unknown> = {}
): Response {
  return new Response(
    JSON.stringify({ error: message, ...extraBody }),
    {
      status,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        ...extraHeaders,
      },
    }
  );
}

// ─── Start ────────────────────────────────────────────────────────────────────

console.log(`\n🚀 Telegram Video Streaming Service`);
console.log(`   Port              : ${PORT}`);
console.log(`   Chunk Size        : ${formatBytes(CHUNK_SIZE)} (fixed aligned)`);
console.log(`   Max Response      : ${formatBytes(MAX_RESPONSE_SIZE)}`);
console.log(`   Max Streams       : ${RATE_LIMIT.maxConcurrentStreams}`);
console.log(`   Req/min per IP    : ${RATE_LIMIT.requestsPerMinute}`);
console.log(`   Telegram calls/s  : ${RATE_LIMIT.telegramCallsPerSec}`);
console.log(`   API ID            : ${API_ID || "⚠️  NOT SET (demo mode)"}`);
console.log(`   Channel           : ${CHANNEL_ID || "pass ?channel=..."}`);
console.log(`\n📡 Endpoints:`);
console.log(`   GET  /stream/:messageId?channel=ID`);
console.log(`   GET  /info/:messageId?channel=ID`);
console.log(`   GET  /demo-stream`);
console.log(`   GET  /health`);

serve({
  port: PORT,
  fetch: handleRequest,
  idleTimeout: 120,
});

console.log(`\n✅ Ready → http://localhost:${PORT}\n`);