import { serve } from "bun";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions";
import { Api } from "telegram";
import BigInteger from "big-integer";

const PORT = parseInt(process.env.PORT || "3030");
const MAX_RESPONSE_SIZE = 4 * 1024 * 1024; // 4MB per HTTP response
const MAX_RETRIES = 3;
const RETRY_DELAY = 1000;

const API_ID = parseInt(process.env.TELEGRAM_API_ID || "0");
const API_HASH = process.env.TELEGRAM_API_HASH || "";
const SESSION_STRING = process.env.TELEGRAM_SESSION || "";
const CHANNEL_ID = process.env.TELEGRAM_CHANNEL_ID || "";

// ─── Telegram Rules ───────────────────────────────────────────────────────────
// Rule 1: limit MUST be one of these exact values (power of 2)
// Rule 2: offset MUST be a multiple of limit (NOT just 4096!)
// Rule 3: max limit is 1MB
//
// Example: if limit=1MB, offset must be 0, 1MB, 2MB, 3MB... (multiples of 1MB)
//          if limit=512KB, offset must be 0, 512KB, 1MB... (multiples of 512KB)
//
// We use 128KB as our fixed chunk size because:
// - Small enough for smooth streaming
// - Large enough for efficiency  
// - offset alignment is easy: any multiple of 131072
const CHUNK_SIZE = 131072; // 128KB - fixed size, never changes

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

// ─── Cache ────────────────────────────────────────────────────────────────────

const fileInfoCache = new Map<string, FileInfo>();
const CACHE_TTL = 10 * 60 * 1000;

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
      console.log("✅ Telegram connected");
    })().catch((err) => {
      connectionPromise = null;
      telegramClient = null;
      throw err;
    });
  }

  await connectionPromise;
  return telegramClient!;
}

// ─── File Info ────────────────────────────────────────────────────────────────

async function getFileInfo(
  messageId: string,
  channelId: string
): Promise<FileInfo> {
  const cacheKey = `${channelId}:${messageId}`;
  const cached = fileInfoCache.get(cacheKey);
  if (cached && Date.now() - cached.timestamp < CACHE_TTL) return cached;

  const client = await getTelegramClient();
  const messages = await client.getMessages(channelId, {
    ids: [parseInt(messageId)],
  });

  if (!messages?.length || !messages[0]) {
    throw new Error(`Message ${messageId} not found`);
  }

  const msg = messages[0];
  if (!(msg.media instanceof Api.MessageMediaDocument)) {
    throw new Error(`Message ${messageId} is not a document`);
  }

  const document = msg.media.document;
  if (!(document instanceof Api.Document)) {
    throw new Error("Invalid document");
  }

  const rawSize = document.size;
  let size: number;
  if (typeof rawSize === "bigint") size = Number(rawSize);
  else if (rawSize && typeof rawSize === "object" && "toJSNumber" in rawSize)
    size = (rawSize as any).toJSNumber();
  else size = Number(rawSize);

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
  console.log(`📁 Cached: msg=${messageId} | ${formatBytes(size)} | dc=${info.dcId} | ${info.mimeType}`);
  return info;
}

// ─── Core: Aligned Fetch ──────────────────────────────────────────────────────

/**
 * THE KEY FIX:
 * 
 * Telegram rule: offset % limit === 0  (MUST be true)
 * 
 * We use FIXED chunk size (128KB) always.
 * offset must always be multiple of 128KB.
 * 
 * To fetch byte range [start, end]:
 * 1. Find which 128KB block contains `start`
 *    blockIndex = floor(start / CHUNK_SIZE)
 *    blockOffset = blockIndex * CHUNK_SIZE  ← always valid!
 * 2. Fetch from blockOffset with limit=CHUNK_SIZE
 * 3. Slice result to get exact [start, end] bytes
 */
function getBlockOffset(bytePosition: number): number {
  // Which 128KB block does this byte belong to?
  const blockIndex = Math.floor(bytePosition / CHUNK_SIZE);
  // Offset of that block (always multiple of CHUNK_SIZE = valid!)
  return blockIndex * CHUNK_SIZE;
}

async function fetchBlock(
  info: FileInfo,
  blockOffset: number  // MUST be multiple of CHUNK_SIZE
): Promise<Uint8Array> {
  // Verify alignment (safety check)
  if (blockOffset % CHUNK_SIZE !== 0) {
    throw new Error(
      `BUG: blockOffset ${blockOffset} is not aligned to CHUNK_SIZE ${CHUNK_SIZE}`
    );
  }

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
          limit: CHUNK_SIZE,  // Always 128KB - never changes
          precise: true,
        })
      );

      if (result instanceof Api.upload.File) {
        return new Uint8Array(result.bytes);
      }

      if (result instanceof Api.upload.FileCdnRedirect) {
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

      throw new Error("Unknown result type from GetFile");
    } catch (err: any) {
      lastError = err;
      const msg = err?.message || err?.errorMessage || "";

      if (msg.includes("FLOOD")) {
        const wait = RETRY_DELAY * Math.pow(2, attempt);
        console.warn(`⏳ Flood wait ${wait}ms...`);
        await sleep(wait);
        continue;
      }

      if (msg.includes("FILE_REFERENCE")) {
        console.warn("🔄 File reference expired, clearing cache...");
        for (const [key, val] of fileInfoCache.entries()) {
          if (val.id === info.id) fileInfoCache.delete(key);
        }
        throw new Error("FILE_REFERENCE_EXPIRED: Please retry the request");
      }

      // Don't retry other errors
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
      // Find the first aligned block that contains `start`
      let currentBlockOffset = getBlockOffset(start);
      let bytesSent = 0;
      const totalNeeded = end - start + 1;

      console.log(
        `🌊 Stream: [${start}-${end}] = ${formatBytes(totalNeeded)}\n` +
        `   First block offset: ${currentBlockOffset} ` +
        `(block #${currentBlockOffset / CHUNK_SIZE})`
      );

      try {
        while (bytesSent < totalNeeded) {
          // Stop if browser disconnected
          if (cancelled) {
            console.log("  🛑 Cancelled, stopping");
            return;
          }

          console.log(
            `  📥 block=${currentBlockOffset} (${formatBytes(CHUNK_SIZE)}) | ` +
            `sent=${formatBytes(bytesSent)}/${formatBytes(totalNeeded)}`
          );

          // Fetch the aligned 128KB block from Telegram
          const block = await fetchBlock(info, currentBlockOffset);

          if (block.length === 0) {
            console.warn("  ⚠️  Empty block, stopping");
            break;
          }

          // Check again after async fetch
          if (cancelled) {
            console.log("  🛑 Cancelled after fetch, discarding");
            return;
          }

          // Block covers bytes [currentBlockOffset, currentBlockOffset + block.length - 1]
          // We need bytes [start, end]
          // Calculate the overlap
          const blockAbsStart = currentBlockOffset;
          const blockAbsEnd = currentBlockOffset + block.length - 1;

          const overlapStart = Math.max(blockAbsStart, start);
          const overlapEnd = Math.min(blockAbsEnd, end);

          if (overlapStart <= overlapEnd) {
            // Slice only the bytes we need from this block
            const sliceFrom = overlapStart - blockAbsStart;
            const sliceTo = overlapEnd - blockAbsStart + 1;
            const slice = block.slice(sliceFrom, sliceTo);

            try {
              controller.enqueue(slice);
              bytesSent += slice.length;
            } catch (e: any) {
              if (e?.code === "ERR_INVALID_STATE") {
                console.log("  🛑 Controller closed (seek/tab close)");
                return;
              }
              throw e;
            }
          }

          // Move to next block
          currentBlockOffset += CHUNK_SIZE;

          // EOF: block was smaller than CHUNK_SIZE
          if (block.length < CHUNK_SIZE) {
            console.log("  📄 EOF");
            break;
          }
        }

        if (!cancelled) {
          controller.close();
          console.log(`  ✅ Done: sent ${formatBytes(bytesSent)}`);
        }
      } catch (err: any) {
        if (cancelled) {
          console.log("  🛑 Error after cancel:", err.message);
          return;
        }
        console.error("  ❌ Stream error:", err.message);
        try {
          controller.error(err);
        } catch {
          // Already closed
        }
      }
    },

    cancel(reason) {
      cancelled = true;
      console.log(`  🛑 Browser cancelled: ${reason || "seek/close"}`);
    },
  });
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

  try {
    const info = await getFileInfo(messageId, channelId);

    if (request.method === "HEAD") {
      return new Response(null, {
        status: 200,
        headers: {
          "Content-Type": info.mimeType,
          "Content-Length": info.size.toString(),
          "Accept-Ranges": "bytes",
          "Access-Control-Allow-Origin": "*",
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
      start = 0;
      end = info.size - 1;
    }

    // Cap response size
    const cappedEnd = Math.min(end, start + MAX_RESPONSE_SIZE - 1);
    const contentLength = cappedEnd - start + 1;

    console.log(
      `\n🎬 msg=${messageId} | ` +
      `[${start}-${cappedEnd}] | ` +
      `${formatBytes(contentLength)} | ` +
      `file=${formatBytes(info.size)}`
    );

    const stream = createStream(info, start, cappedEnd);

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
    console.error("❌ Handler error:", err.message);
    return jsonError(500, err.message || "Internal Server Error");
  }
}

// ─── Main Router ──────────────────────────────────────────────────────────────

async function handleRequest(request: Request): Promise<Response> {
  const url = new URL(request.url);

  if (request.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers": "Range, Content-Type",
        "Access-Control-Expose-Headers":
          "Content-Range, Accept-Ranges, Content-Length, Content-Type",
        "Access-Control-Max-Age": "86400",
      },
    });
  }

  if (url.pathname === "/health") {
    return new Response(
      JSON.stringify({
        status: "ok",
        connected: telegramClient?.connected ?? false,
        cachedFiles: fileInfoCache.size,
        chunkSize: formatBytes(CHUNK_SIZE),
        maxResponse: formatBytes(MAX_RESPONSE_SIZE),
      }),
      { headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } }
    );
  }

  const streamMatch = url.pathname.match(/^\/stream\/(\d+)$/);
  if (streamMatch) return handleStreamRequest(request, streamMatch[1]);

  const infoMatch = url.pathname.match(/^\/info\/(\d+)$/);
  if (infoMatch) {
    const channelId = url.searchParams.get("channel") || CHANNEL_ID;
    try {
      const info = await getFileInfo(infoMatch[1], channelId);
      return new Response(
        JSON.stringify({
          messageId: infoMatch[1],
          size: info.size,
          sizeFormatted: formatBytes(info.size),
          mimeType: info.mimeType,
          dcId: info.dcId,
          streamUrl: `/stream/${infoMatch[1]}`,
        }),
        { headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } }
      );
    } catch (err: any) {
      return jsonError(500, err.message);
    }
  }

  return jsonError(404, "Not Found");
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function parseRangeHeader(
  header: string,
  fileSize: number
): { start: number; end: number } | null {
  const match = header.match(/bytes=(\d*)-(\d*)/);
  if (!match) return null;

  let start: number;
  let end: number;

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
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(2)} ${units[i]}`;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function jsonError(status: number, message: string): Response {
  return new Response(JSON.stringify({ error: message }), {
    status,
    headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
  });
}

// ─── Start ────────────────────────────────────────────────────────────────────

console.log(`\n🚀 Telegram Streaming Service`);
console.log(`   Port      : ${PORT}`);
console.log(`   Chunk     : ${formatBytes(CHUNK_SIZE)} (fixed, always aligned)`);
console.log(`   Max Resp  : ${formatBytes(MAX_RESPONSE_SIZE)}`);
console.log(`   API ID    : ${API_ID || "⚠️  NOT SET"}`);

serve({ port: PORT, fetch: handleRequest, idleTimeout: 120 });

console.log(`✅ Ready → http://localhost:${PORT}\n`);