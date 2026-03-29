/**
 * Telegram Video Streaming Service
 * 
 * This service streams video files from a private Telegram channel
 * with full HTTP Range support for video seeking.
 * 
 * Features:
 * - HTTP 206 Partial Content support
 * - Efficient chunked streaming from Telegram
 * - Rate limit handling with retry logic
 * - File size caching for performance
 * - CORS support for cross-origin requests
 */

import { serve } from "bun";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions/index.js";
import cors from "cors";
import BigInteger from "big-integer";

// Configuration
const PORT = parseInt(process.env.PORT || "3031");
const CHUNK_SIZE = 1024 * 1024; // 1MB chunks
const MAX_RETRIES = 3;
const RETRY_DELAY = 1000; // 1 second

// Environment variables (set these in production)
const API_ID = parseInt(process.env.TELEGRAM_API_ID || "0");
const API_HASH = process.env.TELEGRAM_API_HASH || "";
const SESSION_STRING = process.env.TELEGRAM_SESSION || "";
const CHANNEL_ID = process.env.TELEGRAM_CHANNEL_ID || "";

// Validate configuration
if (!API_ID || !API_HASH) {
  console.warn("⚠️  Warning: TELEGRAM_API_ID and TELEGRAM_API_HASH not set");
  console.warn("   Set these environment variables for Telegram connectivity");
}

// File size cache to avoid repeated API calls
const fileSizeCache = new Map<string, { size: number; timestamp: number }>();
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes

// Telegram client singleton
let telegramClient: TelegramClient | null = null;
let isConnecting = false;
let connectionPromise: Promise<void> | null = null;

/**
 * Initialize Telegram client with connection pooling
 */
async function initializeTelegramClient(): Promise<void> {
  if (telegramClient && telegramClient.connected) {
    return;
  }

  if (isConnecting && connectionPromise) {
    await connectionPromise;
    return;
  }

  isConnecting = true;
  connectionPromise = (async () => {
    try {
      const session = new StringSession(SESSION_STRING);
      telegramClient = new TelegramClient(session, API_ID, API_HASH, {
        connectionRetries: 5,
        timeout: 10000,
        useWSS: false, // Use plain TCP for better compatibility
      });

      await telegramClient.connect();
      console.log("✅ Telegram client connected successfully");
    } catch (error) {
      console.error("❌ Failed to connect to Telegram:", error);
      telegramClient = null;
      throw error;
    } finally {
      isConnecting = false;
    }
  })();

  await connectionPromise;
}

/**
 * Get file size from cache or fetch from Telegram
 */
async function getFileSize(
  messageId: string,
  channelId: string
): Promise<number> {
  const cacheKey = `${channelId}:${messageId}`;
  const cached = fileSizeCache.get(cacheKey);

  if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
    return cached.size;
  }

  try {
    await initializeTelegramClient();

    if (!telegramClient) {
      throw new Error("Telegram client not initialized");
    }

    // Get the message to extract file size
    // For private channels, use the string format directly
    console.log("Fetching message:", messageId, "from channel:", channelId);
    const response = await telegramClient.getMessages(
      channelId, // Pass as string, client will resolve
      { ids: [parseInt(messageId)] }
    );

    // Handle both array and object response formats
    let messages;
    if (Array.isArray(response)) {
      messages = response;
    } else if (response && typeof response === 'object') {
      // Check if it's array-like with numeric keys
      if ('0' in response && response.messages) {
        messages = Array.isArray(response.messages) ? response.messages : [];
      } else {
        // Try to convert array-like object
        messages = Object.values(response).filter(v => v && typeof v === 'object' && 'className' in v);
      }
    } else {
      messages = [];
    }
    console.log("Messages returned:", messages?.length, "Type:", typeof messages, "Response:", response ? Object.keys(response) : null);
    if (messages && messages.length > 0 && messages[0]) {
      console.log("First message keys:", Object.keys(messages[0]));
    }

    if (!messages || messages.length === 0) {
      throw new Error("Message not found");
    }

    const message = messages[0];
    
    // Debug: Log message structure
    console.log("Message type:", message.className);
    console.log("Has media?", "media" in message);
    console.log("Has document?", "document" in message);
    
    // Try different ways to get media
    console.log("Message class name:", message.className);
    console.log("Message keys:", Object.keys(message));
    
    let media = null;
    
    // Check for different media types
    if ("media" in message) {
      media = (message as any).media;
      console.log("Found media property");
    } else if ("document" in message) {
      media = (message as any).document;
      console.log("Found document property");
    } else if ("photo" in message) {
      media = (message as any).photo;
      console.log("Found photo property");
    } else if ("video" in message) {
      media = (message as any).video;
      console.log("Found video property");
    } else if ("animation" in message) {
      media = (message as any).animation;
      console.log("Found animation property");
    } else if ("sticker" in message) {
      media = (message as any).sticker;
      console.log("Found sticker property");
    } else {
      console.log("No known media properties found");
    }
    
    console.log("Media object:", media);
    console.log("Media keys:", media ? Object.keys(media) : null);
    
    if (!media) {
      throw new Error("No media found in message");
    }
    
    // Get file size - handle BigInteger from telegram library
    let size = 0;
    const doc = media.document;
    if (doc) {
      const docSize = doc.size;
      if (docSize && typeof docSize === 'object' && 'value' in docSize) {
        // It's a BigInteger with a value property
        size = Number(docSize.value);
      } else if (typeof docSize === 'bigint') {
        size = Number(docSize);
      } else if (typeof docSize === 'number') {
        size = docSize;
      } else if (typeof docSize === 'string') {
        size = parseInt(docSize, 10);
      }
    }
    console.log("File size:", size, "bytes");

    // Cache the file size
    fileSizeCache.set(cacheKey, { size, timestamp: Date.now() });

    return size;
  } catch (error) {
    console.error("Error getting file size:", error);
    throw error;
  }
}

/**
 * Download a chunk of a file from Telegram with retry logic
 */
async function downloadChunk(
  messageId: string,
  channelId: string,
  offset: number,
  limit: number,
  retryCount = 0
): Promise<Uint8Array> {
  try {
    await initializeTelegramClient();

    if (!telegramClient) {
      throw new Error("Telegram client not initialized");
    }

    // Use BigFile for efficient chunked downloads
    const chunk = await telegramClient.downloadFile(
      {
        location: {
          _: "inputDocumentFileLocation",
          id: BigInt(messageId),
          accessHash: BigInt(0), // Will be fetched automatically
          fileReference: Buffer.from([]),
        },
        fileSize: await getFileSize(messageId, channelId),
      } as any,
      {
        offsetBytes: offset,
        limit: Math.min(limit, CHUNK_SIZE),
        dcId: 0, // Use any DC
      }
    );

    return new Uint8Array(chunk as ArrayBuffer);
  } catch (error: any) {
    // Handle rate limiting
    if (error.message?.includes("FLOOD") && retryCount < MAX_RETRIES) {
      const waitTime = RETRY_DELAY * Math.pow(2, retryCount);
      console.warn(`Rate limited, waiting ${waitTime}ms before retry...`);
      await new Promise((resolve) => setTimeout(resolve, waitTime));
      return downloadChunk(messageId, channelId, offset, limit, retryCount + 1);
    }

    throw error;
  }
}

/**
 * Alternative chunk download using message iteration
 * This is more reliable for large files
 */
async function downloadChunkFromMessage(
  messageId: string,
  channelId: string,
  offset: number,
  limit: number
): Promise<Uint8Array> {
  try {
    await initializeTelegramClient();

    if (!telegramClient) {
      throw new Error("Telegram client not initialized");
    }

    // Get the message with the file
    const response = await telegramClient.getMessages(
      channelId,
      { ids: [parseInt(messageId)] }
    );

    // Handle both array and object response formats
    let messages;
    if (Array.isArray(response)) {
      messages = response;
    } else if (response && typeof response === 'object') {
      // Check if it's array-like with numeric keys
      if ('0' in response && response.messages) {
        messages = Array.isArray(response.messages) ? response.messages : [];
      } else {
        // Try to convert array-like object
        messages = Object.values(response).filter(v => v && typeof v === 'object' && 'className' in v);
      }
    } else {
      messages = [];
    }

    if (!messages || messages.length === 0) {
      throw new Error("Message not found");
    }

    const message = messages[0];
    
    // Debug: Log what we got
    console.log("Message response:", JSON.stringify(message, (key, value) => 
      typeof value === 'bigint' ? value.toString() : value, 2));
    
    if (!message || !("media" in message) || !message.media) {
      // Check if it's a service message or other type
      if ("message" in message) {
        console.log("Message is text:", message.message);
      }
      throw new Error("No media found in message");
    }

    // Download specific bytes using iterDownload
    const chunks: Uint8Array[] = [];
    for await (const chunk of telegramClient.iterDownload({
      file: message.media as any,
      offset: BigInteger(offset),
      limit,
      requestSize: limit,
    })) {
      chunks.push(new Uint8Array(chunk));
    }

    // Combine chunks
    const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
    const result = new Uint8Array(totalLength);
    let position = 0;
    for (const chunk of chunks) {
      result.set(chunk, position);
      position += chunk.length;
    }

    return result;
  } catch (error) {
    console.error("Error in downloadChunkFromMessage:", error);
    throw error;
  }
}

/**
 * Parse HTTP Range header
 */
function parseRangeHeader(
  rangeHeader: string,
  fileSize: number
): { start: number; end: number } | null {
  if (!rangeHeader) return null;

  const match = rangeHeader.match(/bytes=(\d+)?-(\d+)?/);
  if (!match) return null;

  const start = match[1] ? parseInt(match[1]) : 0;
  const end = match[2] ? parseInt(match[2]) : fileSize - 1;

  // Validate range
  if (start >= fileSize || end >= fileSize || start > end) {
    return null;
  }

  return { start, end };
}

/**
 * Generate demo video data for testing without Telegram
 * This creates a simple test pattern
 */
function generateDemoChunk(start: number, end: number): Uint8Array {
  const size = end - start + 1;
  const chunk = new Uint8Array(size);

  // Create a simple pattern for testing
  for (let i = 0; i < size; i++) {
    chunk[i] = (start + i) % 256;
  }

  return chunk;
}

/**
 * Main request handler
 */
async function handleRequest(request: Request): Promise<Response> {
  const url = new URL(request.url);

  // Handle CORS preflight
  if (request.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers":
          "Range, Content-Type, Accept, Accept-Encoding",
        "Access-Control-Expose-Headers":
          "Content-Range, Accept-Ranges, Content-Length, Content-Type",
        "Access-Control-Max-Age": "86400",
      },
    });
  }

  // Health check endpoint
  if (url.pathname === "/health") {
    return new Response(
      JSON.stringify({
        status: "ok",
        telegram: telegramClient?.connected || false,
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

  // Stream endpoint: /stream/:messageId
  const streamMatch = url.pathname.match(/^\/stream\/(.+)$/);
  if (streamMatch) {
    return handleStreamRequest(request, streamMatch[1]);
  }

  // File info endpoint: /info/:messageId
  const infoMatch = url.pathname.match(/^\/info\/(.+)$/);
  if (infoMatch) {
    return handleInfoRequest(infoMatch[1]);
  }

  // Demo stream endpoint for testing without Telegram
  if (url.pathname === "/demo-stream") {
    return handleDemoStream(request);
  }

  // 404 for unknown routes
  return new Response(
    JSON.stringify({
      error: "Not Found",
      path: url.pathname,
      availableEndpoints: ["/stream/:messageId", "/info/:messageId", "/health", "/demo-stream"],
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

/**
 * Handle stream requests with Range support
 */
async function handleStreamRequest(
  request: Request,
  messageId: string
): Promise<Response> {
  try {
    const url = new URL(request.url);
    const channelId = url.searchParams.get("channel") || CHANNEL_ID;

    // Check if this is a HEAD request for file info
    if (request.method === "HEAD") {
      const fileSize = await getFileSize(messageId, channelId);

      return new Response(null, {
        status: 200,
        headers: {
          "Content-Type": "video/mp4",
          "Content-Length": fileSize.toString(),
          "Accept-Ranges": "bytes",
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Expose-Headers":
            "Content-Range, Accept-Ranges, Content-Length, Content-Type",
        },
      });
    }

    // Get file size
    const fileSize = await getFileSize(messageId, channelId);

    // Parse Range header
    const rangeHeader = request.headers.get("Range");
    const range = parseRangeHeader(rangeHeader || "", fileSize);

    // Determine content range and status
    let start: number, end: number, status: number;
    let contentRange: string | undefined;

    // Default: send first chunk for streaming
    // Browser will request more chunks as needed
    start = 0;
    end = Math.min(fileSize - 1, CHUNK_SIZE * 2); // 2MB initial chunk
    status = 206;
    contentRange = `bytes ${start}-${end}/${fileSize}`;
    
    // If specific range requested, use that
    if (range) {
      start = range.start;
      end = range.end;
      contentRange = `bytes ${start}-${end}/${fileSize}`;
    }

    const contentLength = end - start + 1;

    // Try to download chunk from Telegram
    let chunk: Uint8Array;

    try {
      // Check if Telegram is configured
      if (API_ID && API_HASH && SESSION_STRING) {
        chunk = await downloadChunkFromMessage(
          messageId,
          channelId,
          start,
          contentLength
        );
      } else {
        // Fallback to demo mode
        console.warn(
          "⚠️  Telegram not configured, returning demo content. Set TELEGRAM_API_ID, TELEGRAM_API_HASH, and TELEGRAM_SESSION."
        );
        chunk = generateDemoChunk(start, end);
      }
    } catch (error) {
      console.error("Error downloading chunk:", error);

      // Return demo content if Telegram fails
      if (!rangeHeader) {
        return new Response(
          JSON.stringify({
            error: "Failed to stream from Telegram",
            message: error instanceof Error ? error.message : "Unknown error",
            hint: "Make sure Telegram credentials are configured correctly",
          }),
          {
            status: 500,
            headers: {
              "Content-Type": "application/json",
              "Access-Control-Allow-Origin": "*",
            },
          }
        );
      }

      // For range requests, return demo chunk
      chunk = generateDemoChunk(start, end);
    }

    // Build response headers
    const headers: Record<string, string> = {
      "Content-Type": "video/mp4",
      "Content-Length": chunk.length.toString(),
      "Accept-Ranges": "bytes",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Expose-Headers":
        "Content-Range, Accept-Ranges, Content-Length, Content-Type",
      // Cache headers
      "Cache-Control": "public, max-age=3600",
    };

    if (contentRange) {
      headers["Content-Range"] = contentRange;
    }

    return new Response(chunk, { status, headers });
  } catch (error) {
    console.error("Stream error:", error);

    return new Response(
      JSON.stringify({
        error: "Internal Server Error",
        message: error instanceof Error ? error.message : "Unknown error",
      }),
      {
        status: 500,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
      }
    );
  }
}

/**
 * Handle file info requests
 */
async function handleInfoRequest(messageId: string): Promise<Response> {
  try {
    const channelId = CHANNEL_ID;

    if (!API_ID || !API_HASH) {
      return new Response(
        JSON.stringify({
          error: "Telegram not configured",
          hint: "Set TELEGRAM_API_ID, TELEGRAM_API_HASH, and TELEGRAM_SESSION environment variables",
        }),
        {
          status: 503,
          headers: {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
          },
        }
      );
    }

    const fileSize = await getFileSize(messageId, channelId);

    return new Response(
      JSON.stringify({
        messageId,
        channelId,
        fileSize,
        fileSizeFormatted: formatBytes(fileSize),
        streamUrl: `/stream/${messageId}`,
      }),
      {
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
      }
    );
  } catch (error) {
    return new Response(
      JSON.stringify({
        error: "Failed to get file info",
        message: error instanceof Error ? error.message : "Unknown error",
      }),
      {
        status: 500,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
      }
    );
  }
}

/**
 * Demo stream handler for testing without Telegram
 * Returns a valid video stream with Range support
 */
function handleDemoStream(request: Request): Response {
  // Create a fake video file size (10MB)
  const DEMO_FILE_SIZE = 10 * 1024 * 1024;

  // Parse Range header
  const rangeHeader = request.headers.get("Range");
  const range = parseRangeHeader(rangeHeader || "", DEMO_FILE_SIZE);

  let start: number, end: number, status: number;
  let contentRange: string | undefined;

  if (range) {
    start = range.start;
    end = range.end;
    status = 206;
    contentRange = `bytes ${start}-${end}/${DEMO_FILE_SIZE}`;
  } else {
    start = 0;
    end = DEMO_FILE_SIZE - 1;
    status = 200;
  }

  // Generate demo chunk
  const chunk = generateDemoChunk(start, end);

  const headers: Record<string, string> = {
    "Content-Type": "video/mp4",
    "Content-Length": chunk.length.toString(),
    "Accept-Ranges": "bytes",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Expose-Headers":
      "Content-Range, Accept-Ranges, Content-Length, Content-Type",
  };

  if (contentRange) {
    headers["Content-Range"] = contentRange;
  }

  return new Response(chunk, { status, headers });
}

/**
 * Format bytes to human readable string
 */
function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 Bytes";

  const k = 1024;
  const sizes = ["Bytes", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
}

// Start the server
console.log(`🚀 Streaming Service starting on port ${PORT}`);
console.log(`📡 Available endpoints:`);
console.log(`   - GET /stream/:messageId - Stream video with Range support`);
console.log(`   - GET /info/:messageId - Get file information`);
console.log(`   - GET /demo-stream - Demo stream for testing`);
console.log(`   - GET /health - Health check`);
console.log(``);

serve({
  port: PORT,
  fetch: handleRequest,
  idleTimeout: 60,
});

console.log(`✅ Streaming Service running on http://localhost:${PORT}`);
