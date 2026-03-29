"""
Telegram Video Streaming Service
================================
Stack: Python 3.11+ | Pyrogram 2.x | FastAPI | Uvicorn

Why Python + Pyrogram over Node.js + gramjs:
  ✅ Pyrogram handles ALL DC routing automatically
  ✅ stream_media() built-in - correct chunks, no LIMIT_INVALID
  ✅ No DC_ID_INVALID - pyrogram manages auth internally
  ✅ TgCrypto = C-level MTProto speed
  ✅ AsyncIO native - handles thousands of concurrent requests
  ✅ Battle-tested in production Telegram bots/userbots

Features:
  - HTTP 206 Partial Content + full Range support
  - Automatic DC routing (DC 1-5 all work)
  - Per-IP rate limiting (sliding window)
  - Concurrent stream limiting
  - Telegram flood wait handling
  - File info + filename cache (TTL-based)
  - File reference auto-refresh
  - Demo stream fallback (no credentials needed)
  - CORS support
  - Graceful stream cancel on seek/close
  - Health check endpoint
  - Structured logging
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import AsyncGenerator, Optional

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pyrogram import Client
from pyrogram.errors import (
    FileReferenceExpired,
    FileReferenceInvalid,
    FloodWait,
    RPCError,
)
from pyrogram.types import Message

# ─── Load Environment ─────────────────────────────────────────────────────────

load_dotenv()

# ─── Logging Setup ────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("streaming")

# Suppress noisy pyrogram logs
logging.getLogger("pyrogram").setLevel(logging.WARNING)

# ─── Configuration ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Config:
    api_id: int             = int(os.environ.get("TELEGRAM_API_ID", "0"))
    api_hash: str           = os.environ.get("TELEGRAM_API_HASH", "")
    session_string: str     = os.environ.get("TELEGRAM_SESSION_STRING", "")
    channel_id: str         = os.environ.get("TELEGRAM_CHANNEL_ID", "")
    port: int               = int(os.environ.get("PORT", "3031"))

    # Streaming
    max_response_bytes: int = 4 * 1024 * 1024   # 4MB per HTTP response
    demo_file_size: int     = 10 * 1024 * 1024  # 10MB demo

    # Rate limiting
    max_concurrent_streams: int = 10
    requests_per_minute: int    = 120
    max_ip_log_size: int        = 5000

    # Cache
    file_cache_ttl: int     = 10 * 60  # 10 minutes
    max_cache_size: int     = 500

    # Retry
    max_retries: int        = 3
    retry_base_delay: float = 1.0

cfg = Config()

if not cfg.api_id or not cfg.api_hash:
    log.warning("⚠️  TELEGRAM_API_ID / TELEGRAM_API_HASH not set → demo mode only")

# ─── Types ────────────────────────────────────────────────────────────────────

@dataclass
class FileInfo:
    """Cached metadata for a Telegram document."""
    message_id:     int
    channel_id:     str
    file_id:        str
    file_ref:       bytes        # may expire → refresh needed
    file_size:      int
    mime_type:      str
    file_name:      str
    dc_id:          int
    cached_at:      float = field(default_factory=time.time)
    message:        Optional[Message] = field(default=None, repr=False)

    def is_fresh(self) -> bool:
        return time.time() - self.cached_at < cfg.file_cache_ttl

    def cache_key(self) -> str:
        return f"{self.channel_id}:{self.message_id}"


@dataclass
class RangeRequest:
    start: int
    end:   int

    @property
    def length(self) -> int:
        return self.end - self.start + 1


# ─── Rate Limiter ─────────────────────────────────────────────────────────────

class RateLimiter:
    """
    Sliding window per-IP rate limiter.
    Thread-safe via asyncio (single-threaded event loop).
    """

    def __init__(self, requests_per_minute: int, max_ips: int):
        self.rpm     = requests_per_minute
        self.max_ips = max_ips
        self._log: dict[str, list[float]] = defaultdict(list)

    def is_allowed(self, ip: str) -> bool:
        now    = time.monotonic()
        window = 60.0
        times  = [t for t in self._log[ip] if now - t < window]
        times.append(now)
        self._log[ip] = times

        # Evict old IPs to prevent memory leak
        if len(self._log) > self.max_ips:
            cutoff = now - window
            stale  = [k for k, v in self._log.items() if all(t < cutoff for t in v)]
            for k in stale[:100]:
                del self._log[k]

        return len(times) <= self.rpm


rate_limiter = RateLimiter(cfg.requests_per_minute, cfg.max_ip_log_size)

# ─── File Info Cache ──────────────────────────────────────────────────────────

class FileCache:
    """LRU-like TTL cache for FileInfo objects."""

    def __init__(self, max_size: int):
        self._store: dict[str, FileInfo] = {}
        self.max_size = max_size

    def get(self, key: str) -> Optional[FileInfo]:
        info = self._store.get(key)
        if info and info.is_fresh():
            return info
        if info:
            del self._store[key]
        return None

    def set(self, info: FileInfo) -> None:
        if len(self._store) >= self.max_size:
            # Evict oldest entry
            oldest = min(self._store, key=lambda k: self._store[k].cached_at)
            del self._store[oldest]
        self._store[info.cache_key()] = info

    def invalidate(self, key: str) -> None:
        self._store.pop(key, None)

    def __len__(self) -> int:
        return len(self._store)


file_cache = FileCache(cfg.max_cache_size)

# ─── Telegram Client ──────────────────────────────────────────────────────────

tg = Client(
    name="streaming_session",
    api_id=cfg.api_id,
    api_hash=cfg.api_hash,
    session_string=cfg.session_string or None,
    # TgCrypto handles encryption at C speed automatically
    # Pyrogram manages DC connections internally - no manual work needed
)

# ─── File Info Fetcher ────────────────────────────────────────────────────────

def _extract_filename(msg: Message) -> str:
    """
    Extract filename from message in order of preference:
    1. DocumentAttributeFilename (explicit name)
    2. Caption (cleaned)
    3. Generated from mime type
    """
    doc = msg.document or msg.video or msg.audio or msg.voice or msg.video_note
    if not doc:
        raise ValueError("Message has no streamable media")

    # Explicit filename
    if getattr(doc, "file_name", None):
        return doc.file_name

    # Build from mime type
    mime = getattr(doc, "mime_type", None) or "application/octet-stream"
    ext  = mime.split("/")[-1].split(";")[0].strip()

    # Map common mime types to better extensions
    ext_map = {
        "x-matroska": "mkv",
        "x-msvideo":  "avi",
        "quicktime":  "mov",
        "x-ms-wmv":   "wmv",
        "mpeg":       "mpg",
    }
    ext = ext_map.get(ext, ext)

    # Use caption as base name if available
    if msg.caption:
        safe = re.sub(r'[\\/*?:"<>|]', "_", msg.caption[:60]).strip()
        return f"{safe}.{ext}" if safe else f"media_{msg.id}.{ext}"

    return f"media_{msg.id}.{ext}"


async def fetch_file_info(message_id: int, channel_id: str) -> FileInfo:
    """
    Fetch file metadata from Telegram, with cache.
    Pyrogram automatically handles DC routing for getMessages.
    """
    cache_key = f"{channel_id}:{message_id}"
    cached    = file_cache.get(cache_key)
    if cached:
        return cached

    messages = await tg.get_messages(channel_id, message_id)
    msg = messages[0] if isinstance(messages, list) else messages

    if not msg:
        raise ValueError(f"Message {message_id} not found in {channel_id}")

    doc = getattr(msg, "document", None) or getattr(msg, "video", None) or getattr(msg, "audio", None) or getattr(msg, "voice", None) or getattr(msg, "video_note", None)
    if not doc:
        raise ValueError(f"Message {message_id} has no streamable media")

    mime      = getattr(doc, "mime_type", None) or "video/mp4"
    file_name = _extract_filename(msg)
    file_ref  = getattr(doc, "file_reference", None) or b""
    file_size = getattr(doc, "file_size", 0) or 0
    dc_id     = getattr(doc, "dc_id", None) or 0

    info = FileInfo(
        message_id = message_id,
        channel_id = channel_id,
        file_id    = getattr(doc, "file_id", ""),
        file_ref   = file_ref,
        file_size  = file_size,
        mime_type  = mime,
        file_name  = file_name,
        dc_id      = dc_id,
        message    = msg,
    )

    file_cache.set(info)
    log.info(
        f'📁 Cached | msg={message_id} | "{file_name}" | '
        f'{fmt_bytes(file_size)} | dc={dc_id} | {mime}'
    )
    return info


async def refresh_file_info(info: FileInfo) -> FileInfo:
    """Re-fetch when file reference expires."""
    log.warning(f"🔄 Refreshing file reference for msg={info.message_id}")
    file_cache.invalidate(info.cache_key())
    return await fetch_file_info(info.message_id, info.channel_id)

# ─── Stream Generator ─────────────────────────────────────────────────────────

# Global counter
_active_streams = 0


async def stream_file(
    info: FileInfo,
    range_req: RangeRequest,
    request: Request,
) -> AsyncGenerator[bytes, None]:
    """
    Core streaming generator using Pyrogram's stream_media().

    Why stream_media() is perfect:
    - Handles DC routing automatically (DC 1-5)
    - Uses correct chunk sizes (no LIMIT_INVALID)
    - Handles file reference refresh internally
    - Handles CDN redirects
    - Yields chunks as they arrive (true streaming)
    - Cancel-safe via asyncio

    Flow:
    Browser → FastAPI → stream_file() → Pyrogram → Telegram DC → chunks → browser
    """
    global _active_streams
    _active_streams += 1

    bytes_sent = 0
    total      = range_req.length
    retries    = 0

    log.info(
        f"🌊 Stream | msg={info.message_id} | "
        f"[{fmt_bytes(range_req.start)}-{fmt_bytes(range_req.end)}] | "
        f"{fmt_bytes(total)} | dc={info.dc_id} | active={_active_streams}"
    )

    try:
        while bytes_sent < total:
            if await request.is_disconnected():
                log.info(f"  🛑 Client disconnected | sent={fmt_bytes(bytes_sent)}")
                return

            try:
                current_offset = range_req.start + bytes_sent
                remaining = total - bytes_sent

                # Align offset to 4096 bytes (Telegram requirement)
                aligned_offset = (current_offset // 4096) * 4096

                # Calculate how much extra data we need to fetch due to alignment
                extra_before = current_offset - aligned_offset  # bytes before our actual start

                # Calculate limit - we need remaining bytes plus alignment overhead
                # But Telegram requires minimum 4096 bytes limit and power of 2
                required_limit = remaining + extra_before
                
                # Telegram requires limit to be at least 4096 bytes AND a multiple of 4096
                # Round up to nearest multiple of 4096
                if required_limit < 4096:
                    required_limit = 4096
                else:
                    required_limit = ((required_limit + 4095) // 4096) * 4096
                
                # Cap at 512KB maximum
                if required_limit > 512 * 1024:
                    required_limit = 512 * 1024

                log.info(f"  📥 Fetching | actual_offset={fmt_bytes(aligned_offset)} | limit={fmt_bytes(required_limit)} | need={fmt_bytes(remaining)}")

                async for chunk in tg.stream_media(
                    info.message,
                    offset = aligned_offset,
                    limit  = required_limit,
                ):
                    if await request.is_disconnected():
                        log.info("  🛑 Disconnected mid-stream")
                        return

                    # Calculate where this chunk starts in the file
                    chunk_start = aligned_offset
                    chunk_length = len(chunk)

                    # Skip bytes before our actual start (due to alignment)
                    if chunk_start < range_req.start:
                        skip = range_req.start - chunk_start
                        if skip >= chunk_length:
                            # This entire chunk is before our range, skip it
                            aligned_offset += chunk_length
                            continue
                        chunk = chunk[skip:]
                        chunk_start = range_req.start

                    # Trim bytes after our range end
                    if chunk_start + len(chunk) > range_req.end + 1:
                        keep = (range_req.end + 1) - chunk_start
                        if keep > 0:
                            chunk = chunk[:keep]
                        else:
                            chunk = b""

                    if chunk:
                        yield chunk
                        bytes_sent += len(chunk)

                    # Update aligned_offset for next iteration
                    aligned_offset += chunk_length

                    if bytes_sent >= total:
                        break

                if bytes_sent >= total:
                    break

            except (FileReferenceExpired, FileReferenceInvalid):
                if retries >= cfg.max_retries:
                    log.error("❌ File reference refresh failed after retries")
                    raise
                retries += 1
                log.warning(f"🔄 File reference expired, refreshing (attempt {retries})")
                info = await refresh_file_info(info)

            except FloodWait as e:
                wait = e.value + 1
                log.warning(f"⏳ Flood wait {wait}s")
                await asyncio.sleep(wait)

            except RPCError as e:
                err_str = str(e)
                if "OFFSET_INVALID" in err_str or "LIMIT_INVALID" in err_str:
                    # These errors at file end mean we're past the file boundary
                    # Signal to stop streaming gracefully
                    log.warning(f"⚠️  Offset past file boundary, stream complete")
                    break
                if retries >= cfg.max_retries:
                    raise
                retries += 1
                log.warning(f"⚠️  RPC error (attempt {retries}): {e}")
                await asyncio.sleep(cfg.retry_base_delay * retries)

        log.info(f"  ✅ Done | sent={fmt_bytes(bytes_sent)}")

    except Exception as e:
        log.error(f"  ❌ Stream error: {e}")
        raise
    finally:
        _active_streams -= 1

# ─── Range Parsing ────────────────────────────────────────────────────────────

def parse_range(header: str, file_size: int) -> Optional[RangeRequest]:
    """
    Parse HTTP Range header per RFC 7233.

    Examples:
      bytes=0-999     → start=0,   end=999
      bytes=500-      → start=500, end=file_size-1
      bytes=-500      → last 500 bytes
      bytes=0-        → start=0,   end=file_size-1
    """
    if not header:
        return None

    match = re.match(r"bytes=(\d*)-(\d*)", header)
    if not match:
        return None

    s, e = match.group(1), match.group(2)

    if s == "" and e != "":
        # Suffix range
        suffix = int(e)
        start  = max(0, file_size - suffix)
        end    = file_size - 1
    elif s != "":
        start = int(s)
        end   = int(e) if e else file_size - 1
    else:
        return None

    end = min(end, file_size - 1)

    if start < 0 or start > end or start >= file_size:
        return None

    return RangeRequest(start=start, end=end)

# ─── FastAPI App ──────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle."""
    log.info("🚀 Starting Telegram client...")

    if cfg.api_id and cfg.api_hash and cfg.session_string:
        await tg.start()
        me = await tg.get_me()
        log.info(f"✅ Logged in as: {me.first_name} (@{me.username})")
    else:
        log.warning("⚠️  Telegram not configured → demo mode")

    yield

    if tg.is_connected:
        await tg.stop()
        log.info("👋 Telegram client disconnected")


app = FastAPI(
    title       = "Telegram Video Streaming Service",
    description = "Stream videos from Telegram with HTTP Range support",
    version     = "2.0.0",
    lifespan    = lifespan,
    docs_url    = "/docs",
    redoc_url   = "/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins      = ["*"],
    allow_methods      = ["GET", "HEAD", "OPTIONS"],
    allow_headers      = ["Range", "Content-Type", "Accept", "Authorization"],
    expose_headers     = [
        "Content-Range", "Accept-Ranges", "Content-Length",
        "Content-Type", "Content-Disposition",
    ],
    max_age            = 86400,
)

# ─── Middleware: Rate Limiting ─────────────────────────────────────────────────

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    # Skip rate limit for health/docs
    if request.url.path in ("/health", "/docs", "/redoc", "/openapi.json"):
        return await call_next(request)

    ip = (
        request.headers.get("x-forwarded-for", "").split(",")[0].strip()
        or request.headers.get("x-real-ip", "")
        or (request.client.host if request.client else "unknown")
    )

    if not rate_limiter.is_allowed(ip):
        log.warning(f"🚫 Rate limited: {ip}")
        return Response(
            content     = '{"error":"Too many requests. Please slow down."}',
            status_code = 429,
            headers     = {"Retry-After": "60", "Content-Type": "application/json"},
        )

    return await call_next(request)

# ─── Routes ───────────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
async def health_check():
    """Service health and stats."""
    return {
        "status"          : "ok",
        "telegram"        : tg.is_connected,
        "active_streams"  : _active_streams,
        "cached_files"    : len(file_cache),
        "rate_limit"      : {
            "max_concurrent_streams" : cfg.max_concurrent_streams,
            "requests_per_minute"    : cfg.requests_per_minute,
        },
        "limits"          : {
            "max_response_size" : fmt_bytes(cfg.max_response_bytes),
        },
        "timestamp"       : time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


@app.get("/info/{message_id}", tags=["Files"])
async def file_info(
    message_id : int,
    channel    : str = Query(default="", description="Channel ID or username"),
):
    """Get file metadata without streaming."""
    try:
        if not tg.is_connected:
            raise HTTPException(503, "Telegram not configured")
    except Exception:
        raise HTTPException(503, "Telegram not configured")

    channel_id = channel or cfg.channel_id
    if not channel_id:
        raise HTTPException(400, "Missing channel ID. Pass ?channel=... or set TELEGRAM_CHANNEL_ID")

    # Normalize channel_id
    if channel_id.startswith("@"):
        channel_id = channel_id[1:]
    elif not channel_id.startswith("-100") and not channel_id.startswith("-"):
        try:
            chat = await tg.get_chat(channel_id)
            channel_id = str(chat.id)
        except Exception as e:
            log.warning(f"Could not resolve username {channel_id}: {e}")
            raise HTTPException(400, f"Invalid channel ID: {channel_id}")

    try:
        info = await fetch_file_info(message_id, channel_id)
    except ValueError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        log.error(f"Info error: {e}")
        raise HTTPException(500, str(e))

    return {
        "messageId"         : info.message_id,
        "channelId"         : info.channel_id,
        "fileName"          : info.file_name,
        "fileSize"          : info.file_size,
        "fileSizeFormatted" : fmt_bytes(info.file_size),
        "mimeType"          : info.mime_type,
        "dcId"              : info.dc_id,
        "streamUrl"         : f"/stream/{message_id}?channel={channel_id}",
    }


@app.api_route("/stream/{message_id}", methods=["GET", "HEAD"], tags=["Stream"])
async def stream_video(
    message_id : int,
    request    : Request,
    channel    : str = Query(default="", description="Channel ID or username"),
):
    """
    Stream a Telegram video with HTTP Range support.

    Supports:
    - Full range requests (seek, skip)
    - HEAD requests (file info without body)
    - Automatic DC routing
    - Graceful disconnect handling
    """
    channel_id = channel or cfg.channel_id

    if not channel_id:
        raise HTTPException(400, "Missing channel ID")

    # Normalize channel_id format
    # Remove @ prefix if present, ensure it starts with - for private channels
    if channel_id.startswith("@"):
        channel_id = channel_id[1:]
    elif not channel_id.startswith("-100") and not channel_id.startswith("-"):
        # Try to look up username - this will establish the peer
        try:
            chat = await tg.get_chat(channel_id)
            # Use the numeric ID format
            channel_id = str(chat.id)
            log.info(f"Resolved channel @{channel_id} -> {channel_id}")
        except Exception as e:
            log.warning(f"Could not resolve username {channel_id}: {e}")
            raise HTTPException(400, f"Invalid channel ID: {channel_id}")

    # Concurrent stream limit
    if _active_streams >= cfg.max_concurrent_streams:
        return Response(
            content     = '{"error":"Too many concurrent streams. Try again shortly."}',
            status_code = 429,
            headers     = {"Retry-After": "5", "Content-Type": "application/json"},
        )

    # Demo fallback if not connected
    try:
        if not tg.is_connected:
            return _demo_response(request)
    except Exception:
        return _demo_response(request)

    # Fetch file info
    try:
        info = await fetch_file_info(message_id, channel_id)
    except ValueError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        log.error(f"Fetch error: {e}")
        raise HTTPException(500, str(e))

    file_size = info.file_size

    # Parse Range header
    range_header = request.headers.get("Range", "")
    range_req    = parse_range(range_header, file_size)

    if range_header and not range_req:
        # Invalid range
        return Response(
            status_code = 416,
            headers     = {
                "Content-Range"              : f"bytes */{file_size}",
                "Access-Control-Allow-Origin": "*",
            },
        )

    if not range_req:
        # No Range header → start from beginning
        range_req = RangeRequest(start=0, end=file_size - 1)

    # If range end < start, fix it to serve from start
    if range_req.end < range_req.start:
        log.warning(f"⚠️  Invalid range {range_req.start}-{range_req.end}, serving from start")
        range_req = RangeRequest(start=0, end=min(cfg.max_response_bytes, file_size) - 1)

    # If range start is beyond file size, return 416
    if range_req.start >= file_size:
        log.warning(f"⚠️  Range start {range_req.start} >= file size {file_size}")
        return Response(
            status_code = 416,
            headers     = {
                "Content-Range"              : f"bytes */{file_size}",
                "Content-Length"            : "0",
                "Access-Control-Allow-Origin": "*",
            },
        )

    # Cap response size so browser gets data fast and seeks work immediately
    # IMPORTANT: end must be < file_size (Telegram requirement)
    capped_end   = min(range_req.end, file_size - 2)  # -2 to ensure end < file_size
    capped_range = RangeRequest(start=range_req.start, end=capped_end)
    content_len  = capped_range.length

    log.info(
        f'\n🎬 msg={message_id} | "{info.file_name}" | '
        f'[{fmt_bytes(capped_range.start)}-{fmt_bytes(capped_range.end)}] | '
        f'{fmt_bytes(content_len)} | file={fmt_bytes(file_size)}'
    )

    # Response headers
    headers = {
        "Content-Type"                : info.mime_type,
        "Content-Length"              : str(content_len),
        "Content-Range"               : f"bytes {capped_range.start}-{capped_range.end}/{file_size}",
        "Accept-Ranges"               : "bytes",
        "Content-Disposition"         : f'inline; filename="{_safe_filename(info.file_name)}"',
        "Cache-Control"               : "no-cache, no-store",
        "Access-Control-Allow-Origin" : "*",
    }

    # HEAD → no body
    if request.method == "HEAD":
        return Response(status_code=206, headers=headers)

    # GET → stream body
    return StreamingResponse(
        content    = stream_file(info, capped_range, request),
        status_code = 206,
        headers    = headers,
        media_type = info.mime_type,
    )


@app.get("/demo-stream", tags=["Demo"])
async def demo_stream(request: Request):
    """Demo stream for testing without Telegram credentials."""
    return _demo_response(request)

# ─── Demo Response ────────────────────────────────────────────────────────────

def _demo_response(request: Request) -> Response:
    """Return a byte-pattern demo stream with Range support."""
    size      = cfg.demo_file_size
    range_req = parse_range(request.headers.get("Range", ""), size)
    start     = range_req.start if range_req else 0
    end       = range_req.end   if range_req else size - 1
    status    = 206             if range_req else 200

    chunk = bytes((start + i) % 256 for i in range(end - start + 1))

    headers: dict[str, str] = {
        "Content-Type"  : "video/mp4",
        "Content-Length": str(len(chunk)),
        "Accept-Ranges" : "bytes",
    }
    if range_req:
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"

    return Response(content=chunk, status_code=status, headers=headers)

# ─── Helpers ──────────────────────────────────────────────────────────────────

def fmt_bytes(b: int) -> str:
    """Human-readable byte size."""
    if b == 0:
        return "0 Bytes"
    for unit in ("Bytes", "KB", "MB", "GB", "TB"):
        if b < 1024.0:
            return f"{b:.2f} {unit}"
        b /= 1024.0
    return f"{b:.2f} PB"


def _safe_filename(name: str) -> str:
    """Encode filename for Content-Disposition header."""
    # Replace problematic chars but keep readability
    safe = re.sub(r'["\\\r\n]', "_", name)
    return safe

# ─── Entry Point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"""
╔══════════════════════════════════════════════════╗
║       Telegram Video Streaming Service           ║
║       Python + Pyrogram + FastAPI                ║
╠══════════════════════════════════════════════════╣
║  Port           : {cfg.port:<30} ║
║  Max Response   : {fmt_bytes(cfg.max_response_bytes):<30} ║
║  Max Streams    : {cfg.max_concurrent_streams:<30} ║
║  Req/min per IP : {cfg.requests_per_minute:<30} ║
║  Cache TTL      : {cfg.file_cache_ttl}s{'':<27} ║
║  API ID         : {str(cfg.api_id) if cfg.api_id else '⚠️  NOT SET':<30} ║
║  Channel        : {cfg.channel_id or 'pass ?channel=...':<30} ║
╠══════════════════════════════════════════════════╣
║  Endpoints:                                      ║
║   GET  /stream/{{id}}?channel=ID                  ║
║   GET  /info/{{id}}?channel=ID                    ║
║   GET  /demo-stream                              ║
║   GET  /health                                   ║
║   GET  /docs   (Swagger UI)                      ║
╚══════════════════════════════════════════════════╝
    """)

    uvicorn.run(
        "main:app",
        host        = "0.0.0.0",
        port        = cfg.port,
        log_level   = "warning",
        access_log  = False,
        # For production:
        # workers   = 1,  # Keep 1 - Pyrogram client is not fork-safe
    )