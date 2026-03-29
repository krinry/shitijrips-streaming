"""
Telegram Video Streaming Service
=================================
Stack: Python 3.11-3.13 | Pyrogram 2.x | FastAPI | Uvicorn

FIXES:
  - OFFSET_INVALID: stream_media(offset=N) means Nth CHUNK not Nth BYTE
    offset=0 means chunk 0 (bytes 0-512KB)
    offset=1 means chunk 1 (bytes 512KB-1MB)
    chunk_size = 1MB (pyrogram default)
    So: chunk_number = byte_offset // (1024*1024)
  - Python 3.14: use pyrogram 2.0.106 max (not compatible with 3.14)
  - Render: bind 0.0.0.0, print port before uvicorn starts
  - Ctrl+C: proper signal handling

IMPORTANT: Use Python 3.11 or 3.12 (NOT 3.13/3.14)
  render.yaml: pythonVersion: "3.11.9"
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import signal
import sys
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
    OffsetInvalid,
    RPCError,
)
from pyrogram.types import Message

# ─── Load Environment ─────────────────────────────────────────────────────────

load_dotenv()

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt = "%H:%M:%S",
)
log = logging.getLogger("stream")
logging.getLogger("pyrogram").setLevel(logging.WARNING)

# ─── Config ───────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Config:
    api_id:         int   = int(os.environ.get("TELEGRAM_API_ID", "0"))
    api_hash:       str   = os.environ.get("TELEGRAM_API_HASH", "")
    session_string: str   = os.environ.get("TELEGRAM_SESSION_STRING", "")
    channel_id:     str   = os.environ.get("TELEGRAM_CHANNEL_ID", "")
    port:           int   = int(os.environ.get("PORT", "3031"))

    # Pyrogram stream_media chunk size (FIXED - do not change)
    # pyrogram internally uses 1MB chunks
    # offset=N means: skip first N chunks (each 1MB)
    pyrogram_chunk_bytes: int = 1024 * 1024  # 1MB

    # Max bytes per HTTP response
    max_response_bytes: int = 5 * 1024 * 1024   # 5MB

    # Demo
    demo_file_size: int = 10 * 1024 * 1024

    # Rate limits
    max_concurrent_streams: int = 10
    requests_per_minute:    int = 120
    max_ip_log_size:        int = 5000

    # Cache
    file_cache_ttl:  int = 10 * 60
    max_cache_size:  int = 500

    # Retry
    max_retries:        int   = 3
    retry_base_delay:   float = 1.0


cfg = Config()

if not cfg.api_id or not cfg.api_hash:
    log.warning("⚠️  Telegram not configured → demo mode only")

# ─── Types ────────────────────────────────────────────────────────────────────

@dataclass
class FileInfo:
    message_id: int
    channel_id: str
    file_id:    str
    file_size:  int
    mime_type:  str
    file_name:  str
    dc_id:      int
    cached_at:  float   = field(default_factory=time.time)
    message:    Optional[Message] = field(default=None, repr=False)

    def is_fresh(self) -> bool:
        return time.time() - self.cached_at < cfg.file_cache_ttl

    def cache_key(self) -> str:
        return f"{self.channel_id}:{self.message_id}"


@dataclass
class RangeReq:
    start: int
    end:   int

    @property
    def length(self) -> int:
        return self.end - self.start + 1

# ─── Rate Limiter ─────────────────────────────────────────────────────────────

class RateLimiter:
    def __init__(self, rpm: int, max_ips: int):
        self.rpm     = rpm
        self.max_ips = max_ips
        self._log: dict[str, list[float]] = defaultdict(list)

    def is_allowed(self, ip: str) -> bool:
        now    = time.monotonic()
        window = 60.0
        times  = [t for t in self._log[ip] if now - t < window]
        times.append(now)
        self._log[ip] = times

        if len(self._log) > self.max_ips:
            cutoff = now - window
            stale  = [k for k, v in self._log.items()
                      if all(t < cutoff for t in v)]
            for k in stale[:100]:
                del self._log[k]

        return len(times) <= self.rpm


rate_limiter = RateLimiter(cfg.requests_per_minute, cfg.max_ip_log_size)

# ─── File Cache ───────────────────────────────────────────────────────────────

class FileCache:
    def __init__(self, max_size: int):
        self._store: dict[str, FileInfo] = {}
        self.max_size = max_size

    def get(self, key: str) -> Optional[FileInfo]:
        info = self._store.get(key)
        if info and info.is_fresh():
            return info
        self._store.pop(key, None)
        return None

    def set(self, info: FileInfo) -> None:
        if len(self._store) >= self.max_size:
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
    name           = "stream",
    api_id         = cfg.api_id,
    api_hash       = cfg.api_hash,
    session_string = cfg.session_string or None,
)

# ─── File Info ────────────────────────────────────────────────────────────────

_MIME_EXT = {
    "x-matroska" : "mkv",
    "x-msvideo"  : "avi",
    "quicktime"  : "mov",
    "x-ms-wmv"   : "wmv",
    "mpeg"       : "mpg",
    "x-flv"      : "flv",
    "webm"       : "webm",
}

def _extract_filename(msg: Message, mime: str, msg_id: int) -> str:
    doc = (msg.document or msg.video or msg.audio
           or msg.voice or msg.video_note)
    if not doc:
        return f"media_{msg_id}.bin"

    if getattr(doc, "file_name", None):
        return doc.file_name

    ext_raw = mime.split("/")[-1].split(";")[0].strip()
    ext     = _MIME_EXT.get(ext_raw, ext_raw)

    if msg.caption:
        safe = re.sub(r'[\\/*?:"<>|]', "_", msg.caption[:60]).strip()
        return f"{safe}.{ext}" if safe else f"media_{msg_id}.{ext}"

    return f"media_{msg_id}.{ext}"


async def fetch_file_info(message_id: int, channel_id: str) -> FileInfo:
    cache_key = f"{channel_id}:{message_id}"
    cached    = file_cache.get(cache_key)
    if cached:
        return cached

    msg = await tg.get_messages(channel_id, message_id)
    if isinstance(msg, list):
        msg = msg[0] if msg else None

    if not msg or msg.empty:
        raise ValueError(f"Message {message_id} not found")

    doc = (msg.document or msg.video or msg.audio
           or msg.voice or msg.video_note)
    if not doc:
        raise ValueError(f"Message {message_id} has no streamable media")

    mime      = getattr(doc, "mime_type", None) or "video/mp4"
    file_name = _extract_filename(msg, mime, message_id)
    file_size = getattr(doc, "file_size", 0) or 0
    dc_id     = getattr(doc, "dc_id", 0) or 0

    info = FileInfo(
        message_id = message_id,
        channel_id = channel_id,
        file_id    = getattr(doc, "file_id", ""),
        file_size  = file_size,
        mime_type  = mime,
        file_name  = file_name,
        dc_id      = dc_id,
        message    = msg,
    )

    file_cache.set(info)
    log.info(
        f'📁 msg={message_id} | "{file_name}" | '
        f'{fmt_bytes(file_size)} | dc={dc_id}'
    )
    return info


async def refresh_file_info(info: FileInfo) -> FileInfo:
    log.warning(f"🔄 Refreshing msg={info.message_id}")
    file_cache.invalidate(info.cache_key())
    return await fetch_file_info(info.message_id, info.channel_id)

# ─── Core Stream Generator ────────────────────────────────────────────────────

_active_streams = 0

# THE KEY FIX:
# pyrogram stream_media(offset=N) means:
#   "skip first N chunks, where each chunk = 1MB (1048576 bytes)"
#
# So to seek to byte position B:
#   chunk_number = B // 1_048_576      ← which 1MB block contains byte B
#   skip_bytes   = B % 1_048_576       ← bytes to skip within that block
#
# Example: seek to byte 5_000_000
#   chunk_number = 5_000_000 // 1_048_576 = 4  (chunk index 4)
#   skip_bytes   = 5_000_000 % 1_048_576  = 814848
#   First chunk received starts at byte 4*1048576 = 4194304
#   We skip first 805696 bytes of that chunk

async def stream_file(
    info:      FileInfo,
    range_req: RangeReq,
    request:   Request,
) -> AsyncGenerator[bytes, None]:
    global _active_streams
    _active_streams += 1

    bytes_sent = 0
    total      = range_req.length
    retries    = 0

    # Calculate starting chunk
    # pyrogram chunk = 1MB = 1_048_576 bytes
    CHUNK = cfg.pyrogram_chunk_bytes
    start_chunk   = range_req.start // CHUNK   # which chunk to start from
    skip_in_chunk = range_req.start % CHUNK    # bytes to skip in first chunk

    log.info(
        f"🌊 Stream | msg={info.message_id} | "
        f"[{fmt_bytes(range_req.start)}-{fmt_bytes(range_req.end)}] | "
        f"{fmt_bytes(total)} | chunk_start={start_chunk} skip={skip_in_chunk}"
    )

    try:
        while bytes_sent < total:
            if await request.is_disconnected():
                log.info(f"  🛑 Disconnected | sent={fmt_bytes(bytes_sent)}")
                return

            try:
                # Recalculate chunk position based on bytes already sent
                current_byte  = range_req.start + bytes_sent
                current_chunk = current_byte // CHUNK
                skip_bytes    = current_byte % CHUNK

                log.debug(
                    f"  📥 chunk={current_chunk} skip={skip_bytes} "
                    f"sent={fmt_bytes(bytes_sent)}/{fmt_bytes(total)}"
                )

                chunk_iter = tg.stream_media(
                    info.message,
                    offset = current_chunk,   # chunk index, NOT byte offset
                )

                async for raw_chunk in chunk_iter:
                    if await request.is_disconnected():
                        log.info("  🛑 Disconnected mid-chunk")
                        return

                    # First chunk: skip bytes before our actual start
                    if skip_bytes > 0:
                        raw_chunk = raw_chunk[skip_bytes:]
                        skip_bytes = 0

                    # Don't send more than requested
                    remaining = total - bytes_sent
                    if len(raw_chunk) > remaining:
                        raw_chunk = raw_chunk[:remaining]

                    if raw_chunk:
                        yield raw_chunk
                        bytes_sent += len(raw_chunk)

                    if bytes_sent >= total:
                        break

                # If loop ended without getting enough bytes → EOF
                break

            except (FileReferenceExpired, FileReferenceInvalid):
                if retries >= cfg.max_retries:
                    log.error("❌ File reference expired, giving up")
                    raise
                retries += 1
                log.warning(f"🔄 File ref expired, retry {retries}")
                info = await refresh_file_info(info)

            except FloodWait as e:
                wait = e.value + 1
                log.warning(f"⏳ FloodWait {wait}s")
                await asyncio.sleep(wait)

            except OffsetInvalid:
                # offset past end of file → we're done
                log.info("  📄 OffsetInvalid → EOF reached")
                break

            except RPCError as e:
                if retries >= cfg.max_retries:
                    raise
                retries += 1
                log.warning(f"⚠️  RPC error retry {retries}: {e}")
                await asyncio.sleep(cfg.retry_base_delay * retries)

        log.info(f"  ✅ Done | sent={fmt_bytes(bytes_sent)}")

    except Exception as e:
        log.error(f"  ❌ Stream error: {e}")
        raise
    finally:
        _active_streams -= 1

# ─── Range Header Parser ──────────────────────────────────────────────────────

def parse_range(header: str, file_size: int) -> Optional[RangeReq]:
    if not header:
        return None

    m = re.match(r"bytes=(\d*)-(\d*)", header)
    if not m:
        return None

    s, e = m.group(1), m.group(2)

    if s == "" and e != "":
        suffix = int(e)
        start  = max(0, file_size - suffix)
        end    = file_size - 1
    elif s != "":
        start = int(s)
        end   = int(e) if e else file_size - 1
    else:
        return None

    end = min(end, file_size - 1)
    if start < 0 or start >= file_size or start > end:
        return None

    return RangeReq(start=start, end=end)

# ─── FastAPI ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("🚀 Starting Telegram client...")
    if cfg.api_id and cfg.api_hash and cfg.session_string:
        await tg.start()
        me = await tg.get_me()
        log.info(f"✅ Logged in as {me.first_name} (@{me.username})")
    else:
        log.warning("⚠️  Telegram not configured → demo mode")

    yield  # Server runs here

    log.info("👋 Shutting down...")
    if tg.is_connected:
        await tg.stop()


app = FastAPI(
    title      = "Telegram Streaming",
    version    = "3.0.0",
    lifespan   = lifespan,
    docs_url   = "/docs",
    redoc_url  = None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins    = ["*"],
    allow_methods    = ["GET", "HEAD", "OPTIONS"],
    allow_headers    = ["Range", "Content-Type", "Accept"],
    expose_headers   = [
        "Content-Range", "Accept-Ranges", "Content-Length",
        "Content-Type", "Content-Disposition",
    ],
    max_age = 86400,
)

# ─── Rate Limit Middleware ────────────────────────────────────────────────────

SKIP_RATE_LIMIT = {"/health", "/docs", "/openapi.json"}

@app.middleware("http")
async def rate_limit_mw(request: Request, call_next):
    if request.url.path not in SKIP_RATE_LIMIT:
        ip = (
            request.headers.get("x-forwarded-for", "").split(",")[0].strip()
            or request.headers.get("x-real-ip", "")
            or (request.client.host if request.client else "unknown")
        )
        if not rate_limiter.is_allowed(ip):
            log.warning(f"🚫 Rate limit: {ip}")
            return Response(
                '{"error":"Too many requests"}',
                status_code = 429,
                headers     = {
                    "Retry-After"   : "60",
                    "Content-Type"  : "application/json",
                },
            )
    return await call_next(request)

# ─── Routes ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status"         : "ok",
        "telegram"       : tg.is_connected,
        "active_streams" : _active_streams,
        "cached_files"   : len(file_cache),
        "config"         : {
            "max_concurrent_streams" : cfg.max_concurrent_streams,
            "requests_per_minute"    : cfg.requests_per_minute,
            "max_response_size"      : fmt_bytes(cfg.max_response_bytes),
            "pyrogram_chunk"         : fmt_bytes(cfg.pyrogram_chunk_bytes),
        },
        "timestamp" : time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


@app.get("/info/{message_id}")
async def file_info(
    message_id : int,
    channel    : str = Query(""),
):
    if not tg.is_connected:
        raise HTTPException(503, "Telegram not connected")

    cid = channel or cfg.channel_id
    if not cid:
        raise HTTPException(400, "Missing channel ID")

    try:
        info = await fetch_file_info(message_id, cid)
    except ValueError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(500, str(e))

    return {
        "messageId"         : info.message_id,
        "channelId"         : info.channel_id,
        "fileName"          : info.file_name,
        "fileSize"          : info.file_size,
        "fileSizeFormatted" : fmt_bytes(info.file_size),
        "mimeType"          : info.mime_type,
        "dcId"              : info.dc_id,
        "streamUrl"         : f"/stream/{message_id}?channel={cid}",
    }


@app.api_route("/stream/{message_id}", methods=["GET", "HEAD"])
async def stream(
    message_id : int,
    request    : Request,
    channel    : str = Query(""),
):
    cid = channel or cfg.channel_id
    if not cid:
        raise HTTPException(400, "Missing channel ID")

    # Concurrent limit
    if _active_streams >= cfg.max_concurrent_streams:
        return Response(
            '{"error":"Too many streams"}',
            status_code = 429,
            headers     = {"Retry-After": "5", "Content-Type": "application/json"},
        )

    # Demo fallback
    if not tg.is_connected:
        return _demo_response(request)

    # Fetch metadata
    try:
        info = await fetch_file_info(message_id, cid)
    except ValueError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        log.error(f"Fetch error: {e}")
        raise HTTPException(500, str(e))

    file_size = info.file_size

    # Parse range
    rh        = request.headers.get("Range", "")
    range_req = parse_range(rh, file_size)

    if rh and not range_req:
        return Response(
            status_code = 416,
            headers     = {"Content-Range": f"bytes */{file_size}"},
        )

    if not range_req:
        range_req = RangeReq(start=0, end=file_size - 1)

    # Cap response size
    capped_end = min(range_req.end, range_req.start + cfg.max_response_bytes - 1)
    capped_end = min(capped_end, file_size - 1)
    cr         = RangeReq(start=range_req.start, end=capped_end)

    log.info(
        f'\n🎬 msg={message_id} | "{info.file_name}" | '
        f'[{fmt_bytes(cr.start)}-{fmt_bytes(cr.end)}] | '
        f'{fmt_bytes(cr.length)} / {fmt_bytes(file_size)}'
    )

    headers = {
        "Content-Type"        : info.mime_type,
        "Content-Length"      : str(cr.length),
        "Content-Range"       : f"bytes {cr.start}-{cr.end}/{file_size}",
        "Accept-Ranges"       : "bytes",
        "Content-Disposition" : f'inline; filename="{_safe_name(info.file_name)}"',
        "Cache-Control"       : "no-cache, no-store",
    }

    if request.method == "HEAD":
        return Response(status_code=206, headers=headers)

    return StreamingResponse(
        content     = stream_file(info, cr, request),
        status_code = 206,
        headers     = headers,
        media_type  = info.mime_type,
    )


@app.get("/demo-stream")
async def demo_stream(request: Request):
    return _demo_response(request)

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _demo_response(request: Request) -> Response:
    size      = cfg.demo_file_size
    rng       = parse_range(request.headers.get("Range", ""), size)
    start     = rng.start if rng else 0
    end       = rng.end   if rng else size - 1
    chunk     = bytes((start + i) % 256 for i in range(end - start + 1))
    headers   = {
        "Content-Type"  : "video/mp4",
        "Content-Length": str(len(chunk)),
        "Accept-Ranges" : "bytes",
    }
    if rng:
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
    return Response(content=chunk, status_code=206 if rng else 200, headers=headers)


def fmt_bytes(b: int) -> str:
    if b == 0:
        return "0 Bytes"
    for u in ("Bytes", "KB", "MB", "GB", "TB"):
        if b < 1024.0:
            return f"{b:.2f} {u}"
        b /= 1024.0
    return f"{b:.2f} PB"


def _safe_name(name: str) -> str:
    return re.sub(r'["\\\r\n]', "_", name)

# ─── Entry Point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Print BEFORE uvicorn starts so Render detects the port
    print(f"""
╔══════════════════════════════════════╗
║   Telegram Video Streaming v3.0     ║
║   Python + Pyrogram + FastAPI       ║
╠══════════════════════════════════════╣
║  Port     : {cfg.port:<26} ║
║  API ID   : {str(cfg.api_id) if cfg.api_id else "⚠️  NOT SET":<26} ║
║  Channel  : {cfg.channel_id or "pass ?channel=...":<26} ║
╠══════════════════════════════════════╣
║  GET /stream/{{id}}?channel=ID        ║
║  GET /info/{{id}}?channel=ID          ║
║  GET /health                         ║
║  GET /docs                           ║
╚══════════════════════════════════════╝
Listening on http://0.0.0.0:{cfg.port}
""", flush=True)

    uvicorn.run(
        "main:app",
        host       = "0.0.0.0",
        port       = cfg.port,
        log_level  = "warning",
        access_log = False,
    )