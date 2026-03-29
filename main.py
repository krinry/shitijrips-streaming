"""
Telegram Video Streaming Service v4.1
Python 3.11 + Pyrogram + FFmpeg + FastAPI

FIXES:
- PEER_ID_INVALID: startup pe channel resolve karo
- Session string se channel peer cache build karo
- Render restart pe bhi kaam kare
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
    OffsetInvalid,
    RPCError,
    PeerIdInvalid,
)
from pyrogram.types import Message

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
    api_id:         int = int(os.environ.get("TELEGRAM_API_ID", "0"))
    api_hash:       str = os.environ.get("TELEGRAM_API_HASH", "")
    session_string: str = os.environ.get("TELEGRAM_SESSION_STRING", "")
    channel_id:     str = os.environ.get("TELEGRAM_CHANNEL_ID", "")
    port:           int = int(os.environ.get("PORT", "10000"))

    pyrogram_chunk_bytes: int = 1024 * 1024
    max_response_bytes:   int = 5 * 1024 * 1024
    demo_file_size:       int = 10 * 1024 * 1024

    max_concurrent_streams: int = 5
    requests_per_minute:    int = 120
    max_ip_log_size:        int = 5000

    file_cache_ttl: int = 10 * 60
    max_cache_size: int = 500

    max_retries:      int   = 3
    retry_base_delay: float = 1.0

    ffmpeg_path:    str = os.environ.get("FFMPEG_PATH", "ffmpeg")
    ffmpeg_threads: int = int(os.environ.get("FFMPEG_THREADS", "2"))


cfg = Config()

# ─── MIME ─────────────────────────────────────────────────────────────────────

BROWSER_NATIVE_MIME = {
    "video/mp4", "video/webm", "video/ogg",
    "audio/mpeg", "audio/ogg", "audio/wav", "audio/webm",
}

MIME_EXT_MAP = {
    "x-matroska": "mkv", "x-msvideo": "avi",
    "quicktime": "mov",  "x-ms-wmv": "wmv",
    "mpeg": "mpg",       "x-flv": "flv",
}

def needs_transcode(mime: str) -> bool:
    return mime not in BROWSER_NATIVE_MIME

# ─── Types ────────────────────────────────────────────────────────────────────

@dataclass
class FileInfo:
    message_id:      int
    channel_id:      str
    file_id:         str
    file_size:       int
    mime_type:       str
    file_name:       str
    dc_id:           int
    needs_transcode: bool    = False
    cached_at:       float   = field(default_factory=time.time)
    message:         Optional[Message] = field(default=None, repr=False)

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
        self.rpm = rpm
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

# Resolved channel peers cache
# channel_id string → resolved peer object
_resolved_channels: dict[str, any] = {}


async def resolve_channel(channel_id: str) -> str:
    """
    THE FIX: Pyrogram needs to 'know' a peer before using it.
    
    Problem: New session string pe channel peer DB me nahi hota.
             get_messages() directly call karne pe PEER_ID_INVALID aata hai.
    
    Fix: get_chat() se pehle channel resolve karo.
         Yeh channel ko peer DB me add kar deta hai.
         Phir get_messages() kaam karta hai.
    """
    if channel_id in _resolved_channels:
        return channel_id

    log.info(f"🔍 Resolving channel: {channel_id}")

    try:
        # get_chat() peer ko session DB me register karta hai
        chat = await tg.get_chat(channel_id)
        resolved_id = str(chat.id)
        
        # Dono forms cache karo (with/without -100 prefix)
        _resolved_channels[channel_id]  = True
        _resolved_channels[resolved_id] = True
        
        log.info(
            f"✅ Channel resolved: {channel_id} → "
            f"{chat.title} (id={chat.id}, members={getattr(chat, 'members_count', 'N/A')})"
        )
        return resolved_id

    except Exception as e:
        log.error(f"❌ Cannot resolve channel '{channel_id}': {e}")
        raise ValueError(
            f"Cannot access channel '{channel_id}'. "
            "Make sure the account is a member of this channel."
        )


async def warm_up_channels():
    """
    Startup pe saare configured channels resolve karo.
    Yeh ensure karta hai ki restart ke baad bhi channels accessible hon.
    """
    channels_to_resolve = []

    if cfg.channel_id:
        channels_to_resolve.append(cfg.channel_id)

    if not channels_to_resolve:
        log.warning("⚠️  No TELEGRAM_CHANNEL_ID configured")
        return

    log.info(f"🔥 Warming up {len(channels_to_resolve)} channel(s)...")

    for cid in channels_to_resolve:
        try:
            await resolve_channel(cid)
        except Exception as e:
            log.warning(f"  ⚠️  Could not warm up {cid}: {e}")

    log.info("✅ Channel warm-up complete")

# ─── File Info ────────────────────────────────────────────────────────────────

def _extract_filename(msg: Message, mime: str) -> str:
    doc = (msg.document or msg.video or msg.audio
           or msg.voice or msg.video_note)
    if not doc:
        return f"media_{msg.id}.bin"
    if getattr(doc, "file_name", None):
        return doc.file_name
    ext_raw = mime.split("/")[-1].split(";")[0].strip()
    ext     = MIME_EXT_MAP.get(ext_raw, ext_raw)
    if msg.caption:
        safe = re.sub(r'[\\/*?:"<>|]', "_", msg.caption[:60]).strip()
        return f"{safe}.{ext}" if safe else f"media_{msg.id}.{ext}"
    return f"media_{msg.id}.{ext}"


async def fetch_file_info(message_id: int, channel_id: str) -> FileInfo:
    cache_key = f"{channel_id}:{message_id}"
    cached    = file_cache.get(cache_key)
    if cached:
        return cached

    # Resolve channel first (fixes PEER_ID_INVALID)
    resolved_cid = await resolve_channel(channel_id)

    try:
        msg = await tg.get_messages(resolved_cid, message_id)
    except PeerIdInvalid:
        # Force re-resolve and retry once
        _resolved_channels.pop(channel_id, None)
        _resolved_channels.pop(resolved_cid, None)
        resolved_cid = await resolve_channel(channel_id)
        msg = await tg.get_messages(resolved_cid, message_id)

    if isinstance(msg, list):
        msg = msg[0] if msg else None

    if not msg or msg.empty:
        raise ValueError(f"Message {message_id} not found")

    doc = (msg.document or msg.video or msg.audio
           or msg.voice or msg.video_note)
    if not doc:
        raise ValueError(f"Message {message_id} has no media")

    mime      = getattr(doc, "mime_type", None) or "video/mp4"
    file_name = _extract_filename(msg, mime)
    file_size = getattr(doc, "file_size", 0) or 0
    dc_id     = getattr(doc, "dc_id", 0) or 0
    transcode = needs_transcode(mime)

    info = FileInfo(
        message_id      = message_id,
        channel_id      = resolved_cid,
        file_id         = getattr(doc, "file_id", ""),
        file_size       = file_size,
        mime_type       = mime,
        file_name       = file_name,
        dc_id           = dc_id,
        needs_transcode = transcode,
        message         = msg,
    )

    file_cache.set(info)
    mode = "🔄 transcode" if transcode else "✅ direct"
    log.info(
        f'📁 msg={message_id} | "{file_name}" | '
        f'{fmt_bytes(file_size)} | {mime} | {mode}'
    )
    return info


async def refresh_file_info(info: FileInfo) -> FileInfo:
    log.warning(f"🔄 Refreshing msg={info.message_id}")
    file_cache.invalidate(info.cache_key())
    return await fetch_file_info(info.message_id, info.channel_id)

# ─── Active Streams ───────────────────────────────────────────────────────────

_active_streams = 0

# ─── Direct Stream ────────────────────────────────────────────────────────────

async def stream_direct(
    info:      FileInfo,
    range_req: RangeReq,
    request:   Request,
) -> AsyncGenerator[bytes, None]:
    global _active_streams
    _active_streams += 1

    CHUNK      = cfg.pyrogram_chunk_bytes
    bytes_sent = 0
    total      = range_req.length
    retries    = 0

    log.info(
        f"🌊 Direct | msg={info.message_id} | "
        f"[{fmt_bytes(range_req.start)}-{fmt_bytes(range_req.end)}] | "
        f"{fmt_bytes(total)}"
    )

    try:
        while bytes_sent < total:
            if await request.is_disconnected():
                log.info(f"  🛑 Disconnected | sent={fmt_bytes(bytes_sent)}")
                return

            try:
                current_byte  = range_req.start + bytes_sent
                current_chunk = current_byte // CHUNK
                skip_bytes    = current_byte % CHUNK

                async for raw_chunk in tg.stream_media(
                    info.message,
                    offset = current_chunk,
                ):
                    if await request.is_disconnected():
                        return

                    if skip_bytes > 0:
                        raw_chunk  = raw_chunk[skip_bytes:]
                        skip_bytes = 0

                    remaining = total - bytes_sent
                    if len(raw_chunk) > remaining:
                        raw_chunk = raw_chunk[:remaining]

                    if raw_chunk:
                        yield raw_chunk
                        bytes_sent += len(raw_chunk)

                    if bytes_sent >= total:
                        break

                break

            except (FileReferenceExpired, FileReferenceInvalid):
                if retries >= cfg.max_retries:
                    raise
                retries += 1
                info = await refresh_file_info(info)

            except FloodWait as e:
                await asyncio.sleep(e.value + 1)

            except OffsetInvalid:
                log.info("  📄 EOF")
                break

            except RPCError as e:
                if retries >= cfg.max_retries:
                    raise
                retries += 1
                await asyncio.sleep(cfg.retry_base_delay * retries)

        log.info(f"  ✅ Done | sent={fmt_bytes(bytes_sent)}")

    finally:
        _active_streams -= 1

# ─── Transcode Stream ─────────────────────────────────────────────────────────

async def stream_transcode(
    info:    FileInfo,
    request: Request,
) -> AsyncGenerator[bytes, None]:
    global _active_streams
    _active_streams += 1

    log.info(f"🔄 Transcode | msg={info.message_id} | {info.mime_type} → mp4")

    ffmpeg_cmd = [
        cfg.ffmpeg_path,
        "-hide_banner", "-loglevel", "error",
        "-threads", str(cfg.ffmpeg_threads),
        "-i", "pipe:0",
        "-c:v", "copy",
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "frag_keyframe+empty_moov+default_base_moof",
        "-f", "mp4",
        "pipe:1",
    ]

    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *ffmpeg_cmd,
            stdin  = asyncio.subprocess.PIPE,
            stdout = asyncio.subprocess.PIPE,
            stderr = asyncio.subprocess.PIPE,
        )

        async def feed_ffmpeg():
            try:
                async for raw_chunk in tg.stream_media(info.message, offset=0):
                    if proc.returncode is not None:
                        break
                    if proc.stdin:
                        try:
                            proc.stdin.write(raw_chunk)
                            await proc.stdin.drain()
                        except (BrokenPipeError, ConnectionResetError):
                            break
            except Exception as e:
                log.warning(f"  Feed error: {e}")
            finally:
                if proc.stdin:
                    try:
                        proc.stdin.close()
                    except Exception:
                        pass

        feed_task  = asyncio.create_task(feed_ffmpeg())
        bytes_sent = 0

        try:
            while True:
                if await request.is_disconnected():
                    log.info("  🛑 Client disconnected")
                    break
                chunk = await proc.stdout.read(65536)
                if not chunk:
                    break
                yield chunk
                bytes_sent += len(chunk)
        finally:
            feed_task.cancel()
            try:
                await feed_task
            except asyncio.CancelledError:
                pass

        log.info(f"  ✅ Transcode done | sent={fmt_bytes(bytes_sent)}")

    except Exception as e:
        log.error(f"  ❌ Transcode error: {e}")
        raise
    finally:
        if proc and proc.returncode is None:
            try:
                proc.kill()
                await proc.wait()
            except Exception:
                pass
        _active_streams -= 1

# ─── FFmpeg Check ─────────────────────────────────────────────────────────────

async def check_ffmpeg() -> bool:
    try:
        proc = await asyncio.create_subprocess_exec(
            cfg.ffmpeg_path, "-version",
            stdout = asyncio.subprocess.PIPE,
            stderr = asyncio.subprocess.PIPE,
        )
        await proc.wait()
        return proc.returncode == 0
    except FileNotFoundError:
        return False

# ─── Range Parser ─────────────────────────────────────────────────────────────

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

ffmpeg_available = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    global ffmpeg_available

    log.info("🚀 Starting...")

    if cfg.api_id and cfg.api_hash and cfg.session_string:
        await tg.start()
        me = await tg.get_me()
        log.info(f"✅ Telegram: {me.first_name} (@{me.username})")

        # THE FIX: Startup pe channel resolve karo
        await warm_up_channels()
    else:
        log.warning("⚠️  Telegram not configured → demo mode")

    ffmpeg_available = await check_ffmpeg()
    if ffmpeg_available:
        log.info(f"✅ FFmpeg ready")
    else:
        log.warning(f"⚠️  FFmpeg not found at '{cfg.ffmpeg_path}'")

    yield

    log.info("👋 Shutting down...")
    if tg.is_connected:
        await tg.stop()


app = FastAPI(
    title     = "Telegram Streaming",
    version   = "4.1.0",
    lifespan  = lifespan,
    docs_url  = "/docs",
    redoc_url = None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins  = ["*"],
    allow_methods  = ["GET", "HEAD", "OPTIONS"],
    allow_headers  = ["Range", "Content-Type", "Accept"],
    expose_headers = [
        "Content-Range", "Accept-Ranges", "Content-Length",
        "Content-Type", "Content-Disposition",
    ],
    max_age = 86400,
)

# ─── Middleware ───────────────────────────────────────────────────────────────

SKIP_RL = {"/health", "/docs", "/openapi.json"}

@app.middleware("http")
async def rate_limit_mw(request: Request, call_next):
    if request.url.path not in SKIP_RL:
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
                headers     = {"Retry-After": "60", "Content-Type": "application/json"},
            )
    return await call_next(request)

# ─── Routes ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status"           : "ok",
        "telegram"         : tg.is_connected,
        "ffmpeg"           : ffmpeg_available,
        "active_streams"   : _active_streams,
        "cached_files"     : len(file_cache),
        "resolved_channels": list(_resolved_channels.keys()),
        "port"             : cfg.port,
        "timestamp"        : time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
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

    effective_mime = "video/mp4" if info.needs_transcode else info.mime_type

    return {
        "messageId"         : info.message_id,
        "channelId"         : info.channel_id,
        "fileName"          : info.file_name,
        "fileSize"          : info.file_size,
        "fileSizeFormatted" : fmt_bytes(info.file_size),
        "mimeType"          : info.mime_type,
        "effectiveMime"     : effective_mime,
        "needsTranscode"    : info.needs_transcode,
        "ffmpegAvailable"   : ffmpeg_available,
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

    if _active_streams >= cfg.max_concurrent_streams:
        return Response(
            '{"error":"Too many streams"}',
            status_code = 429,
            headers     = {"Retry-After": "5", "Content-Type": "application/json"},
        )

    if not tg.is_connected:
        return _demo_response(request)

    try:
        info = await fetch_file_info(message_id, cid)
    except ValueError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        log.error(f"Fetch error: {e}")
        raise HTTPException(500, str(e))

    # Transcode path
    if info.needs_transcode:
        if not ffmpeg_available:
            raise HTTPException(501, f"FFmpeg not installed. Cannot play {info.mime_type}.")

        log.info(f'\n🎬 TRANSCODE | msg={message_id} | "{info.file_name}"')
        headers = {
            "Content-Type"        : "video/mp4",
            "Accept-Ranges"       : "none",
            "Content-Disposition" : f'inline; filename="{_safe_name(info.file_name)}.mp4"',
            "Cache-Control"       : "no-cache, no-store",
        }
        if request.method == "HEAD":
            return Response(status_code=200, headers=headers)

        return StreamingResponse(
            content     = stream_transcode(info, request),
            status_code = 200,
            headers     = headers,
            media_type  = "video/mp4",
        )

    # Direct stream path
    file_size = info.file_size
    rh        = request.headers.get("Range", "")
    range_req = parse_range(rh, file_size)

    if rh and not range_req:
        return Response(
            status_code = 416,
            headers     = {"Content-Range": f"bytes */{file_size}"},
        )

    if not range_req:
        range_req = RangeReq(start=0, end=file_size - 1)

    capped_end = min(range_req.end, range_req.start + cfg.max_response_bytes - 1)
    capped_end = min(capped_end, file_size - 1)
    cr         = RangeReq(start=range_req.start, end=capped_end)

    log.info(
        f'\n🎬 DIRECT | msg={message_id} | "{info.file_name}" | '
        f'[{fmt_bytes(cr.start)}-{fmt_bytes(cr.end)}] | {fmt_bytes(cr.length)}'
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
        content     = stream_direct(info, cr, request),
        status_code = 206,
        headers     = headers,
        media_type  = info.mime_type,
    )


@app.get("/demo-stream")
async def demo_stream(request: Request):
    return _demo_response(request)

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _demo_response(request: Request) -> Response:
    size  = cfg.demo_file_size
    rng   = parse_range(request.headers.get("Range", ""), size)
    start = rng.start if rng else 0
    end   = rng.end   if rng else size - 1
    chunk = bytes((start + i) % 256 for i in range(end - start + 1))
    hdrs  = {
        "Content-Type"  : "video/mp4",
        "Content-Length": str(len(chunk)),
        "Accept-Ranges" : "bytes",
    }
    if rng:
        hdrs["Content-Range"] = f"bytes {start}-{end}/{size}"
    return Response(content=chunk, status_code=206 if rng else 200, headers=hdrs)


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

# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"Listening on http://0.0.0.0:{cfg.port}", flush=True)
    print(f"""
╔══════════════════════════════════════════╗
║   Telegram Video Streaming v4.1         ║
╠══════════════════════════════════════════╣
║  Port    : {cfg.port:<30} ║
║  API ID  : {str(cfg.api_id) if cfg.api_id else "⚠️  NOT SET":<30} ║
║  Channel : {cfg.channel_id or "⚠️  NOT SET":<30} ║
║  FFmpeg  : {cfg.ffmpeg_path:<30} ║
╚══════════════════════════════════════════╝
""", flush=True)

    uvicorn.run(
        "main:app",
        host       = "0.0.0.0",
        port       = cfg.port,
        log_level  = "warning",
        access_log = False,
    )