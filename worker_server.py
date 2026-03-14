import os
import hmac
import hashlib
import asyncio
import json
import uuid
import time
import secrets
from urllib.parse import parse_qsl
from typing import Any, Dict

import aiohttp
from aiohttp import web

from worker_task_runner import run_worker_task, run_worker_task_stream


WORKER_API_SECRET = os.getenv("WORKER_API_SECRET", "").strip()
WORKER_HOST = os.getenv("WORKER_HOST", "0.0.0.0").strip()
WORKER_PORT = int(os.getenv("WORKER_PORT", "8787"))
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    try:
        from config import BOT_TOKEN as CFG_BOT_TOKEN  # type: ignore
        BOT_TOKEN = (CFG_BOT_TOKEN or "").strip()
    except Exception:
        BOT_TOKEN = ""
WEBAPP_ALLOWED_ORIGIN = os.getenv("WEBAPP_ALLOWED_ORIGIN", "https://jackhuynh1132.github.io").strip()
WEBAPP_EXTRA_ORIGINS = [
    o.strip() for o in os.getenv("WEBAPP_EXTRA_ORIGINS", "https://app.xchudai.store").split(",") if o.strip()
]
JOB_TTL_SECONDS = int(os.getenv("WEBAPP_JOB_TTL_SECONDS", "900"))
AUTH_CODE_TTL_SECONDS = int(os.getenv("WEBAPP_AUTH_CODE_TTL_SECONDS", "300"))
AUTH_SESSION_TTL_SECONDS = int(os.getenv("WEBAPP_AUTH_SESSION_TTL_SECONDS", "86400"))


JOBS: Dict[str, Dict[str, Any]] = {}
AUTH_CODES: Dict[str, Dict[str, Any]] = {}
AUTH_SESSIONS: Dict[str, Dict[str, Any]] = {}


def _is_allowed_origin(origin: str) -> bool:
    if not origin:
        return False
    if origin == WEBAPP_ALLOWED_ORIGIN:
        return True
    return origin in WEBAPP_EXTRA_ORIGINS


def _authorized(request: web.Request) -> bool:
    if not WORKER_API_SECRET:
        return False
    return request.headers.get("X-Worker-Secret", "") == WORKER_API_SECRET


async def _get_public_ip() -> str:
    try:
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get("https://api.ipify.org?format=json") as resp:
                if resp.status != 200:
                    return "unknown"
                data = await resp.json()
                return data.get("ip", "unknown")
    except Exception:
        return "unknown"


async def health_handler(request: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def run_handler(request: web.Request) -> web.Response:
    if not _authorized(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    try:
        payload: Dict[str, Any] = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    try:
        result = await run_worker_task(payload)
        result["worker_ip"] = await _get_public_ip()
        return web.json_response(result)
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)[:500]}, status=500)


def _verify_telegram_init_data(init_data: str) -> bool:
    """Verify Telegram WebApp initData signature."""
    if not init_data or not BOT_TOKEN:
        return False
    try:
        pairs = dict(parse_qsl(init_data, keep_blank_values=True))
        recv_hash = pairs.pop("hash", "")
        if not recv_hash:
            return False
        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
        secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode("utf-8"), hashlib.sha256).digest()
        calc_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
        return hmac.compare_digest(calc_hash, recv_hash)
    except Exception:
        return False


def _extract_user_from_init_data(init_data: str) -> Dict[str, Any]:
    try:
        pairs = dict(parse_qsl(init_data or "", keep_blank_values=True))
        raw_user = pairs.get("user", "")
        if not raw_user:
            return {}
        import json as _json
        user = _json.loads(raw_user)
        return user if isinstance(user, dict) else {}
    except Exception:
        return {}


def _auth_gc():
    now = time.time()
    expired_codes = [uid for uid, meta in AUTH_CODES.items() if now > float(meta.get("expires_at") or 0)]
    for uid in expired_codes:
        AUTH_CODES.pop(uid, None)
    expired_sessions = [token for token, meta in AUTH_SESSIONS.items() if now > float(meta.get("expires_at") or 0)]
    for token in expired_sessions:
        AUTH_SESSIONS.pop(token, None)


async def _telegram_api(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN missing")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    timeout = aiohttp.ClientTimeout(total=12)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, json=payload) as resp:
            data = await resp.json(content_type=None)
            if resp.status != 200 or not data.get("ok"):
                raise RuntimeError((data.get("description") or f"telegram http {resp.status}")[:200])
            return data.get("result") or {}


async def _load_telegram_profile(user_id: int) -> Dict[str, Any]:
    profile: Dict[str, Any] = {"id": user_id, "first_name": "Telegram User", "username": "", "photo_url": ""}
    try:
        chat = await _telegram_api("getChat", {"chat_id": int(user_id)})
        profile["first_name"] = chat.get("first_name") or chat.get("username") or profile["first_name"]
        profile["username"] = chat.get("username") or ""
    except Exception:
        pass
    try:
        photos = await _telegram_api("getUserProfilePhotos", {"user_id": int(user_id), "limit": 1})
        if photos.get("total_count"):
            sizes = (photos.get("photos") or [[]])[0]
            if sizes:
                best = sizes[-1]
                file_info = await _telegram_api("getFile", {"file_id": best.get("file_id")})
                file_path = file_info.get("file_path") or ""
                if file_path:
                    profile["photo_url"] = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
    except Exception:
        pass
    return profile


def _user_from_session_token(session_token: str) -> Dict[str, Any]:
    _auth_gc()
    meta = AUTH_SESSIONS.get((session_token or "").strip())
    if not meta:
        return {}
    return dict(meta.get("user") or {})


def _resolve_webapp_user(payload: Dict[str, Any]) -> Dict[str, Any]:
    init_data = (payload.get("init_data") or "").strip()
    if init_data:
        if not _verify_telegram_init_data(init_data):
            raise web.HTTPUnauthorized(text=json.dumps({"ok": False, "error": "unauthorized webapp"}), content_type="application/json")
        tg_user = _extract_user_from_init_data(init_data)
        if tg_user:
            return tg_user
    session_user = _user_from_session_token(payload.get("session_token") or "")
    if session_user:
        return session_user
    return {}


async def webapp_request_code_handler(request: web.Request) -> web.Response:
    try:
        payload: Dict[str, Any] = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)
    _auth_gc()
    user_id = int(str(payload.get("user_id") or "0").strip() or 0)
    if not user_id:
        return web.json_response({"ok": False, "error": "missing user_id"}, status=400)
    code = f"{secrets.randbelow(900000) + 100000}"
    AUTH_CODES[str(user_id)] = {"code": code, "expires_at": time.time() + AUTH_CODE_TTL_SECONDS}
    try:
        await _telegram_api("sendMessage", {
            "chat_id": user_id,
            "text": f"<b>J.HUYNH OTP</b>\n\nCode: <code>{code}</code>\nExpires in 5 minutes.",
            "parse_mode": "HTML",
        })
    except Exception as e:
        return web.json_response({"ok": False, "error": f"failed to send code: {str(e)[:120]}"}, status=400)
    return web.json_response({"ok": True, "sent": True, "expires_in": AUTH_CODE_TTL_SECONDS})


async def webapp_verify_code_handler(request: web.Request) -> web.Response:
    try:
        payload: Dict[str, Any] = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)
    _auth_gc()
    user_id = str(payload.get("user_id") or "").strip()
    code = str(payload.get("code") or "").strip()
    meta = AUTH_CODES.get(user_id)
    if not user_id or not code or not meta:
        return web.json_response({"ok": False, "error": "invalid code"}, status=400)
    if time.time() > float(meta.get("expires_at") or 0):
        AUTH_CODES.pop(user_id, None)
        return web.json_response({"ok": False, "error": "code expired"}, status=400)
    if code != str(meta.get("code")):
        return web.json_response({"ok": False, "error": "wrong code"}, status=400)
    AUTH_CODES.pop(user_id, None)
    profile = await _load_telegram_profile(int(user_id))
    session_token = secrets.token_urlsafe(24)
    AUTH_SESSIONS[session_token] = {"user": profile, "expires_at": time.time() + AUTH_SESSION_TTL_SECONDS}
    return web.json_response({"ok": True, "session_token": session_token, "user": profile})


async def webapp_run_handler(request: web.Request) -> web.Response:
    """Public WebApp endpoint: auth by Telegram initData instead of static secret."""
    try:
        payload: Dict[str, Any] = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    try:
        tg_user = _resolve_webapp_user(payload)
    except web.HTTPUnauthorized as exc:
        return web.Response(status=exc.status, text=exc.text, content_type="application/json")

    # Ensure worker task has a stable Telegram user context for premium/owner checks.
    if tg_user:
        try:
            payload["user_id"] = int(tg_user.get("id") or payload.get("user_id") or 0)
        except Exception:
            payload["user_id"] = int(payload.get("user_id") or 0)
        payload["chat_id"] = int(payload.get("chat_id") or payload.get("user_id") or 0)
        payload["user_name"] = (
            tg_user.get("first_name")
            or tg_user.get("username")
            or payload.get("user_name")
            or "WebAppUser"
        )

    try:
        result = await run_worker_task(payload)
        result["worker_ip"] = await _get_public_ip()
        return web.json_response(result)
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)[:500]}, status=500)


def _job_gc():
    now = asyncio.get_event_loop().time()
    expired = []
    for jid, meta in JOBS.items():
        created = float(meta.get("created_at") or 0)
        if now - created > JOB_TTL_SECONDS:
            expired.append(jid)
    for jid in expired:
        JOBS.pop(jid, None)


async def webapp_start_handler(request: web.Request) -> web.Response:
    try:
        payload: Dict[str, Any] = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    try:
        tg_user = _resolve_webapp_user(payload)
    except web.HTTPUnauthorized as exc:
        return web.Response(status=exc.status, text=exc.text, content_type="application/json")

    if tg_user:
        payload["user_id"] = int(tg_user.get("id") or payload.get("user_id") or 0)
        payload["chat_id"] = int(payload.get("chat_id") or payload.get("user_id") or 0)
        payload["user_name"] = (
            tg_user.get("first_name")
            or tg_user.get("username")
            or payload.get("user_name")
            or "WebAppUser"
        )

    _job_gc()
    job_id = uuid.uuid4().hex[:12]
    queue: asyncio.Queue = asyncio.Queue()
    log_buffer: list = []  # Keep all log lines so reconnecting clients can catch up

    async def _log_hook(line: str):
        event = {"type": "log", "line": line}
        log_buffer.append(event)
        await queue.put(event)
        # Fan out to all SSE subscriber queues
        for sub in JOBS.get(job_id, {}).get("subscribers", []):
            try:
                sub.put_nowait(event)
            except Exception:
                pass

    async def _runner():
        try:
            # Yield once so the HTTP response with job_id can flush before
            # the worker starts any potentially blocking command flow.
            await asyncio.sleep(0)
            start_event = {"type": "log", "line": "Session started..."}
            log_buffer.append(start_event)
            await queue.put(start_event)
            for sub in JOBS.get(job_id, {}).get("subscribers", []):
                try:
                    sub.put_nowait(start_event)
                except Exception:
                    pass
            result = await run_worker_task_stream(payload, log_hook=_log_hook)
            done_event = {"type": "done", "result": result}
            log_buffer.append(done_event)
            await queue.put(done_event)
            for sub in JOBS.get(job_id, {}).get("subscribers", []):
                try:
                    sub.put_nowait(done_event)
                except Exception:
                    pass
        except asyncio.CancelledError:
            cancel_event = {"type": "done", "result": {"ok": False, "error": "stopped"}}
            log_buffer.append(cancel_event)
            await queue.put(cancel_event)
            for sub in JOBS.get(job_id, {}).get("subscribers", []):
                try:
                    sub.put_nowait(cancel_event)
                except Exception:
                    pass
            raise
        except Exception as e:
            err_event = {"type": "done", "result": {"ok": False, "error": str(e)[:300]}}
            log_buffer.append(err_event)
            await queue.put(err_event)
            for sub in JOBS.get(job_id, {}).get("subscribers", []):
                try:
                    sub.put_nowait(err_event)
                except Exception:
                    pass

    JOBS[job_id] = {
        "task": None,
        "queue": queue,
        "log_buffer": log_buffer,
        "created_at": asyncio.get_event_loop().time(),
        "subscribers": [],
    }
    task = asyncio.create_task(_runner())
    JOBS[job_id]["task"] = task
    return web.json_response({"ok": True, "job_id": job_id})


async def webapp_stream_handler(request: web.Request) -> web.StreamResponse:
    job_id = (request.match_info.get("job_id") or "").strip()
    meta = JOBS.get(job_id)
    if not meta:
        return web.json_response({"ok": False, "error": "job not found"}, status=404)

    queue: asyncio.Queue = meta["queue"]
    log_buffer: list = meta.get("log_buffer", [])
    origin = request.headers.get("Origin", "")
    sse_headers = {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    if _is_allowed_origin(origin):
        sse_headers["Access-Control-Allow-Origin"] = origin
        sse_headers["Access-Control-Allow-Credentials"] = "true"
        sse_headers["Vary"] = "Origin"

    resp = web.StreamResponse(
        status=200,
        reason="OK",
        headers=sse_headers,
    )
    await resp.prepare(request)

    async def _send(event: Dict[str, Any]):
        payload = json.dumps(event, ensure_ascii=False)
        await resp.write(f"data: {payload}\n\n".encode("utf-8"))

    try:
        await _send({"type": "hello", "job_id": job_id})

        # Replay buffered events so reconnecting clients catch up
        last_id_str = request.headers.get("Last-Event-ID", "")
        replay_from = int(last_id_str) if last_id_str.isdigit() else 0
        done_in_buffer = False
        for idx in range(replay_from, len(log_buffer)):
            evt = log_buffer[idx]
            evt_copy = dict(evt)
            evt_copy["_seq"] = idx + 1
            await _send(evt_copy)
            if evt.get("type") == "done":
                done_in_buffer = True
                break

        if not done_in_buffer:
            # Create a per-client queue that receives new events from the shared log_buffer
            client_queue: asyncio.Queue = asyncio.Queue()
            # Patch: add a subscriber list to the job so new events fan out
            subscribers = meta.setdefault("subscribers", [])
            subscribers.append(client_queue)
            seq = len(log_buffer)
            try:
                while True:
                    try:
                        event = await asyncio.wait_for(client_queue.get(), timeout=25)
                    except asyncio.TimeoutError:
                        # Send keepalive comment to prevent proxy/browser timeout
                        await resp.write(b": keepalive\n\n")
                        continue
                    seq += 1
                    event_copy = dict(event)
                    event_copy["_seq"] = seq
                    await _send(event_copy)
                    if event.get("type") == "done":
                        break
            finally:
                try:
                    subscribers.remove(client_queue)
                except ValueError:
                    pass
    except (asyncio.CancelledError, ConnectionResetError):
        pass
    finally:
        try:
            await resp.write_eof()
        except Exception:
            pass
    return resp


async def webapp_stop_handler(request: web.Request) -> web.Response:
    job_id = (request.match_info.get("job_id") or "").strip()
    meta = JOBS.get(job_id)
    if not meta:
        return web.json_response({"ok": False, "error": "job not found"}, status=404)
    task: asyncio.Task | None = meta.get("task")
    if task and not task.done():
        task.cancel()
    return web.json_response({"ok": True, "stopped": True})


@web.middleware
async def cors_middleware(request: web.Request, handler):
    origin = request.headers.get("Origin", "")
    if request.method == "OPTIONS":
        response = web.Response(status=204)
    else:
        response = await handler(request)
    if _is_allowed_origin(origin):
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Credentials"] = "true"
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type,X-Worker-Secret"
    response.headers["Access-Control-Max-Age"] = "86400"
    response.headers["Vary"] = "Origin"
    return response


def create_app() -> web.Application:
    app = web.Application(middlewares=[cors_middleware])
    app.router.add_get("/health", health_handler)
    app.router.add_post("/run", run_handler)
    app.router.add_post("/webapp/auth/request-code", webapp_request_code_handler)
    app.router.add_post("/webapp/auth/verify-code", webapp_verify_code_handler)
    app.router.add_post("/webapp/run", webapp_run_handler)
    app.router.add_post("/webapp/start", webapp_start_handler)
    app.router.add_get("/webapp/stream/{job_id}", webapp_stream_handler)
    app.router.add_post("/webapp/stop/{job_id}", webapp_stop_handler)
    return app


if __name__ == "__main__":
    app = create_app()
    web.run_app(app, host=WORKER_HOST, port=WORKER_PORT)
