import asyncio
import io
import json
import os
import re
from types import SimpleNamespace
from typing import Any, Awaitable, Callable, Dict, List, Optional

import aiohttp

from commands import co as co_module
from commands import ay as ay_module


class _FakeSentMessage:
    def __init__(self, sink: List[str], text: str, log_hook: Optional[Callable[[str], Awaitable[None]]] = None):
        self._sink = sink
        self.text = text
        self.message_id = len(sink)
        self._log_hook = log_hook

    async def edit_text(self, text: str, **kwargs):
        self.text = text
        self._sink.append(text)
        if self._log_hook:
            await self._log_hook(text)
        return self

    async def delete(self):
        return True


class _FakeBot:
    async def get_file(self, file_id: str):
        return SimpleNamespace(file_path=file_id)

    async def download_file(self, file_path: str):
        return io.BytesIO(b"")

    async def send_message(self, chat_id: int, text: str, **kwargs):
        return SimpleNamespace(message_id=1, text=text)


class _FakeMessage:
    def __init__(
        self,
        text: str,
        user_id: int,
        user_name: str,
        chat_id: int,
        sink: List[str],
        log_hook: Optional[Callable[[str], Awaitable[None]]] = None,
    ):
        self.text = text
        self._sink = sink
        self._log_hook = log_hook
        self.from_user = SimpleNamespace(id=user_id, full_name=user_name, first_name=user_name)
        self.chat = SimpleNamespace(id=chat_id)
        self.reply_to_message = None
        self.bot = _FakeBot()

    async def answer(self, text: str, **kwargs):
        self._sink.append(text)
        if self._log_hook:
            await self._log_hook(text)
        return _FakeSentMessage(self._sink, text, self._log_hook)

    async def reply(self, text: str, **kwargs):
        return await self.answer(text, **kwargs)

    async def answer_document(self, *args, **kwargs):
        self._sink.append("[worker] document response emitted")
        return _FakeSentMessage(self._sink, "[worker] document response emitted")


def _normalize_command_text(command: str, raw_text: str) -> str:
    text = (raw_text or "").strip()
    if command == "co":
        if text.startswith("/co2"):
            return text
        if text.startswith("/co"):
            return text
        return f"/co {text}".strip()
    if command == "co2":
        if text.startswith("/co2"):
            return text
        if text.startswith("/co"):
            return "/co2 " + text[3:].strip()
        return f"/co2 {text}".strip()
    if command == "url":
        if text.startswith("/url2"):
            return text
        if text.startswith("/url"):
            return text
        return f"/url {text}".strip()
    if command == "url2":
        if text.startswith("/url2"):
            return text
        if text.startswith("/url"):
            return "/url2 " + text[4:].strip()
        return f"/url2 {text}".strip()
    if command == "ay":
        if text.startswith("/ay"):
            return text
        return f"/ay {text}".strip()
    if command == "status":
        return "/mystatus"
    if command == "proxy":
        if text.startswith("/proxy"):
            return text
        return "/proxy check"
    if command == "redeem":
        if text.startswith("/redeem"):
            return text
        return f"/redeem {text}".strip()
    if command == "addproxy":
        if text.startswith("/addproxy"):
            return text
        return f"/addproxy {text}".strip()
    return text


async def run_worker_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    return await run_worker_task_stream(payload)


async def run_worker_task_stream(
    payload: Dict[str, Any],
    log_hook: Optional[Callable[[str], Awaitable[None]]] = None,
) -> Dict[str, Any]:
    command = (payload.get("command") or "").strip().lower()
    user_id = int(payload.get("user_id") or 0)
    user_name = (payload.get("user_name") or "WorkerUser").strip() or "WorkerUser"
    chat_id = int(payload.get("chat_id") or user_id or 0)
    text = _normalize_command_text(command, payload.get("text") or "")

    if command == "history":
        entries = _load_user_history(user_id)
        lines = ["HIT HISTORY"] + [
            f"{idx+1}. {e.get('merchant','N/A')} | {e.get('amount','N/A')} | {e.get('result','-')} | {e.get('card_masked','N/A')}"
            for idx, e in enumerate(entries[-20:])
        ]
        return {"ok": True, "messages": lines if len(lines) > 1 else ["HIT HISTORY\nNo records yet."]}

    messages: List[str] = []
    fake_msg = _FakeMessage(
        text=text,
        user_id=user_id,
        user_name=user_name,
        chat_id=chat_id,
        sink=messages,
        log_hook=log_hook,
    )

    # WebApp traffic can lose Telegram user context on some clients.
    # Bypass premium gate for worker execution path only.
    original_co_check_access = getattr(co_module, "check_access", None)
    original_ay_check_access = getattr(ay_module, "check_access", None)
    co_module.check_access = lambda _msg: True
    ay_module.check_access = lambda _msg: True
    try:
        if command == "co":
            await co_module.co_handler(fake_msg)
        elif command == "co2":
            await co_module.co2_handler(fake_msg)
        elif command == "url":
            await co_module.url_handler(fake_msg)
        elif command == "url2":
            await co_module.url2_handler(fake_msg)
        elif command == "ay":
            await ay_module.ay_handler(fake_msg)
        elif command == "status":
            await co_module.mystatus_handler(fake_msg)
        elif command == "proxy":
            await co_module.proxy_handler(fake_msg)
        elif command == "redeem":
            await co_module.redeem_handler(fake_msg)
        elif command == "addproxy":
            await co_module.addproxy_handler(fake_msg)
        else:
            raise ValueError("Unsupported command")
    finally:
        if original_co_check_access is not None:
            co_module.check_access = original_co_check_access
        if original_ay_check_access is not None:
            ay_module.check_access = original_ay_check_access

    cleaned = [m for m in messages if isinstance(m, str) and m.strip()]
    if not cleaned:
        cleaned = ["Worker finished but no message was generated."]
    recent_messages = cleaned[-10:]

    hit_info = _extract_hit_info(recent_messages)
    notify_group = bool(payload.get("notify_group"))
    show_card = bool(payload.get("show_card"))
    group_notified = False
    history_saved = False

    if hit_info.get("hit"):
        history_saved = _save_hit_history(user_id, user_name, hit_info)
        if notify_group:
            group_notified = await _send_group_hit_notification(user_name, hit_info, show_card)

    return {
        "ok": True,
        "messages": recent_messages,
        "hit": bool(hit_info.get("hit")),
        "group_notified": group_notified,
        "history_saved": history_saved,
    }


HISTORY_FILE = "webapp_hit_history.json"


def _extract_hit_info(lines: List[str]) -> Dict[str, Any]:
    text = "\n".join(lines)
    upper = text.upper()
    is_hit = any(k in upper for k in ("CHARGED", "APPROVED", "BIN HIT"))
    if not is_hit:
        return {"hit": False}

    merchant = _first_match(text, [r"Merchant\s*:\s*(.+)"])
    amount = _first_match(text, [r"Amount\s*:\s*(.+)", r"Price\s*:\s*(.+)"])
    attempts = _first_match(text, [r"Attempts\s*:\s*(\d+)"]) or "1"
    card = _first_match(text, [r"(\d{12,19}\|\d{1,2}\|\d{2,4}\|\d{3,4})"])
    result = _first_match(text, [r"Result\s*:\s*(.+)"]) or "CHARGED"

    return {
        "hit": True,
        "merchant": (merchant or "N/A").strip(),
        "amount": (amount or "N/A").strip(),
        "attempts": str(attempts).strip(),
        "card": (card or "").strip(),
        "result": result.strip(),
    }


def _first_match(text: str, patterns: List[str]) -> str:
    for p in patterns:
        m = re.search(p, text, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return ""


def _mask_card(card: str) -> str:
    if not card:
        return "N/A"
    parts = card.split("|")
    cc = parts[0] if parts else card
    if len(cc) < 10:
        return cc
    masked = cc[:6] + "*" * (len(cc) - 10) + cc[-4:]
    if len(parts) >= 4:
        return f"{masked}|{parts[1]}|{parts[2]}|***"
    return masked


def _save_hit_history(user_id: int, user_name: str, hit: Dict[str, Any]) -> bool:
    try:
        data: Dict[str, List[Dict[str, Any]]] = {}
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        key = str(user_id or 0)
        entries = data.get(key, [])
        entries.append(
            {
                "time": __import__("datetime").datetime.utcnow().isoformat() + "Z",
                "user_name": user_name,
                "merchant": hit.get("merchant", "N/A"),
                "amount": hit.get("amount", "N/A"),
                "attempts": hit.get("attempts", "1"),
                "result": hit.get("result", "CHARGED"),
                "card_masked": _mask_card(hit.get("card", "")),
            }
        )
        data[key] = entries[-50:]
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception:
        return False


def _render_group_hit_text(user_name: str, hit: Dict[str, Any], show_card: bool) -> str:
    card_val = hit.get("card", "N/A") if show_card else _mask_card(hit.get("card", ""))
    return (
        "------------------------------\n"
        "BIN HIT - CHARGED\n"
        "------------------------------\n"
        f"User     : {user_name}\n"
        f"Merchant : {hit.get('merchant', 'N/A')}\n"
        f"Amount   : {hit.get('amount', 'N/A')}\n"
        f"Card     : {card_val}\n"
        f"Attempts : {hit.get('attempts', '1')}\n"
        f"Result   : {hit.get('result', 'CHARGED')}\n"
        "------------------------------\n"
        "  Thanks for using J.Huynh BOT\n"
        "       By @jackhuynh1001\n"
        "------------------------------"
    )


async def _send_group_hit_notification(user_name: str, hit: Dict[str, Any], show_card: bool) -> bool:
    token = (os.getenv("BOT_TOKEN") or "").strip()
    if not token:
        try:
            from config import BOT_TOKEN as CFG_BOT_TOKEN  # type: ignore
            token = (CFG_BOT_TOKEN or "").strip()
        except Exception:
            token = ""
    if not token:
        return False
    chat_id = getattr(co_module, "CHARGED_GROUP", None)
    if not chat_id:
        return False
    text = _render_group_hit_text(user_name, hit, show_card)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": f"<pre>{text}</pre>", "parse_mode": "HTML"}
    try:
        timeout = aiohttp.ClientTimeout(total=12)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.post(url, json=payload) as resp:
                return resp.status == 200
    except Exception:
        return False


def _load_user_history(user_id: int) -> List[Dict[str, Any]]:
    try:
        if not os.path.exists(HISTORY_FILE):
            return []
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return list(data.get(str(user_id or 0), []))
    except Exception:
        return []


if __name__ == "__main__":
    raise SystemExit("Use from worker_server.py")
