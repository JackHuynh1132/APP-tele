"""
Telegram Web App handler - processes data sent from the Mini App
and adds the "Open App" button to /start command.
"""

import json
from aiogram import Router, F
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    WebAppInfo,
    ContentType,
)
from aiogram.filters import Command
from aiogram.enums import ParseMode

from commands import co as co_module

router = Router()

WEBAPP_URL = "https://jackhuynh1132.github.io/APP-tele/index.html"
OWNER_ID = 1911136815

# App lock state: True = only owner can open, False = everyone can open
APP_LOCKED = False


def get_webapp_keyboard() -> ReplyKeyboardMarkup:
    """Create reply keyboard with Open App button (private chat only)."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(
                text="Open App",
                web_app=WebAppInfo(url=WEBAPP_URL),
            )]
        ],
        resize_keyboard=True,
    )


@router.message(Command("lockapp"))
async def lock_app(msg: Message):
    """Owner only: lock app so only owner can open it."""
    global APP_LOCKED
    if msg.from_user.id != OWNER_ID:
        await msg.answer("<pre>[ ACCESS DENIED ]</pre>", parse_mode=ParseMode.HTML)
        return
    APP_LOCKED = True
    await msg.answer("<pre>[ APP LOCKED ]\nOnly you can open the app now.</pre>", parse_mode=ParseMode.HTML)


@router.message(Command("unlockapp"))
async def unlock_app(msg: Message):
    """Owner only: unlock app for everyone."""
    global APP_LOCKED
    if msg.from_user.id != OWNER_ID:
        await msg.answer("<pre>[ ACCESS DENIED ]</pre>", parse_mode=ParseMode.HTML)
        return
    APP_LOCKED = False
    await msg.answer("<pre>[ APP UNLOCKED ]\nEveryone can open the app now.</pre>", parse_mode=ParseMode.HTML)


@router.message(Command("app"))
async def app_command(msg: Message):
    """Send the Open App button. Blocked for non-owners when locked."""
    if APP_LOCKED and msg.from_user.id != OWNER_ID:
        await msg.answer(
            "<pre>[ APP LOCKED ]\nApp is currently restricted.\nContact admin.</pre>",
            parse_mode=ParseMode.HTML,
        )
        return
    await msg.answer(
        "<b>J.HUYNH BOT</b>\n\n"
        "Tap the button below to open the app.",
        parse_mode=ParseMode.HTML,
        reply_markup=get_webapp_keyboard(),
    )


class _WebAppAliasMessage:
    """Reuse existing command handlers with custom text from WebApp."""

    def __init__(self, original: Message, new_text: str):
        self._original = original
        self.text = new_text

    def __getattr__(self, name):
        return getattr(self._original, name)


async def _dispatch_command_from_webapp(msg: Message, command_text: str):
    low = command_text.lower()
    alias = _WebAppAliasMessage(msg, command_text)

    if low.startswith("/co2"):
        await co_module.co2_handler(alias)
        return
    if low.startswith("/co"):
        await co_module.co_handler(alias)
        return
    if low.startswith("/url2"):
        await co_module.url2_handler(alias)
        return
    if low.startswith("/url"):
        await co_module.url_handler(alias)
        return

    await msg.answer(
        f"<pre>[ WEBAPP ]\nUnsupported direct command:\n{command_text}\n\nSend this command in chat.</pre>",
        parse_mode=ParseMode.HTML,
    )


@router.message(F.content_type == ContentType.WEB_APP_DATA)
async def handle_webapp_data(msg: Message):
    """Process data sent from the Telegram Mini App via tg.sendData()."""
    if APP_LOCKED and msg.from_user.id != OWNER_ID:
        await msg.answer("<pre>[ APP LOCKED ]\nAccess denied.</pre>", parse_mode=ParseMode.HTML)
        return

    try:
        data = json.loads(msg.web_app_data.data)
    except (json.JSONDecodeError, AttributeError):
        await msg.answer("Invalid data from app.")
        return

    action = data.get("action")
    user_id = data.get("user_id", msg.from_user.id)

    if action == "login":
        await msg.answer(
            f"Logged in via Web App\nUser ID: <code>{user_id}</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    if action in {"command", "run_command"}:
        command_text = (data.get("command") or data.get("command_text") or "").strip()
        if not command_text:
            await msg.answer("No command received.")
            return
        await _dispatch_command_from_webapp(msg, command_text)
        return

    await msg.answer("Unknown action from app.")
