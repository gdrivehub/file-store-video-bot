"""
Interactive /broadcast
=======================
Admin-friendly broadcast flow:

  1. Admin sends /broadcast (no reply needed)
  2. Bot asks the admin to send the content (text/photo/video/etc.)
  3. Admin sends ANY message (text, photo, video, document, ...)
  4. Bot copies it back as a live PREVIEW exactly as users will see it,
     with "✅ Confirm & Broadcast" / "❌ Cancel" buttons
  5. Admin either taps Confirm, or types /done — both trigger the actual
     broadcast to every user who has ever started the bot (same delivery
     logic as the existing /broadcast-by-reply command: per-user copy,
     flood-wait handling, auto-cleanup of blocked/deleted accounts).
     /cancel (or the Cancel button) aborts at any point.

This is purely additive - it does not touch or replace the existing
reply-based /broadcast, /grp_broadcast, /bcast_btn or scheduled-broadcast
commands in plugins/broadcast.py; it only adds this new guided flow.

Implementation notes
---------------------
* Per-admin state is kept in memory (dict keyed by admin user_id). A
  10-minute inactivity timeout auto-clears stale flows so an admin can
  never get "stuck" if they abandon the flow.
* The content-capturing handler only fires for an admin who is currently
  mid-flow AND only consumes non-command messages, so it never interferes
  with any other command or feature - everything else passes through
  completely normally for everyone, admins included.
"""

import asyncio
import datetime
import time
import logging

from pyrogram import Client, filters
from pyrogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

from info import ADMINS
from database.users_chats_db import db
from utils import broadcast_messages

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# user_id -> {"state": "awaiting_content" | "preview", "message": Message, "ts": float}
_pending: dict[int, dict] = {}

_FLOW_TIMEOUT_SECONDS = 600  # 10 minutes of inactivity auto-cancels the flow

_CONFIRM_BUTTONS = InlineKeyboardMarkup(
    [[
        InlineKeyboardButton("✅ Confirm & Broadcast", callback_data="bcast_confirm"),
        InlineKeyboardButton("❌ Cancel", callback_data="bcast_cancel"),
    ]]
)


def _is_stale(state: dict) -> bool:
    return (time.monotonic() - state.get("ts", 0)) > _FLOW_TIMEOUT_SECONDS


def _clear(user_id: int):
    _pending.pop(user_id, None)


@Client.on_message(filters.command("broadcast") & filters.user(ADMINS) & ~filters.reply)
async def start_broadcast_flow(client, message: Message):
    # NOTE: the existing reply-based /broadcast (filters.reply) keeps working
    # unchanged in plugins/broadcast.py - this only handles the no-reply case.
    user_id = message.from_user.id
    _pending[user_id] = {"state": "awaiting_content", "ts": time.monotonic()}
    await message.reply_text(
        "📢 <b>Broadcast — step 1 of 2</b>\n\n"
        "Send me the message you want to broadcast — text, photo, video, "
        "document, anything. I'll show you a preview before it goes out.\n\n"
        "Send /cancel anytime to abort.",
        parse_mode="html",
    )


@Client.on_message(filters.command("cancel") & filters.user(ADMINS))
async def cancel_broadcast_flow(client, message: Message):
    user_id = message.from_user.id
    if user_id in _pending:
        _clear(user_id)
        await message.reply_text("❌ Broadcast cancelled.")
    # If there was no pending flow, silently do nothing here so /cancel
    # doesn't produce confusing noise for admins using it for something else.


@Client.on_message(
    filters.user(ADMINS)
    & ~filters.command(["broadcast", "cancel", "done"])
    & filters.private,
    group=-5,
)
async def capture_broadcast_content(client, message: Message):
    user_id = message.from_user.id
    state = _pending.get(user_id)
    if not state or state["state"] != "awaiting_content":
        return  # not in this flow - let every other handler process normally
    if _is_stale(state):
        _clear(user_id)
        return await message.reply_text(
            "⌛ That broadcast setup timed out. Send /broadcast to start again."
        )

    _pending[user_id] = {"state": "preview", "message": message, "ts": time.monotonic()}

    preview = await message.copy(chat_id=user_id)
    await preview.reply_text(
        "👆 <b>This is exactly what users will receive.</b>\n\n"
        "Send /done or tap below to confirm and broadcast, or /cancel to abort.",
        parse_mode="html",
        reply_markup=_CONFIRM_BUTTONS,
    )


async def _run_broadcast(client, source_message: Message, status_target):
    """Shared delivery loop - mirrors the existing reply-based /broadcast logic."""
    start_time = time.time()
    total_users = await db.total_users_count()
    done = success = blocked = deleted = failed = 0

    users = await db.get_all_users()
    async for user in users:
        ok, reason = await broadcast_messages(int(user["id"]), source_message)
        if ok:
            success += 1
        elif reason == "Blocked":
            blocked += 1
        elif reason == "Deleted":
            deleted += 1
        else:
            failed += 1
        done += 1
        await asyncio.sleep(0.05)
        if not done % 20:
            try:
                await status_target.edit(
                    f"📢 Broadcasting…\n\nTotal: {total_users} | Done: {done}\n"
                    f"Success: {success} | Blocked: {blocked} | Deleted: {deleted}"
                )
            except Exception:
                pass

    time_taken = datetime.timedelta(seconds=int(time.time() - start_time))
    await status_target.edit(
        f"✅ <b>Broadcast Completed</b> in {time_taken}!\n\n"
        f"Total: {total_users} | Done: {done}\n"
        f"Success: {success} | Blocked: {blocked} | Deleted: {deleted} | Failed: {failed}",
        parse_mode="html",
    )


async def _start_delivery(client, user_id, source_message, reply_target):
    _clear(user_id)
    status = await reply_target.reply_text("📢 Starting broadcast…")
    await _run_broadcast(client, source_message, status)


@Client.on_message(filters.command("done") & filters.user(ADMINS))
async def confirm_broadcast_flow(client, message: Message):
    user_id = message.from_user.id
    state = _pending.get(user_id)
    if not state or state["state"] != "preview":
        return await message.reply_text(
            "Nothing to confirm. Start a broadcast with /broadcast first."
        )
    if _is_stale(state):
        _clear(user_id)
        return await message.reply_text(
            "⌛ That broadcast setup timed out. Send /broadcast to start again."
        )
    await _start_delivery(client, user_id, state["message"], message)


@Client.on_callback_query(filters.regex(r"^bcast_confirm$"))
async def confirm_broadcast_button(client, query: CallbackQuery):
    user_id = query.from_user.id
    if user_id not in ADMINS:
        return await query.answer("Not authorized.", show_alert=True)
    state = _pending.get(user_id)
    if not state or state["state"] != "preview":
        return await query.answer("This broadcast preview has expired.", show_alert=True)
    if _is_stale(state):
        _clear(user_id)
        return await query.answer("⌛ Timed out — send /broadcast again.", show_alert=True)

    await query.answer("Broadcasting…")
    try:
        await query.message.edit_reply_markup(None)
    except Exception:
        pass
    await _start_delivery(client, user_id, state["message"], query.message)


@Client.on_callback_query(filters.regex(r"^bcast_cancel$"))
async def cancel_broadcast_button(client, query: CallbackQuery):
    user_id = query.from_user.id
    if user_id not in ADMINS:
        return await query.answer("Not authorized.", show_alert=True)
    _clear(user_id)
    await query.answer("Cancelled.")
    try:
        await query.message.edit_reply_markup(None)
    except Exception:
        pass
    await query.message.reply_text("❌ Broadcast cancelled.")
