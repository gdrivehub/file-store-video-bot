"""
Live activity log
==================
Mirrors EVERY incoming message the bot receives (private chats and groups)
to ACTIVITY_LOG_CHANNEL in near real time, so an admin can watch a live
feed of what users are sending and quickly spot spam/abuse.

Design notes
------------
* Runs as a low-priority handler (group=-10) with no command/text filter,
  so it sees every message *alongside* all your existing handlers without
  ever blocking or interfering with them (it never raises StopPropagation).
* Never raises/blocks the user-facing flow - logging happens through an
  internal queue + background worker, so even if Telegram is slow or
  flood-limits the log channel, normal bot replies are completely
  unaffected. The queue absorbs bursts; the worker paces sends so the log
  channel itself never gets flood-banned.
* Lightweight in-memory "is this user spamming?" heuristic: if the same
  user sends more than ACTIVITY_SPAM_COUNT messages within
  ACTIVITY_SPAM_WINDOW seconds, the log entry gets a 🚨 flag so it's easy
  to spot at a glance. This is just a visual aid, not an auto-ban.
* Text messages are logged as quoted text; media is copied as-is (photo,
  video, document, voice, sticker, etc.) with the sender info as caption,
  so you see exactly what was sent, not just a description of it.
* Admins can pause/resume the feed at runtime with /logson and /logsoff
  without redeploying.
"""

import logging
import asyncio
import time
import html
import collections

from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait

from info import ADMINS, ACTIVITY_LOG_CHANNEL, ACTIVITY_SPAM_COUNT, ACTIVITY_SPAM_WINDOW

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ── runtime toggle (admin-controlled, in-memory) ────────────────────────────
_enabled = True

# ── queue + background worker so logging never blocks real bot replies ─────
_queue: asyncio.Queue = asyncio.Queue()
_worker_task = None
_worker_lock = asyncio.Lock()
# Gentle pacing between consecutive log-channel sends to stay well clear of
# Telegram's per-chat flood limits even under heavy bot traffic.
_SEND_GAP = 0.35

# ── simple per-user recent-message timestamps, for the spam heuristic ──────
_recent_msgs: dict[int, collections.deque] = collections.defaultdict(lambda: collections.deque(maxlen=20))


def _is_likely_spam(user_id: int) -> bool:
    now = time.monotonic()
    dq = _recent_msgs[user_id]
    dq.append(now)
    while dq and now - dq[0] > ACTIVITY_SPAM_WINDOW:
        dq.popleft()
    return len(dq) >= ACTIVITY_SPAM_COUNT


def _build_header(message) -> str:
    user = message.from_user
    chat = message.chat

    if user:
        name = html.escape(user.first_name or "Unknown")
        uname = f"@{user.username}" if user.username else "no username"
        who = f"👤 <b>{name}</b> ({uname}) — <code>{user.id}</code>"
    else:
        who = "👤 Unknown sender"

    if chat.type == enums.ChatType.PRIVATE:
        where = "💬 Private chat (PM)"
    else:
        title = html.escape(chat.title or "Unknown chat")
        where = f"💬 <b>{title}</b> — <code>{chat.id}</code>"

    spam_tag = ""
    if user and _is_likely_spam(user.id):
        spam_tag = "\n🚨 <b>POSSIBLE SPAM</b> — sending messages rapidly"

    return f"{who}\n{where}{spam_tag}"


async def _ensure_worker(client):
    global _worker_task
    if _worker_task is None or _worker_task.done():
        async with _worker_lock:
            if _worker_task is None or _worker_task.done():
                _worker_task = asyncio.create_task(_worker(client))


async def _worker(client):
    while True:
        message = await _queue.get()
        try:
            await _deliver(client, message)
        except FloodWait as e:
            await asyncio.sleep(e.value)
            try:
                await _deliver(client, message)
            except Exception as e2:
                logger.warning(f"activity log: dropped one entry after FloodWait retry: {e2}")
        except Exception as e:
            logger.warning(f"activity log: failed to deliver entry: {e}")
        await asyncio.sleep(_SEND_GAP)


async def _deliver(client, message):
    header = _build_header(message)

    if message.media:
        # Copy the actual media so the admin sees exactly what was sent.
        try:
            await message.copy(ACTIVITY_LOG_CHANNEL, caption=header, parse_mode=enums.ParseMode.HTML)
            return
        except Exception:
            # Some media types (stickers, polls, locations, contacts...) don't
            # accept a caption override - fall back to header + plain copy.
            await client.send_message(ACTIVITY_LOG_CHANNEL, header, parse_mode=enums.ParseMode.HTML)
            try:
                await message.copy(ACTIVITY_LOG_CHANNEL)
            except Exception as e:
                logger.warning(f"activity log: couldn't copy media body: {e}")
            return

    text = message.text or message.caption or "<i>(empty message)</i>"
    body = f"{header}\n\n💭 {html.escape(text)}"
    # Telegram message length cap safety.
    if len(body) > 4000:
        body = body[:3990] + "…"
    await client.send_message(ACTIVITY_LOG_CHANNEL, body, parse_mode=enums.ParseMode.HTML)


@Client.on_message(filters.command("logson") & filters.user(ADMINS))
async def logs_on(client, message):
    global _enabled
    _enabled = True
    await message.reply_text("✅ Live activity log is now **ON**.")


@Client.on_message(filters.command("logsoff") & filters.user(ADMINS))
async def logs_off(client, message):
    global _enabled
    _enabled = False
    await message.reply_text("⏸️ Live activity log is now **OFF**.")


# group=-10: runs alongside (not instead of) every other handler, on every
# single incoming message, without ever stopping propagation.
@Client.on_message(filters.incoming, group=-10)
async def mirror_to_log(client, message):
    if not _enabled or not ACTIVITY_LOG_CHANNEL:
        return
    # Never log inside the log channel itself (avoids any chance of a loop).
    if message.chat and str(message.chat.id) == str(ACTIVITY_LOG_CHANNEL):
        return
    if not message.from_user and not message.text and not message.media:
        return

    try:
        await _ensure_worker(client)
        _queue.put_nowait(message)
    except Exception as e:
        logger.warning(f"activity log: couldn't queue message: {e}")
