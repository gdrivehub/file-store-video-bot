"""
/getvid feature
================
Sends a random video from an admin-configured Telegram channel.

Commands
--------
/setvidchannel <channel_id | @username>   (admin only)
    Sets/changes the source channel. Automatically (re)indexes it.

/indexvidchannel                          (admin only)
    Force a re-scan of the configured channel (e.g. after new videos were
    added) without changing which channel is active.

/vidstatus                                (admin only)
    Shows how many videos are indexed and how big the pool is.

/getvid                                   (anyone, PM or group)
    Sends one random video the calling user hasn't seen yet (per-user,
    no repeats until the whole pool has been exhausted, then it reshuffles
    and starts a fresh cycle automatically).

Anti-spam / performance design
-------------------------------
* A single process-wide asyncio.Lock serializes all /getvid sends. Inside
  the lock we check how long it's been since the last video was actually
  sent and asyncio.sleep() just enough to guarantee a >= GETVID_DELAY
  (default 0.4s) gap before sending the next one. This means: if a user
  (or many users) spams /getvid, requests queue up FIFO and each one still
  gets a video - just throttled - instead of overloading Telegram/Mongo.
* The same lock conveniently also makes the "pick next id / advance
  pointer" read-modify-write on a user's progress document race-free,
  with zero extra locking machinery.
* The channel is indexed ONCE (message ids only - a few bytes per video)
  and cached in Mongo. Normal /getvid calls never touch the Telegram API
  except to fetch+send the single chosen video, so they stay fast even
  with a channel containing thousands of videos.
"""

import logging
import re
import random
import asyncio
import time

from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait
from pyrogram.errors.exceptions.bad_request_400 import ChannelInvalid, ChatAdminRequired, UsernameInvalid, UsernameNotModified

from info import ADMINS, GETVID_CHANNEL, GETVID_DELAY, PROTECT_CONTENT
from database import getvid_db as gvdb

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ── Global anti-spam gate (see module docstring) ────────────────────────────
_send_lock = asyncio.Lock()
_last_sent_at = 0.0

# Indexing has its own lock so two admins can't trigger a double-scan.
_index_lock = asyncio.Lock()

# Safety cap on how many "dead message" retries we do per request before
# giving up - keeps a single /getvid call bounded even if a channel has a
# lot of stale deleted ids in its pool.
_MAX_DEAD_RETRIES = 5

_LINK_RE = re.compile(r"(https://)?(t\.me/|telegram\.me/|telegram\.dog/)(c/)?(\d+|[a-zA-Z_0-9]+)/(\d+)$")


def _is_admin(user_id):
    return user_id in ADMINS


async def _resolve_channel():
    """Runtime DB setting wins; falls back to GETVID_CHANNEL env var."""
    ch = await gvdb.get_active_channel()
    if ch:
        return ch
    return GETVID_CHANNEL


async def _resolve_target(client, message):
    """
    Bots cannot call messages.GetHistory (Telegram blocks it for bot accounts),
    so we can't "browse" a channel's history blindly. Instead - exactly like
    this repo's existing /index flow - we need the channel's LAST message id,
    obtained either by:
      (a) the admin forwarding the channel's most recent post to the bot, or
      (b) the admin pasting a https://t.me/<channel>/<id> link to that post.
    Returns (chat_id, last_msg_id) or (None, None) + sends an error reply.
    """
    chat_id = None
    last_msg_id = None

    if message.text:
        match = _LINK_RE.search(message.text.strip())
        if match:
            chat_id = match.group(4)
            last_msg_id = int(match.group(5))
            if chat_id.isnumeric():
                chat_id = int("-100" + chat_id)

    if chat_id is None and message.reply_to_message:
        fwd = message.reply_to_message
        if fwd.forward_from_chat and fwd.forward_from_chat.type == enums.ChatType.CHANNEL:
            last_msg_id = fwd.forward_from_message_id
            chat_id = fwd.forward_from_chat.username or fwd.forward_from_chat.id

    if chat_id is None:
        await message.reply_text(
            "<b>Usage:</b>\n"
            "1️⃣ Forward the <u>most recent post</u> from the target channel here, "
            "then reply to that forwarded message with:\n"
            "<code>/setvidchannel</code>\n\n"
            "2️⃣ Or just send its link directly:\n"
            "<code>/setvidchannel https://t.me/channelname/12345</code>\n\n"
            "ℹ️ I can't scan a channel's full history myself (Telegram blocks that "
            "for bots) — I need to know the id of the latest post once, then I scan "
            "everything from 1 up to that id using bot-safe calls.",
            parse_mode=enums.ParseMode.HTML,
        )
        return None, None

    return chat_id, last_msg_id


async def _scan_channel_for_videos(client, chat_id, last_msg_id, status=None):
    """
    Walk message ids 1..last_msg_id in batches via get_messages (bot-API safe,
    unlike get_chat_history/iter_messages-by-offset which bots can't use) and
    collect every video message id. Reports progress on `status` if given.
    """
    ids = []
    current = 1
    batch = 200
    while current <= last_msg_id:
        chunk_end = min(current + batch - 1, last_msg_id)
        id_list = list(range(current, chunk_end + 1))
        try:
            messages = await client.get_messages(chat_id, id_list)
        except FloodWait as e:
            await asyncio.sleep(e.value)
            messages = await client.get_messages(chat_id, id_list)
        for m in messages:
            if m and not m.empty and m.video:
                ids.append(m.id)
        current = chunk_end + 1
        if status and current % 2000 < batch:
            try:
                await status.edit_text(f"🔎 Scanning… {current}/{last_msg_id} messages checked, {len(ids)} videos found so far.")
            except Exception:
                pass
    return ids


@Client.on_message(filters.command("setvidchannel") & filters.user(ADMINS))
async def set_vid_channel(client, message):
    chat_id, last_msg_id = await _resolve_target(client, message)
    if chat_id is None:
        return  # usage message already sent

    status = await message.reply_text("🔎 Checking channel access…")
    try:
        probe = await client.get_messages(chat_id, last_msg_id)
    except (ChannelInvalid, UsernameInvalid, UsernameNotModified):
        return await status.edit_text(
            "❌ Invalid channel, or I'm not a member/admin there yet. "
            "Add me as admin in that channel first, then try again."
        )
    except ChatAdminRequired:
        return await status.edit_text("❌ I need to be an admin in that channel.")
    except Exception as e:
        logger.exception(e)
        return await status.edit_text(f"❌ Error accessing channel: {e}")

    if probe is None or probe.empty:
        return await status.edit_text(
            "❌ Couldn't read that message. Make sure I'm an admin in the channel "
            "and the link/forwarded post is correct."
        )

    if _index_lock.locked():
        return await status.edit_text("⏳ An indexing job is already running, please wait and try again shortly.")

    try:
        async with _index_lock:
            await status.edit_text("🔎 Scanning channel for videos, please wait… (this can take a bit for large channels)")
            ids = await _scan_channel_for_videos(client, chat_id, last_msg_id, status)
            await gvdb.save_pool(chat_id, ids)
            await gvdb.set_active_channel(chat_id)
    except Exception as e:
        logger.exception(e)
        return await status.edit_text(f"❌ Failed to index channel: {e}")

    if not ids:
        return await status.edit_text(
            "⚠️ Channel set, but I couldn't find any videos in it yet.\n"
            "Run /indexvidchannel again later after videos are added."
        )

    await status.edit_text(
        f"✅ <b>/getvid channel set!</b>\n\n"
        f"Channel: <code>{chat_id}</code>\n"
        f"Videos indexed: <code>{len(ids)}</code>\n\n"
        f"Users can now use /getvid to get random videos from it.",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_message(filters.command("indexvidchannel") & filters.user(ADMINS))
async def reindex_vid_channel(client, message):
    channel = await _resolve_channel()
    if not channel:
        return await message.reply_text(
            "No channel configured yet. Use /setvidchannel first."
        )

    chat_id, last_msg_id = await _resolve_target(client, message)
    if chat_id is None:
        return  # usage message already sent (must point at the same channel)

    if _index_lock.locked():
        return await message.reply_text("⏳ An indexing job is already running, please wait.")

    status = await message.reply_text("🔎 Re-scanning channel for videos, please wait…")
    try:
        async with _index_lock:
            ids = await _scan_channel_for_videos(client, chat_id, last_msg_id, status)
            await gvdb.save_pool(chat_id, ids)
    except Exception as e:
        logger.exception(e)
        return await status.edit_text(f"❌ Failed to index channel: {e}")

    await status.edit_text(
        f"✅ Re-indexed <code>{chat_id}</code>\nVideos found: <code>{len(ids)}</code>",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_message(filters.command("vidstatus") & filters.user(ADMINS))
async def vid_status(client, message):
    channel = await _resolve_channel()
    if not channel:
        return await message.reply_text("No /getvid channel configured. Use /setvidchannel.")
    count = await gvdb.pool_count(channel)
    await message.reply_text(
        f"📺 <b>/getvid status</b>\n\nChannel: <code>{channel}</code>\nIndexed videos: <code>{count}</code>",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_message(filters.command("getvid") & filters.incoming)
async def get_random_video(client, message):
    global _last_sent_at

    user = message.from_user
    if not user:
        return await message.reply_text("Couldn't identify you, please try again from a normal account.")
    user_id = user.id

    channel = await _resolve_channel()
    if not channel:
        return await message.reply_text(
            "⚠️ No video channel has been configured for /getvid yet. "
            "An admin needs to run /setvidchannel first."
        )

    pool = await gvdb.get_pool(channel)
    if not pool:
        return await message.reply_text(
            "⚠️ The configured channel has no indexed videos yet. "
            "An admin should run /indexvidchannel."
        )

    # Let the requester know immediately if there's already a queue, so a
    # spam-tap doesn't look like the bot ignored them.
    if _send_lock.locked():
        ack = await message.reply_text("⏳ Queued…")
    else:
        ack = None

    async with _send_lock:
        # ── global throttle: guarantee >= GETVID_DELAY between sends ──────
        elapsed = time.monotonic() - _last_sent_at
        if elapsed < GETVID_DELAY:
            await asyncio.sleep(GETVID_DELAY - elapsed)

        # Re-fetch pool in case it was just reindexed mid-wait.
        pool = await gvdb.get_pool(channel)
        if not pool:
            _last_sent_at = time.monotonic()
            if ack:
                await ack.delete()
            return await message.reply_text("⚠️ No videos available right now.")

        order, pointer = await gvdb.get_or_init_progress(user_id, channel, pool)

        sent = False
        dead_ids = []
        attempts = 0
        chosen_msg = None

        while not sent and attempts < min(_MAX_DEAD_RETRIES, len(order)):
            if pointer >= len(order):
                # Finished a full cycle - reshuffle and start over.
                order = pool[:]
                random.shuffle(order)
                pointer = 0

            video_msg_id = order[pointer]
            pointer += 1
            attempts += 1

            try:
                chosen_msg = await client.get_messages(channel, video_msg_id)
                if not chosen_msg or chosen_msg.empty or not chosen_msg.video:
                    dead_ids.append(video_msg_id)
                    continue
                sent = True
            except FloodWait as e:
                await asyncio.sleep(e.value)
                try:
                    chosen_msg = await client.get_messages(channel, video_msg_id)
                    if chosen_msg and not chosen_msg.empty and chosen_msg.video:
                        sent = True
                    else:
                        dead_ids.append(video_msg_id)
                except Exception:
                    dead_ids.append(video_msg_id)
            except Exception:
                dead_ids.append(video_msg_id)

        if dead_ids:
            asyncio.create_task(gvdb.remove_dead_ids(channel, dead_ids))

        if not sent or chosen_msg is None:
            await gvdb.advance_pointer(user_id, pointer, order)
            _last_sent_at = time.monotonic()
            if ack:
                await ack.delete()
            return await message.reply_text(
                "⚠️ Couldn't fetch a video right now (some indexed videos may have "
                "been deleted). Please try /getvid again."
            )

        try:
            await chosen_msg.copy(
                chat_id=message.chat.id,
                reply_to_message_id=message.id,
                protect_content=PROTECT_CONTENT,
            )
        except FloodWait as e:
            await asyncio.sleep(e.value)
            try:
                await chosen_msg.copy(
                    chat_id=message.chat.id,
                    reply_to_message_id=message.id,
                    protect_content=PROTECT_CONTENT,
                )
            except Exception as e2:
                logger.exception(e2)
                await gvdb.advance_pointer(user_id, pointer, order)
                _last_sent_at = time.monotonic()
                if ack:
                    await ack.delete()
                return await message.reply_text(f"❌ Failed to send video: {e2}")
        except Exception as e:
            logger.exception(e)
            await gvdb.advance_pointer(user_id, pointer, order)
            _last_sent_at = time.monotonic()
            if ack:
                await ack.delete()
            return await message.reply_text(f"❌ Failed to send video: {e}")

        await gvdb.advance_pointer(user_id, pointer, order)
        _last_sent_at = time.monotonic()

    if ack:
        try:
            await ack.delete()
        except Exception:
            pass
