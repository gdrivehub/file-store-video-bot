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
import random
import asyncio
import time

from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait
from pyrogram.errors.exceptions.bad_request_400 import ChannelInvalid, UsernameInvalid, UsernameNotModified

from info import ADMINS, GETVID_CHANNEL, GETVID_DELAY, PROTECT_CONTENT
from info import fix_channel_id
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


def _is_admin(user_id):
    return user_id in ADMINS


async def _resolve_channel():
    """Runtime DB setting wins; falls back to GETVID_CHANNEL env var."""
    ch = await gvdb.get_active_channel()
    if ch:
        return ch
    return GETVID_CHANNEL


async def _scan_channel_for_videos(bot, channel):
    """
    Walk the full channel history once and collect every video message id.
    Uses chunked get_chat_history (200 msgs/call) - fine even for channels
    with tens of thousands of messages since this only runs on demand.
    """
    ids = []
    async for message in bot.get_chat_history(channel):
        if message and not message.empty and message.video:
            ids.append(message.id)
    return ids


@Client.on_message(filters.command("setvidchannel") & filters.user(ADMINS))
async def set_vid_channel(client, message):
    if len(message.command) < 2:
        return await message.reply_text(
            "<b>Usage:</b> <code>/setvidchannel @channelusername</code> or "
            "<code>/setvidchannel -1001234567890</code>\n\n"
            "I must already be an <b>admin</b> in that channel.",
            parse_mode=enums.ParseMode.HTML,
        )
    raw = message.command[1].strip()
    channel = fix_channel_id(raw)

    status = await message.reply_text("🔎 Checking channel access & indexing videos, please wait…")
    try:
        await client.get_chat(channel)
    except (ChannelInvalid, UsernameInvalid, UsernameNotModified):
        return await status.edit_text(
            "❌ Invalid channel, or I'm not a member/admin there yet. "
            "Add me as admin in that channel first."
        )
    except Exception as e:
        logger.exception(e)
        return await status.edit_text(f"❌ Error accessing channel: {e}")

    try:
        async with _index_lock:
            ids = await _scan_channel_for_videos(client, channel)
            await gvdb.save_pool(channel, ids)
            await gvdb.set_active_channel(channel)
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
        f"Channel: <code>{channel}</code>\n"
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

    if _index_lock.locked():
        return await message.reply_text("⏳ An indexing job is already running, please wait.")

    status = await message.reply_text("🔎 Re-scanning channel for videos, please wait…")
    try:
        async with _index_lock:
            ids = await _scan_channel_for_videos(client, channel)
            await gvdb.save_pool(channel, ids)
    except Exception as e:
        logger.exception(e)
        return await status.edit_text(f"❌ Failed to index channel: {e}")

    await status.edit_text(
        f"✅ Re-indexed <code>{channel}</code>\nVideos found: <code>{len(ids)}</code>",
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
