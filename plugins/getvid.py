"""
/getvid feature
================
Sends ONE random, not-yet-seen video to the user — no filename/keyword
involved, just "give me a random video".

Where the videos come from
---------------------------
This bot already indexes channels via the existing /index flow (forward a
post + confirm) into the shared `Media` collection, tagging each entry with
file_type "video" / "document" / "audio". /getvid simply draws from every
entry already tagged file_type == "video" in that SAME collection — so as
soon as you index a channel the normal way, those videos are immediately
available to /getvid too. There is nothing extra to set up.

Commands
--------
/getvid       (anyone, PM or group) — sends one random unseen video.
/vidstatus    (admin only)          — shows how many videos are available.

Anti-spam / performance design
-------------------------------
* A single process-wide asyncio.Lock serializes all /getvid sends. Inside
  the lock we check how long it's been since the last video was actually
  sent and asyncio.sleep() just enough to guarantee a >= GETVID_DELAY
  (default 0.4s) gap before sending the next one. This means: if a user
  (or many users) spams /getvid, requests queue up FIFO and each one still
  gets a video - just throttled - instead of overloading Telegram/Mongo.
* The same lock makes the per-user "pick next id / advance pointer"
  read-modify-write race-free, with no extra locking machinery needed.
* The list of indexed video file_ids is cached and only refreshed when the
  count of indexed videos actually changes, so normal /getvid calls do
  practically no DB work beyond reading a cached list + a tiny user doc.
"""

import logging
import asyncio
import time

from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait

from info import ADMINS, GETVID_DELAY, PROTECT_CONTENT
from database import getvid_db as gvdb

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ── Global anti-spam gate (see module docstring) ────────────────────────────
_send_lock = asyncio.Lock()
_last_sent_at = 0.0

# Safety cap on how many "dead/unusable file" retries we do per request
# before giving up - keeps a single /getvid call bounded.
_MAX_DEAD_RETRIES = 5


@Client.on_message(filters.command("vidstatus") & filters.user(ADMINS))
async def vid_status(client, message):
    count = await gvdb.pool_count()
    await message.reply_text(
        f"📺 <b>/getvid status</b>\n\n"
        f"Indexed videos available: <code>{count}</code>\n\n"
        f"(This is every file tagged as a video across all channels you've "
        f"indexed with /index - no separate setup needed.)",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_message(filters.command("getvid") & filters.incoming)
async def get_random_video(client, message):
    global _last_sent_at

    user = message.from_user
    if not user:
        return await message.reply_text("Couldn't identify you, please try again from a normal account.")
    user_id = user.id

    pool = await gvdb.get_video_pool()
    if not pool:
        return await message.reply_text(
            "⚠️ No videos have been indexed yet. Index a channel with /index first "
            "(the same way you index files for searching) — every video found "
            "there becomes available to /getvid automatically."
        )

    # Let the requester know immediately if there's already a queue, so a
    # spam-tap doesn't look like the bot ignored them.
    ack = await message.reply_text("⏳ Queued…") if _send_lock.locked() else None

    async with _send_lock:
        # ── global throttle: guarantee >= GETVID_DELAY between sends ──────
        elapsed = time.monotonic() - _last_sent_at
        if elapsed < GETVID_DELAY:
            await asyncio.sleep(GETVID_DELAY - elapsed)

        # Re-check in case new videos got indexed while we were waiting.
        pool = await gvdb.get_video_pool()
        if not pool:
            _last_sent_at = time.monotonic()
            if ack:
                await ack.delete()
            return await message.reply_text("⚠️ No videos available right now.")

        order, pointer = await gvdb.get_or_init_progress(user_id, pool)

        sent = False
        dead_count = 0
        attempts = 0
        chosen_file_id = None

        while not sent and attempts < min(_MAX_DEAD_RETRIES, len(order)):
            if pointer >= len(order):
                # Finished a full cycle - reshuffle and start over.
                import random
                order = pool[:]
                random.shuffle(order)
                pointer = 0

            file_id = order[pointer]
            pointer += 1
            attempts += 1

            try:
                await client.send_cached_media(
                    chat_id=message.chat.id,
                    file_id=file_id,
                    reply_to_message_id=message.id,
                    protect_content=PROTECT_CONTENT,
                )
                chosen_file_id = file_id
                sent = True
            except FloodWait as e:
                await asyncio.sleep(e.value)
                try:
                    await client.send_cached_media(
                        chat_id=message.chat.id,
                        file_id=file_id,
                        reply_to_message_id=message.id,
                        protect_content=PROTECT_CONTENT,
                    )
                    chosen_file_id = file_id
                    sent = True
                except Exception as e2:
                    logger.warning(f"/getvid: file {file_id} failed after FloodWait retry: {e2}")
                    dead_count += 1
            except Exception as e:
                # File likely no longer exists / file_reference expired - skip it.
                logger.warning(f"/getvid: skipping unusable file {file_id}: {e}")
                dead_count += 1

        await gvdb.advance_pointer(user_id, pointer, order, pool_count=len(pool))
        _last_sent_at = time.monotonic()

        if ack:
            try:
                await ack.delete()
            except Exception:
                pass

        if not sent or chosen_file_id is None:
            return await message.reply_text(
                "⚠️ Couldn't fetch a working video right now, please try /getvid again."
            )
