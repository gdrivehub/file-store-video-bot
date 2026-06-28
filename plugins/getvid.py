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

Per-video protections (new)
----------------------------
* GETVID_PROTECT_CONTENT - Telegram's "Protected Content": viewers can only
  view the video, they can't forward/save/download it.
* GETVID_SPOILER - the video is blurred behind a tap-to-reveal spoiler
  overlay until the recipient taps it.
* GETVID_AUTO_DELETE_SECONDS - the sent video auto-deletes itself this many
  seconds after being sent (default 1200s = 20 minutes).
* Every video comes with a "🎬 Click for more video" button - tapping it
  instantly sends another random video to whoever tapped it (same anti-spam
  throttle and same no-repeat-until-exhausted logic as /getvid itself), so
  users can keep browsing without retyping the command.

Anti-spam / performance design
-------------------------------
* A single process-wide asyncio.Lock serializes all /getvid sends (command
  AND button clicks share the same lock). Inside the lock we check how long
  it's been since the last video was actually sent and asyncio.sleep() just
  enough to guarantee a >= GETVID_DELAY (default 0.4s) gap before sending
  the next one. This means: if a user (or many users) spams /getvid or the
  button, requests queue up FIFO and each one still gets a video - just
  throttled - instead of overloading Telegram/Mongo.
* The same lock makes the per-user "pick next id / advance pointer"
  read-modify-write race-free, with no extra locking machinery needed.
* The list of indexed video file_ids is cached and only refreshed when the
  count of indexed videos actually changes, so normal /getvid calls do
  practically no DB work beyond reading a cached list + a tiny user doc.
"""

import logging
import asyncio
import random
import time

from pyrogram import Client, filters, enums, raw
from pyrogram import utils as pyroutils
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message

from info import (
    ADMINS,
    GETVID_DELAY,
    GETVID_PROTECT_CONTENT,
    GETVID_SPOILER,
    GETVID_AUTO_DELETE_SECONDS,
)
from database import getvid_db as gvdb

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ── Global anti-spam gate (see module docstring) ────────────────────────────
_send_lock = asyncio.Lock()
_last_sent_at = 0.0

# Safety cap on how many "dead/unusable file" retries we do per request
# before giving up - keeps a single /getvid call bounded.
_MAX_DEAD_RETRIES = 5

MORE_BUTTON = InlineKeyboardMarkup(
    [[InlineKeyboardButton("🎬 Click for more video", callback_data="getvid_more")]]
)


async def _send_cached_video_with_spoiler(
    client,
    chat_id,
    file_id,
    reply_to_message_id=None,
    protect_content=None,
    has_spoiler=False,
    reply_markup=None,
):
    """
    This repo stores files using Pyrogram's internal packed FileId format
    (see database/ia_filterdb.py: pack_new_file_id/unpack_new_file_id) - the
    SAME format Pyrogram's own send_cached_media() decodes via
    utils.get_input_media_from_file_id(). Plain send_video()/send_document()
    treat the string as a literal upload reference instead and fail with
    MEDIA_EMPTY, since that's not what this kind of file_id is.

    So we replicate send_cached_media()'s exact internal logic here (same
    raw.functions.messages.SendMedia call), with one addition: we set
    `spoiler=True` on the decoded InputMediaDocument so videos can be sent
    blurred-until-tapped, which send_cached_media() itself doesn't expose.
    """
    media = pyroutils.get_input_media_from_file_id(file_id)
    if has_spoiler and isinstance(media, raw.types.InputMediaDocument):
        media.spoiler = True

    r = await client.invoke(
        raw.functions.messages.SendMedia(
            peer=await client.resolve_peer(chat_id),
            media=media,
            random_id=client.rnd_id(),
            reply_to_msg_id=reply_to_message_id,
            noforwards=protect_content,
            reply_markup=await reply_markup.write(client) if reply_markup else None,
        )
    )

    for i in r.updates:
        if isinstance(i, (raw.types.UpdateNewMessage, raw.types.UpdateNewChannelMessage, raw.types.UpdateNewScheduledMessage)):
            return await Message._parse(
                client, i.message,
                {u.id: u for u in r.users},
                {c.id: c for c in r.chats},
                is_scheduled=isinstance(i, raw.types.UpdateNewScheduledMessage),
            )
    return None


@Client.on_message(filters.command("vidstatus") & filters.user(ADMINS))
async def vid_status(client, message):
    count = await gvdb.pool_count()
    await message.reply_text(
        f"📺 <b>/getvid status</b>\n\n"
        f"Indexed videos available: <code>{count}</code>\n\n"
        f"Protected content: <code>{GETVID_PROTECT_CONTENT}</code>\n"
        f"Spoiler blur: <code>{GETVID_SPOILER}</code>\n"
        f"Auto-delete: <code>{GETVID_AUTO_DELETE_SECONDS}s</code>\n\n"
        f"(This is every file tagged as a video across all channels you've "
        f"indexed with /index - no separate setup needed.)",
        parse_mode=enums.ParseMode.HTML,
    )


async def _schedule_auto_delete(sent_message):
    if GETVID_AUTO_DELETE_SECONDS <= 0:
        return
    try:
        await asyncio.sleep(GETVID_AUTO_DELETE_SECONDS)
        await sent_message.delete()
    except Exception as e:
        # Message may already be gone, or bot may lack delete rights in a
        # group - either way this is non-critical, just log and move on.
        logger.debug(f"/getvid auto-delete skipped: {e}")


async def _deliver_one_video(client, chat_id, user_id, reply_to_message_id=None):
    """
    Core picker+sender, shared by /getvid and the 'Click for more video'
    button. MUST be called while holding _send_lock. Returns the sent
    Message, or None if nothing could be delivered.
    """
    pool = await gvdb.get_video_pool()
    if not pool:
        return None

    order, pointer = await gvdb.get_or_init_progress(user_id, pool)

    sent_message = None
    attempts = 0

    while sent_message is None and attempts < min(_MAX_DEAD_RETRIES, len(order)):
        if pointer >= len(order):
            # Finished a full cycle - reshuffle and start over.
            order = pool[:]
            random.shuffle(order)
            pointer = 0

        file_id = order[pointer]
        pointer += 1
        attempts += 1

        for retry in range(2):  # one shot + one retry after FloodWait
            try:
                sent_message = await _send_cached_video_with_spoiler(
                    client,
                    chat_id=chat_id,
                    file_id=file_id,
                    reply_to_message_id=reply_to_message_id,
                    protect_content=GETVID_PROTECT_CONTENT,
                    has_spoiler=GETVID_SPOILER,
                    reply_markup=MORE_BUTTON,
                )
                break
            except FloodWait as e:
                await asyncio.sleep(e.value)
                continue
            except Exception as e:
                # File likely no longer exists / file_reference expired - skip it.
                logger.warning(f"/getvid: skipping unusable file {file_id}: {e}")
                break

        if sent_message:
            break

    await gvdb.advance_pointer(user_id, pointer, order, pool_count=len(pool))

    if sent_message:
        asyncio.create_task(_schedule_auto_delete(sent_message))

    return sent_message


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
        elapsed = time.monotonic() - _last_sent_at
        if elapsed < GETVID_DELAY:
            await asyncio.sleep(GETVID_DELAY - elapsed)

        sent_message = await _deliver_one_video(
            client, message.chat.id, user_id, reply_to_message_id=message.id
        )
        _last_sent_at = time.monotonic()

    if ack:
        try:
            await ack.delete()
        except Exception:
            pass

    if not sent_message:
        return await message.reply_text(
            "⚠️ Couldn't fetch a working video right now, please try /getvid again."
        )


@Client.on_callback_query(filters.regex(r"^getvid_more$"))
async def get_random_video_button(client, query: CallbackQuery):
    global _last_sent_at

    user_id = query.from_user.id

    # Ack immediately so Telegram doesn't show a stuck loading spinner while
    # this request may be queued behind others in the throttle.
    if _send_lock.locked():
        await query.answer("⏳ Queued, one moment…")
    else:
        await query.answer()

    pool = await gvdb.get_video_pool()
    if not pool:
        return await client.send_message(
            query.message.chat.id,
            "⚠️ No videos available right now.",
        )

    async with _send_lock:
        elapsed = time.monotonic() - _last_sent_at
        if elapsed < GETVID_DELAY:
            await asyncio.sleep(GETVID_DELAY - elapsed)

        sent_message = await _deliver_one_video(client, query.message.chat.id, user_id)
        _last_sent_at = time.monotonic()

    if not sent_message:
        try:
            await client.send_message(
                query.message.chat.id,
                "⚠️ Couldn't fetch a working video right now, please try again.",
            )
        except Exception:
            pass
