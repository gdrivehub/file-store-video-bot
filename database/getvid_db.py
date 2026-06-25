"""
Database layer for the /getvid feature.

Design goals
------------
1. FAST per-command response: a /getvid call must never re-scan the channel.
   The channel's video message-ids are indexed ONCE (via /indexvidchannel,
   or lazily on first use) into `video_pool`, and every later /getvid only
   does O(1) document reads/updates.
2. NO REPEATS until the whole pool has been shown to that user, even across
   thousands of videos, and even across bot restarts (progress is persisted).
3. Self-contained: uses its own Motor client/collections so this feature can
   never interfere with the existing Media/users/connections collections.
"""

import logging
import random
import datetime

from motor.motor_asyncio import AsyncIOMotorClient
from info import DATABASE_URI, DATABASE_NAME

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_client = AsyncIOMotorClient(DATABASE_URI)
_db = _client[DATABASE_NAME]

# One document per source channel: {_id: channel_id, message_ids: [...], updated_at}
video_pool = _db.getvid_pool
# One document per user: {_id: user_id, channel_id, order: [...], pointer, updated_at}
video_progress = _db.getvid_progress
# Singleton doc holding the admin-configured active channel: {_id: "active", channel_id}
video_settings = _db.getvid_settings

_SETTINGS_ID = "active"


async def ensure_indexes():
    """Create indexes once at startup - idempotent, safe to call every boot."""
    try:
        await video_pool.create_index("_id")
        await video_progress.create_index("_id")
    except Exception as e:
        logger.warning(f"getvid_db index creation warning (non-fatal): {e}")


# ───────────────────────────── Channel setting ──────────────────────────────

async def set_active_channel(channel_id):
    """Persist the admin-chosen channel so it survives restarts/redeploys."""
    await video_settings.update_one(
        {"_id": _SETTINGS_ID},
        {"$set": {"channel_id": channel_id, "updated_at": datetime.datetime.utcnow()}},
        upsert=True,
    )


async def get_active_channel():
    """Return the runtime-configured channel id/username, or None."""
    doc = await video_settings.find_one({"_id": _SETTINGS_ID})
    if doc and doc.get("channel_id"):
        return doc["channel_id"]
    return None


# ───────────────────────────────── Video pool ────────────────────────────────

async def save_pool(channel_id, message_ids):
    """Overwrite the indexed video-message-id pool for a channel."""
    await video_pool.update_one(
        {"_id": str(channel_id)},
        {"$set": {
            "message_ids": message_ids,
            "count": len(message_ids),
            "updated_at": datetime.datetime.utcnow(),
        }},
        upsert=True,
    )


async def get_pool(channel_id):
    """Return the list of indexed video message_ids for a channel (or [])."""
    doc = await video_pool.find_one({"_id": str(channel_id)})
    if doc:
        return doc.get("message_ids", [])
    return []


async def pool_count(channel_id):
    doc = await video_pool.find_one({"_id": str(channel_id)}, {"count": 1})
    return doc.get("count", 0) if doc else 0


async def remove_dead_ids(channel_id, dead_ids):
    """Lazily drop message_ids that turned out to be deleted/invalid."""
    if not dead_ids:
        return
    dead_set = set(dead_ids)
    doc = await video_pool.find_one({"_id": str(channel_id)})
    if not doc:
        return
    cleaned = [m for m in doc.get("message_ids", []) if m not in dead_set]
    await video_pool.update_one(
        {"_id": str(channel_id)},
        {"$set": {"message_ids": cleaned, "count": len(cleaned)}},
    )


# ─────────────────────────────── User progress ──────────────────────────────

async def get_or_init_progress(user_id, channel_id, pool):
    """
    Return (order, pointer) for this user on this channel.
    If the user has no progress yet, or the channel changed, or the pool
    size changed (re-indexed), create a freshly shuffled order so every
    user gets their own independent non-repeating random sequence.
    """
    doc = await video_progress.find_one({"_id": str(user_id)})
    if (
        doc
        and doc.get("channel_id") == str(channel_id)
        and set(doc.get("order", [])) == set(pool)
        and doc.get("order")
    ):
        return doc["order"], doc.get("pointer", 0)

    order = pool[:]
    random.shuffle(order)
    await video_progress.update_one(
        {"_id": str(user_id)},
        {"$set": {
            "channel_id": str(channel_id),
            "order": order,
            "pointer": 0,
            "updated_at": datetime.datetime.utcnow(),
        }},
        upsert=True,
    )
    return order, 0


async def advance_pointer(user_id, new_pointer, order=None):
    """Persist the new pointer (and optionally a freshly-reshuffled order)."""
    update = {"pointer": new_pointer, "updated_at": datetime.datetime.utcnow()}
    if order is not None:
        update["order"] = order
    await video_progress.update_one({"_id": str(user_id)}, {"$set": update})


async def reset_progress(user_id):
    await video_progress.delete_one({"_id": str(user_id)})
