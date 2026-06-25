"""
Database layer for the /getvid feature.

IMPORTANT DESIGN
----------------
This bot already has a fully working indexing system (`/index`, forwarded
post + confirmation flow) that scans channels and saves every file into the
shared `Media` collection (see database/ia_filterdb.py), each tagged with a
`file_type` of "video", "document", or "audio".

So /getvid does NOT maintain its own separate index/pool of channels or
message ids. It just reads from that SAME `Media` collection, filtering for
`file_type == "video"` — i.e. any file already indexed through the existing
system, from ANY channel that's been indexed, is automatically available to
/getvid the moment it's indexed. No extra admin setup commands needed.

Performance
-----------
* We don't want every single /getvid call to scan/count the whole `Media`
  collection. Instead we cache the full list of video file_ids in a tiny
  meta-document, and only refresh that cache when a cheap `count_documents`
  check shows the number of indexed videos has changed (i.e. new videos
  were indexed since we last cached). Normal /getvid calls do zero heavy
  Mongo work beyond reading that cached list + a user's tiny progress doc.
* Per-user "no repeat" order is a shuffled copy of that pool + a pointer,
  so picking the next video is an O(1) array lookup, persisted so it
  survives bot restarts. Once a user has seen every video, it reshuffles
  and a fresh cycle starts automatically.
"""

import logging
import random
import datetime

from motor.motor_asyncio import AsyncIOMotorClient
from info import DATABASE_URI, DATABASE_NAME, COLLECTION_NAME

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_client = AsyncIOMotorClient(DATABASE_URI)
_db = _client[DATABASE_NAME]

# Same physical collection the existing indexing system (Media) writes to.
# We only ever READ from it here — /getvid never inserts/modifies file docs.
_media_coll = _db[COLLECTION_NAME]

# Tiny cache doc: {_id: "video_pool", file_ids: [...], count, updated_at}
_pool_cache = _db.getvid_pool_cache
# One doc per user: {_id: user_id, order: [...], pointer, pool_count, updated_at}
_progress = _db.getvid_progress

_POOL_DOC_ID = "video_pool"


async def ensure_indexes():
    """Create indexes once at startup - idempotent, safe to call every boot."""
    try:
        # Speeds up both the count check and the full-list fetch below.
        await _media_coll.create_index("file_type")
        await _progress.create_index("_id")
    except Exception as e:
        logger.warning(f"getvid_db index creation warning (non-fatal): {e}")


async def get_video_pool(force_refresh=False):
    """
    Return the list of every indexed video's file_id.
    Cheap on the common path: one count_documents() call; only re-fetches
    the full list when the count has actually changed (new videos indexed)
    or when force_refresh is requested.
    """
    current_count = await _media_coll.count_documents({"file_type": "video"})

    if not force_refresh:
        cached = await _pool_cache.find_one({"_id": _POOL_DOC_ID})
        if cached and cached.get("count") == current_count and cached.get("file_ids"):
            return cached["file_ids"]

    file_ids = []
    cursor = _media_coll.find({"file_type": "video"}, {"_id": 1})
    async for doc in cursor:
        file_ids.append(doc["_id"])

    await _pool_cache.update_one(
        {"_id": _POOL_DOC_ID},
        {"$set": {
            "file_ids": file_ids,
            "count": len(file_ids),
            "updated_at": datetime.datetime.utcnow(),
        }},
        upsert=True,
    )
    return file_ids


async def pool_count():
    """Fast count without pulling the full id list (used by /vidstatus)."""
    return await _media_coll.count_documents({"file_type": "video"})


# ─────────────────────────────── User progress ──────────────────────────────

async def get_or_init_progress(user_id, pool):
    """
    Return (order, pointer) for this user.
    If the user has no progress yet, or the pool changed size (new videos
    got indexed since their last /getvid), start a freshly shuffled order
    so every user gets their own independent non-repeating sequence and
    new videos eventually get mixed in.
    """
    doc = await _progress.find_one({"_id": str(user_id)})
    if doc and doc.get("pool_count") == len(pool) and doc.get("order"):
        return doc["order"], doc.get("pointer", 0)

    order = pool[:]
    random.shuffle(order)
    await _progress.update_one(
        {"_id": str(user_id)},
        {"$set": {
            "order": order,
            "pointer": 0,
            "pool_count": len(pool),
            "updated_at": datetime.datetime.utcnow(),
        }},
        upsert=True,
    )
    return order, 0


async def advance_pointer(user_id, new_pointer, order=None, pool_count=None):
    """Persist the new pointer (and optionally a freshly-reshuffled order)."""
    update = {"pointer": new_pointer, "updated_at": datetime.datetime.utcnow()}
    if order is not None:
        update["order"] = order
    if pool_count is not None:
        update["pool_count"] = pool_count
    await _progress.update_one({"_id": str(user_id)}, {"$set": update})


async def reset_progress(user_id):
    await _progress.delete_one({"_id": str(user_id)})
