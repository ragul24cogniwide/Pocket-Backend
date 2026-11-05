"""
Shared counters for rate limits and attempt limits.

With Redis (the normal case) the counts are shared by every server process and updated
atomically, so parallel requests can't slip past a limit. Without Redis (local development,
or Redis briefly unreachable) they fall back to this process's memory, which is still atomic
within one process because nothing awaits between reading and updating a count.

Every counter is a fixed window: it starts at the first hit and expires `window_seconds` later.
"""

import logging
import time
from typing import Optional

from connection_manager import manager

logger = logging.getLogger("RateLimits")

KEY_PREFIX = "rl:"
_MEMORY_MAX_KEYS = 20_000

# key -> [expires_at (monotonic), count]
_memory: dict[str, list] = {}


def _memory_incr(key: str, window_seconds: int) -> tuple[int, int]:
    now = time.monotonic()
    if len(_memory) > _MEMORY_MAX_KEYS:
        for k, (exp, _) in list(_memory.items()):
            if exp <= now:
                del _memory[k]
    entry = _memory.get(key)
    if entry is None or entry[0] <= now:
        entry = [now + window_seconds, 0]
        _memory[key] = entry
    entry[1] += 1
    return entry[1], max(1, int(entry[0] - now))


async def incr_counter(key: str, window_seconds: int) -> tuple[int, int]:
    """
    Adds one to the counter `key` and returns (count including this hit, seconds until it resets).
    """
    redis = manager.redis
    if redis is not None:
        full_key = KEY_PREFIX + key
        try:
            # MULTI/EXEC: create the key with its expiry if missing, then count. A key can never
            # be left without an expiry (which would block someone for good), and this works on
            # every Redis version (EXPIRE ... NX needs Redis 7).
            pipe = redis.pipeline(transaction=True)
            pipe.set(full_key, 0, ex=window_seconds, nx=True)
            pipe.incr(full_key)
            pipe.ttl(full_key)
            _, count, ttl = await pipe.execute()
            return int(count), int(ttl) if ttl and int(ttl) > 0 else window_seconds
        except Exception as e:
            logger.warning("Redis counter %s failed (%s); using in-memory fallback", key, e)
    return _memory_incr(key, window_seconds)


async def reset_counter(key: str) -> None:
    """Forgets the counter `key` (e.g. after a successful login)."""
    _memory.pop(key, None)
    redis = manager.redis
    if redis is not None:
        try:
            await redis.delete(KEY_PREFIX + key)
        except Exception as e:
            logger.warning("Redis counter reset %s failed: %s", key, e)


async def hit(key: str, limit: int, window_seconds: int) -> Optional[int]:
    """
    Counts one request against `limit` per `window_seconds`.
    Returns None when allowed, or the number of seconds to wait when over the limit.
    """
    count, retry_after = await incr_counter(key, window_seconds)
    return None if count <= limit else retry_after


def wait_text(seconds: int) -> str:
    """'45 seconds' / '12 minutes' / '1 hour' for 429 messages."""
    if seconds < 90:
        return f"{max(1, seconds)} seconds"
    if seconds < 90 * 60:
        return f"{round(seconds / 60)} minutes"
    hours = round(seconds / 3600)
    return "1 hour" if hours <= 1 else f"{hours} hours"
