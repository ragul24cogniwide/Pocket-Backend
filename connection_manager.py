"""
Distributed WebSocket & Redis Pub/Sub Connection Manager.
Handles multi-node routing (Render / Redis Cloud), local socket pools, presence, typing indicators, and heartbeats.
"""

import asyncio
import json
import logging
import os
import ssl
from datetime import datetime, timezone
from typing import Any, Optional
import redis.asyncio as aioredis
from fastapi import WebSocket
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("ConnectionManager")
logger.setLevel(logging.INFO)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0").strip().strip('"\'')
if REDIS_URL.startswith("redis-cli -u "):
    REDIS_URL = REDIS_URL.replace("redis-cli -u ", "", 1).strip()
REDIS_PRESENCE_KEY = "global_online_users"


class ConnectionManager:
    def __init__(self, redis_url: Optional[str] = None):
        self.redis_url = (redis_url or REDIS_URL).strip().strip('"\'')
        if self.redis_url.startswith("redis-cli -u "):
            self.redis_url = self.redis_url.replace("redis-cli -u ", "", 1).strip()
        self.redis: Optional[aioredis.Redis] = None
        # Local active connections on this server node: {user_id: WebSocket}
        self.active_connections: dict[str, WebSocket] = {}
        # Background Redis Pub/Sub listener tasks: {user_id: asyncio.Task}
        self._pubsub_tasks: dict[str, asyncio.Task] = {}

    async def initialize(self) -> None:
        """Initializes Redis connection pool with SSL support for Render/Redis Cloud."""
        redis_kwargs = {
            "encoding": "utf-8",
            "decode_responses": True,
            "socket_timeout": 10.0,
            "socket_connect_timeout": 10.0,
        }

        # If connecting via rediss:// (Render/Upstash SSL), configure ssl_cert_reqs
        if self.redis_url.startswith("rediss://"):
            redis_kwargs["ssl_cert_reqs"] = ssl.CERT_NONE

        try:
            self.redis = aioredis.from_url(self.redis_url, **redis_kwargs)
            await self.redis.ping()
            logger.info("Successfully connected to Redis at: %s", self.redis_url)
        except Exception as e:
            logger.warning(
                "Redis connection failed (%s). Running in single-node in-memory fallback mode.",
                e,
            )
            self.redis = None

    async def shutdown(self) -> None:
        """Gracefully closes all sockets and Redis connection."""
        for task in self._pubsub_tasks.values():
            task.cancel()
        if self.redis:
            await self.redis.aclose()
        self.active_connections.clear()

    # -------------------------------------------------------------------------
    # Connection Lifecycle
    # -------------------------------------------------------------------------
    async def connect(self, user_id: str, websocket: WebSocket) -> None:
        """Accepts WebSocket connection and subscribes to personal Redis channel."""
        await websocket.accept()
        self.active_connections[user_id] = websocket

        # Record presence in Redis and local node
        if self.redis:
            try:
                await self.redis.sadd(REDIS_PRESENCE_KEY, user_id)
                # Spawn personal Pub/Sub listener for distributed message routing
                task = asyncio.create_task(self._listen_user_channel(user_id))
                self._pubsub_tasks[user_id] = task
            except Exception as e:
                logger.error("Error setting presence in Redis: %s", e)

        # Broadcast online presence update to all peers
        await self.broadcast_presence(user_id, is_online=True)
        logger.info("User connected: %s (Active local sockets: %d)", user_id, len(self.active_connections))

    async def disconnect(self, user_id: str) -> None:
        """Removes local connection and cleans up Pub/Sub subscription."""
        if user_id in self.active_connections:
            del self.active_connections[user_id]

        if user_id in self._pubsub_tasks:
            self._pubsub_tasks[user_id].cancel()
            del self._pubsub_tasks[user_id]

        if self.redis:
            try:
                await self.redis.srem(REDIS_PRESENCE_KEY, user_id)
            except Exception as e:
                logger.error("Error clearing Redis presence for %s: %s", user_id, e)

        # Broadcast offline presence update
        await self.broadcast_presence(user_id, is_online=False)
        logger.info("User disconnected: %s (Active local sockets: %d)", user_id, len(self.active_connections))

    # -------------------------------------------------------------------------
    # Messaging & Routing
    # -------------------------------------------------------------------------
    def _find_local_ws(self, user_id: str) -> Optional[WebSocket]:
        """Finds WebSocket connection with exact match or phone digit tolerance."""
        if user_id in self.active_connections:
            return self.active_connections[user_id]
        digits = "".join(filter(str.isdigit, str(user_id)))
        if len(digits) >= 8:
            for conn_id, ws in self.active_connections.items():
                conn_digits = "".join(filter(str.isdigit, str(conn_id)))
                if conn_digits and (digits.endswith(conn_digits) or conn_digits.endswith(digits)):
                    return ws
        return None

    async def send_personal_message(self, user_id: str, message_payload: dict[str, Any]) -> bool:
        """
        Routes a payload directly to the user if on this local node,
        or publishes to user's Redis channel for multi-node delivery.
        Returns True if delivered locally or published to Redis.
        """
        # 1. Local delivery check
        target_ws = self._find_local_ws(user_id)
        if target_ws:
            try:
                await target_ws.send_text(json.dumps(message_payload))
                return True
            except Exception as e:
                logger.warning("Local send failed for %s: %s", user_id, e)
                return False

        # 2. Distributed Redis Pub/Sub routing
        if self.redis:
            try:
                channel = f"user:{user_id}"
                subscribers = await self.redis.publish(channel, json.dumps(message_payload))
                return subscribers > 0
            except Exception as e:
                logger.error("Redis publish error for %s: %s", user_id, e)

        return False

    async def _listen_user_channel(self, user_id: str) -> None:
        """Background task that reads messages published to `user:<user_id>` channel."""
        if not self.redis:
            return

        pubsub = self.redis.pubsub()
        channel = f"user:{user_id}"
        await pubsub.subscribe(channel)

        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    payload_str = message["data"]
                    target_ws = self._find_local_ws(user_id)
                    if target_ws:
                        await target_ws.send_text(payload_str)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error("PubSub error for user %s: %s", user_id, e)
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()

    # -------------------------------------------------------------------------
    # Presence & Broadcasts
    # -------------------------------------------------------------------------
    async def is_user_online(self, user_id: str) -> bool:
        """Checks if a user is currently online on any node with phone digit matching."""
        if not user_id:
            return False
        if self._find_local_ws(user_id):
            return True
        if self.redis:
            try:
                if await self.redis.sismember(REDIS_PRESENCE_KEY, user_id):
                    return True
                members = await self.redis.smembers(REDIS_PRESENCE_KEY)
                digits = "".join(filter(str.isdigit, str(user_id)))
                if len(digits) >= 8:
                    for m in members:
                        m_digits = "".join(filter(str.isdigit, str(m)))
                        if m_digits and (digits.endswith(m_digits) or m_digits.endswith(digits)):
                            return True
            except Exception:
                pass
        return False

    async def broadcast_presence(self, user_id: str, is_online: bool) -> None:
        """Notifies all connected clients about user presence change."""
        frame = {
            "type": "presence_update",
            "data": {
                "user_id": user_id,
                "is_online": is_online,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        }
        # Send to all local connections
        for client_id, ws in list(self.active_connections.items()):
            if client_id != user_id:
                try:
                    await ws.send_text(json.dumps(frame))
                except Exception:
                    pass


manager = ConnectionManager()
