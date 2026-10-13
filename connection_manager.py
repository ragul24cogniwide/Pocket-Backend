"""
Distributed WebSocket & Redis Pub/Sub Connection Manager.
Handles multi-node routing (Render / Redis Cloud), local socket pools, presence, typing indicators, and heartbeats.
"""

import asyncio
import json
import logging
import os
import ssl
import uuid
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
        self.node_id = str(uuid.uuid4())
        # Local active connections on this server node: {user_id: WebSocket}
        self.active_connections: dict[str, WebSocket] = {}
        # Background Redis Pub/Sub listener tasks: {user_id: asyncio.Task}
        self._pubsub_tasks: dict[str, asyncio.Task] = {}

    @staticmethod
    def _canonical_id(user_id: str) -> str:
        s = str(user_id).strip()
        digits = "".join(filter(str.isdigit, s))
        if len(digits) >= 10:
            return digits[-10:]
        return s

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

        # Clean up any existing pubsub task for this user before registering a new one
        if user_id in self._pubsub_tasks:
            try:
                self._pubsub_tasks[user_id].cancel()
            except Exception:
                pass
            self._pubsub_tasks.pop(user_id, None)

        if user_id in self.active_connections:
            old_ws = self.active_connections[user_id]
            if old_ws != websocket:
                try:
                    await old_ws.close()
                except Exception:
                    pass

        self.active_connections[user_id] = websocket

        # Record presence in Redis and local node
        if self.redis:
            try:
                await self.redis.sadd(REDIS_PRESENCE_KEY, user_id)
                # Spawn single personal Pub/Sub listener for distributed message routing
                task = asyncio.create_task(self._listen_user_channel(user_id))
                self._pubsub_tasks[user_id] = task
            except Exception as e:
                logger.error("Error setting presence in Redis: %s", e)

        # Broadcast online presence update to all peers
        await self.broadcast_presence(user_id, is_online=True)
        logger.info("User connected: %s (Active local sockets: %d)", user_id, len(self.active_connections))

        # Send current online users snapshot to this connecting user immediately
        try:
            online_users = list(self.active_connections.keys())
            await websocket.send_text(json.dumps({
                "type": "online_users_snapshot",
                "data": {
                    "online_user_ids": online_users
                }
            }))
        except Exception:
            pass

    async def disconnect(self, user_id: str) -> None:
        """Removes local connection and cleans up Pub/Sub subscription."""
        if user_id in self.active_connections:
            del self.active_connections[user_id]

        if user_id in self._pubsub_tasks:
            try:
                self._pubsub_tasks[user_id].cancel()
            except Exception:
                pass
            del self._pubsub_tasks[user_id]

        if self.redis:
            try:
                await self.redis.srem(REDIS_PRESENCE_KEY, user_id)
            except Exception as e:
                logger.error("Error clearing Redis presence for %s: %s", user_id, e)

        # Broadcast offline presence update with last seen timestamp
        now_iso = datetime.now(timezone.utc).isoformat()
        await self.broadcast_presence(user_id, is_online=False, last_seen=now_iso)
        logger.info("User disconnected: %s (Active local sockets: %d)", user_id, len(self.active_connections))

    # -------------------------------------------------------------------------
    # Messaging & Routing
    # -------------------------------------------------------------------------
    def _find_all_local_ws(self, user_id: str) -> list[WebSocket]:
        """Finds all active local WebSocket connections matching exact ID or phone digits."""
        matched: list[WebSocket] = []
        uid_str = str(user_id).strip()
        if uid_str in self.active_connections:
            matched.append(self.active_connections[uid_str])

        digits = "".join(filter(str.isdigit, uid_str))
        if len(digits) >= 8:
            for conn_id, ws in list(self.active_connections.items()):
                if ws in matched:
                    continue
                conn_digits = "".join(filter(str.isdigit, str(conn_id)))
                if conn_digits and (
                    digits.endswith(conn_digits)
                    or conn_digits.endswith(digits)
                    or (len(digits) >= 10 and len(conn_digits) >= 10 and digits[-10:] == conn_digits[-10:])
                ):
                    matched.append(ws)
        return matched

    def _find_local_ws(self, user_id: str) -> Optional[WebSocket]:
        """Finds a primary local WebSocket connection for user_id."""
        sockets = self._find_all_local_ws(user_id)
        return sockets[0] if sockets else None

    async def send_personal_message(self, user_id: str, message_payload: dict[str, Any]) -> bool:
        """
        Routes a payload directly to the user if on this local node,
        and publishes to user's canonical Redis channel for other cluster nodes.
        Avoids duplicate local delivery.
        """
        delivered = False
        payload_str = json.dumps(message_payload)

        # 1. Local delivery check (fan-out to all matching active sockets)
        target_sockets = self._find_all_local_ws(user_id)
        if target_sockets:
            for ws in list(target_sockets):
                try:
                    await ws.send_text(payload_str)
                    delivered = True
                except Exception as e:
                    logger.warning("Local send failed for socket (%s): %s", user_id, e)
                    # Clean up dead socket reference
                    for k, v in list(self.active_connections.items()):
                        if v == ws:
                            self.active_connections.pop(k, None)

        # 2. Distributed Redis Pub/Sub routing (publish ONLY once to canonical channel)
        if self.redis:
            try:
                ch = f"user:{self._canonical_id(user_id)}"
                envelope = json.dumps({
                    "origin_node": self.node_id,
                    "payload": message_payload,
                })
                subscribers = await self.redis.publish(ch, envelope)
                if subscribers > 0:
                    delivered = True
            except Exception as e:
                logger.error("Redis publish error for %s: %s", user_id, e)

        return delivered

    async def _listen_user_channel(self, user_id: str) -> None:
        """Background task that reads messages published to `user:<canonical_user_id>` from peer nodes."""
        if not self.redis:
            return

        pubsub = self.redis.pubsub()
        channel = f"user:{self._canonical_id(user_id)}"
        await pubsub.subscribe(channel)

        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    raw_data = message["data"]
                    try:
                        parsed = json.loads(raw_data)
                        # Avoid duplicate delivery if this node already sent to local sockets
                        if isinstance(parsed, dict) and parsed.get("origin_node") == self.node_id:
                            continue
                        payload_data = parsed.get("payload", parsed) if isinstance(parsed, dict) else parsed
                        payload_str = json.dumps(payload_data)
                    except Exception:
                        payload_str = raw_data

                    target_sockets = self._find_all_local_ws(user_id)
                    for ws in target_sockets:
                        try:
                            await ws.send_text(payload_str)
                        except Exception:
                            pass
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error("PubSub error for user %s: %s", user_id, e)
        finally:
            try:
                await pubsub.unsubscribe(channel)
                await pubsub.aclose()
            except Exception:
                pass

    # -------------------------------------------------------------------------
    # Presence & Broadcasts
    # -------------------------------------------------------------------------
    async def is_user_online(self, user_id: str, phone_number: Optional[str] = None) -> bool:
        """Checks if a user is currently online on any node with phone digit matching."""
        if not user_id and not phone_number:
            return False
        if user_id and self._find_local_ws(user_id):
            return True
        if phone_number and self._find_local_ws(phone_number):
            return True

        # Check digit matching against all local active connections
        all_checks = [str(user_id) if user_id else "", str(phone_number) if phone_number else ""]
        for chk in all_checks:
            if not chk:
                continue
            digits = "".join(filter(str.isdigit, chk))
            if len(digits) >= 8:
                for conn_id in self.active_connections.keys():
                    c_digits = "".join(filter(str.isdigit, str(conn_id)))
                    if c_digits and (
                        digits.endswith(c_digits)
                        or c_digits.endswith(digits)
                        or (len(digits) >= 10 and len(c_digits) >= 10 and digits[-10:] == c_digits[-10:])
                    ):
                        return True

        if self.redis:
            try:
                for chk in all_checks:
                    if chk and await self.redis.sismember(REDIS_PRESENCE_KEY, chk):
                        return True
                members = await self.redis.smembers(REDIS_PRESENCE_KEY)
                for chk in all_checks:
                    if not chk:
                        continue
                    digits = "".join(filter(str.isdigit, chk))
                    if len(digits) >= 8:
                        for m in members:
                            m_digits = "".join(filter(str.isdigit, str(m)))
                            if m_digits and (
                                digits.endswith(m_digits)
                                or m_digits.endswith(digits)
                                or (len(digits) >= 10 and len(m_digits) >= 10 and digits[-10:] == m_digits[-10:])
                            ):
                                return True
            except Exception:
                pass
        return False

    async def broadcast_presence(self, user_id: str, is_online: bool, last_seen: Optional[str] = None) -> None:
        """Notifies all connected clients about user presence change."""
        now_iso = datetime.now(timezone.utc).isoformat()
        frame = {
            "type": "presence_update",
            "data": {
                "user_id": user_id,
                "is_online": is_online,
                "last_seen": last_seen or (now_iso if not is_online else None),
                "timestamp": now_iso,
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
