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
REDIS_PRESENCE_CHANNEL = "presence:broadcast"


class ConnectionManager:
    def __init__(self, redis_url: Optional[str] = None):
        self.redis_url = (redis_url or REDIS_URL).strip().strip('"\'')
        if self.redis_url.startswith("redis-cli -u "):
            self.redis_url = self.redis_url.replace("redis-cli -u ", "", 1).strip()
        self.redis: Optional[aioredis.Redis] = None
        self.node_id = str(uuid.uuid4())
        # Local active connections on this server node: {user_id: WebSocket}
        self.active_connections: dict[str, WebSocket] = {}
        # Users (and their aliases) who chose "Last seen: Nobody"; presence frames omit their timestamp
        self.last_seen_hidden: set[str] = set()
        # Background Redis Pub/Sub listener tasks: {user_id: asyncio.Task}
        self._pubsub_tasks: dict[str, asyncio.Task] = {}
        self._presence_listener_task: Optional[asyncio.Task] = None
        # alias (phone / last-10 digits) -> primary socket user_id
        self.id_aliases: dict[str, str] = {}
        self.user_aliases: dict[str, set[str]] = {}

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
            self._presence_listener_task = asyncio.create_task(self._listen_presence_channel())
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
        if self._presence_listener_task:
            self._presence_listener_task.cancel()
        if self.redis:
            await self.redis.aclose()
        self.active_connections.clear()
        self.id_aliases.clear()
        self.user_aliases.clear()

    def register_aliases(self, primary_id: str, aliases: Optional[list[str]] = None) -> list[str]:
        """Maps phone numbers / digit forms to the connected socket id."""
        primary = str(primary_id).strip()
        collected = self.user_aliases.setdefault(primary, set())
        collected.add(primary)
        canon = self._canonical_id(primary)
        if canon:
            collected.add(canon)
        for alias in aliases or []:
            if not alias:
                continue
            value = str(alias).strip()
            if not value:
                continue
            collected.add(value)
            extra = self._canonical_id(value)
            if extra:
                collected.add(extra)
        for alias in list(collected):
            self.id_aliases[alias] = primary
        return list(collected)

    def _aliases_for(self, user_id: str) -> list[str]:
        primary = self.id_aliases.get(str(user_id), str(user_id))
        aliases = set(self.user_aliases.get(primary, {str(user_id), self._canonical_id(user_id)}))
        aliases.add(str(user_id))
        return [a for a in aliases if a]

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
        aliases = self.register_aliases(user_id, [user_id])

        # Record presence in Redis and local node
        if self.redis:
            try:
                if aliases:
                    await self.redis.sadd(REDIS_PRESENCE_KEY, *aliases)
                else:
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
            online_users = await self.get_online_user_ids()
            await websocket.send_text(json.dumps({
                "type": "online_users_snapshot",
                "data": {
                    "online_user_ids": online_users
                }
            }))
        except Exception:
            pass

    async def disconnect(self, user_id: str, websocket: Optional[WebSocket] = None) -> None:
        """Removes local connection and cleans up Pub/Sub subscription.

        Ignores stale closes after a reconnect so the user is not marked offline
        while a newer socket is already live.
        """
        current = self.active_connections.get(user_id)
        if websocket is not None and current is not None and current is not websocket:
            logger.info("Ignoring stale disconnect for reconnecting user %s", user_id)
            return

        if user_id in self.active_connections:
            del self.active_connections[user_id]

        if user_id in self._pubsub_tasks:
            try:
                self._pubsub_tasks[user_id].cancel()
            except Exception:
                pass
            del self._pubsub_tasks[user_id]

        aliases = self._aliases_for(user_id)
        self.user_aliases.pop(user_id, None)
        for alias in aliases:
            if self.id_aliases.get(alias) == user_id:
                self.id_aliases.pop(alias, None)

        if self.redis:
            try:
                if aliases:
                    await self.redis.srem(REDIS_PRESENCE_KEY, *aliases)
            except Exception as e:
                logger.error("Error clearing Redis presence for %s: %s", user_id, e)

        # Broadcast offline presence update with last seen timestamp
        now_iso = datetime.now(timezone.utc).isoformat()
        await self.broadcast_presence(user_id, is_online=False, last_seen=now_iso, aliases=aliases)
        logger.info("User disconnected: %s (Active local sockets: %d)", user_id, len(self.active_connections))

    # -------------------------------------------------------------------------
    # Messaging & Routing
    # -------------------------------------------------------------------------
    def _find_all_local_ws(self, user_id: str) -> list[WebSocket]:
        """Finds all active local WebSocket connections matching exact ID or phone digits."""
        matched: list[WebSocket] = []
        uid_str = str(user_id).strip()
        mapped = self.id_aliases.get(uid_str)
        for key in (uid_str, mapped, self._canonical_id(uid_str)):
            if key and key in self.active_connections:
                ws = self.active_connections[key]
                if ws not in matched:
                    matched.append(ws)

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

    def _remove_dead_socket(self, ws: WebSocket, reason: str = "") -> None:
        """Removes a dead/stale WebSocket from active_connections and alias mappings."""
        dead_keys = []
        for k, v in list(self.active_connections.items()):
            if v == ws:
                self.active_connections.pop(k, None)
                dead_keys.append(k)
        if dead_keys:
            logger.info("Cleaned up dead socket for %s (reason: %s)", dead_keys, reason or "send_failure")

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
                    self._remove_dead_socket(ws, f"send_personal_message failure: {e}")

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
                        except Exception as e:
                            logger.warning("PubSub local delivery failed for %s: %s", user_id, e)
                            self._remove_dead_socket(ws, f"pubsub delivery failure: {e}")
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
    async def get_online_user_ids(self) -> list[str]:
        """Returns all currently online ids (local sockets + Redis presence)."""
        ids: set[str] = set(self.active_connections.keys())
        for alias_set in self.user_aliases.values():
            ids.update(alias_set)
        if self.redis:
            try:
                members = await self.redis.smembers(REDIS_PRESENCE_KEY)
                ids.update(str(m) for m in members)
            except Exception:
                pass
        return list(ids)

    async def refresh_presence(self, user_id: str) -> None:
        """Re-asserts Redis membership while a socket is still alive."""
        if not self.redis or not user_id:
            return
        aliases = self._aliases_for(user_id)
        try:
            if aliases:
                await self.redis.sadd(REDIS_PRESENCE_KEY, *aliases)
        except Exception:
            pass

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

    async def _fanout_presence_local(self, frame: dict[str, Any], skip_user_id: Optional[str] = None) -> None:
        payload = json.dumps(frame)
        skip_ids = set()
        if skip_user_id:
            skip_ids.update(self._aliases_for(skip_user_id))
            skip_ids.add(str(skip_user_id))
        for client_id, ws in list(self.active_connections.items()):
            if str(client_id) in skip_ids:
                continue
            try:
                await ws.send_text(payload)
            except Exception as e:
                self._remove_dead_socket(ws, f"presence fanout failure: {e}")

    async def _listen_presence_channel(self) -> None:
        """Forwards presence events published by other cluster nodes."""
        if not self.redis:
            return
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(REDIS_PRESENCE_CHANNEL)
        try:
            async for message in pubsub.listen():
                if message.get("type") != "message":
                    continue
                try:
                    parsed = json.loads(message["data"])
                    if parsed.get("origin_node") == self.node_id:
                        continue
                    frame = parsed.get("payload") or parsed
                    skip_id = (frame.get("data") or {}).get("user_id")
                    await self._fanout_presence_local(frame, skip_user_id=skip_id)
                except Exception:
                    continue
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error("Presence pubsub error: %s", e)
        finally:
            try:
                await pubsub.unsubscribe(REDIS_PRESENCE_CHANNEL)
                await pubsub.aclose()
            except Exception:
                pass

    async def broadcast_presence(
        self,
        user_id: str,
        is_online: bool,
        last_seen: Optional[str] = None,
        aliases: Optional[list[str]] = None,
    ) -> None:
        """Notifies all connected clients about user presence change."""
        now_iso = datetime.now(timezone.utc).isoformat()
        alias_list = aliases or self._aliases_for(user_id)
        hide_last_seen = user_id in self.last_seen_hidden or any(a in self.last_seen_hidden for a in alias_list or [])
        frame = {
            "type": "presence_update",
            "data": {
                "user_id": user_id,
                "aliases": alias_list,
                "is_online": is_online,
                "last_seen": None if hide_last_seen else (last_seen or (now_iso if not is_online else None)),
                "timestamp": now_iso,
            },
        }
        await self._fanout_presence_local(frame, skip_user_id=user_id)
        if self.redis:
            try:
                await self.redis.publish(
                    REDIS_PRESENCE_CHANNEL,
                    json.dumps({"origin_node": self.node_id, "payload": frame}),
                )
            except Exception as e:
                logger.error("Presence publish error: %s", e)


manager = ConnectionManager()
