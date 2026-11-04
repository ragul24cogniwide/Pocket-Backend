"""
Distributed WebSocket & Redis Pub/Sub Connection Manager.
Handles multi-node routing (Render / Redis Cloud), local socket pools, presence, typing indicators, and heartbeats.

Redis use is kept small so a free Redis plan (about 30 connections) is enough for any number of users:
- One pub/sub connection per server carries the frames for every user ("user:*") and the presence
  channel. Before, every online user opened their own pub/sub connection, so delivery between
  servers stopped working at about 30 people online.
- Commands share a small, capped connection pool (REDIS_MAX_CONNECTIONS).
- Presence is one short-lived key per id ("online:<id>"), kept alive by the apps' pings. A server
  restart or crash can't leave people "online" for good, as the old global set could.
- A presence change goes only to the people who chat with that user (like WhatsApp), not to every
  connected socket.
"""

import asyncio
import json
import logging
import os
import ssl
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable, Optional
import redis.asyncio as aioredis
from fastapi import WebSocket
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("ConnectionManager")
logger.setLevel(logging.INFO)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0").strip().strip('"\'')
if REDIS_URL.startswith("redis-cli -u "):
    REDIS_URL = REDIS_URL.replace("redis-cli -u ", "", 1).strip()
REDIS_PRESENCE_KEY = "global_online_users"  # the old presence set: no longer used, removed on startup
REDIS_PRESENCE_CHANNEL = "presence:broadcast"
REDIS_USER_CHANNEL_PREFIX = "user:"
PRESENCE_KEY_PREFIX = "online:"
# The apps ping every 10 s. A presence key nobody refreshed for this long belongs to a device that's gone.
PRESENCE_TTL_SECONDS = int(os.getenv("PRESENCE_TTL_SECONDS", "90"))
# A user's presence keys are rewritten at most this often (pings come every 10 s)
PRESENCE_REFRESH_SECONDS = max(5, PRESENCE_TTL_SECONDS // 3)
# Command connections shared by the whole server; pub/sub uses one of them. Free plans allow about 30.
REDIS_MAX_CONNECTIONS = int(os.getenv("REDIS_MAX_CONNECTIONS", "8"))
# How long the pub/sub listener waits for a frame before checking again (keeps the connection healthy)
PUBSUB_POLL_SECONDS = 5.0
PHONE_DEVICE = "phone"  # key for the phone app in ConnectionManager.active_devices


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
        # Users whose app went to the background: shown offline at once, although their socket
        # stays open a few more seconds (calls, camera picker) before the app closes it
        self.away: set[str] = set()
        # Which of a user's devices are in use right now: PHONE_DEVICE (app in the foreground) and/or
        # the session ids of Flick Web tabs that are visible and being used. Online = at least one;
        # an open tab nobody is looking at doesn't count.
        self.active_devices: dict[str, set[str]] = {}
        # The one pub/sub listener of this server (see _listen_cluster)
        self._cluster_listener_task: Optional[asyncio.Task] = None
        # alias (phone / last-10 digits) -> primary socket user_id
        self.id_aliases: dict[str, str] = {}
        self.user_aliases: dict[str, set[str]] = {}
        # Linked browsers (Flick Web), next to the phone's socket: {user_id: {WebSocket: session_id}}
        self.linked: dict[str, dict[WebSocket, str]] = {}
        # alias (id / phone / last-10 digits) -> user_id, for users with a linked browser open
        self.linked_aliases: dict[str, str] = {}
        # Whose presence each connected user receives (the people they chat with), and the reverse:
        # {owner user_id: {ids}} and {id: {owner user_ids}}. Ids include phone and last-10 forms.
        self.presence_peers: dict[str, set[str]] = {}
        self.presence_watchers: dict[str, set[str]] = {}
        # When this server last wrote a user's presence keys, and for which ids: {user_id: (time, ids)}
        self._presence_written: dict[str, tuple[float, frozenset]] = {}

    @staticmethod
    def _canonical_id(user_id: str) -> str:
        s = str(user_id).strip()
        digits = "".join(filter(str.isdigit, s))
        if len(digits) >= 10:
            return digits[-10:]
        return s

    @staticmethod
    def _last10(value: str) -> str:
        digits = "".join(filter(str.isdigit, str(value or "")))
        return digits[-10:] if len(digits) >= 10 else ""

    def _id_forms(self, ids: Iterable[Optional[str]]) -> set[str]:
        """Every form an id can be written in here: as given, and its last 10 digits."""
        forms: set[str] = set()
        for value in ids:
            s = str(value).strip() if value else ""
            if not s:
                continue
            forms.add(s)
            last10 = self._last10(s)
            if last10:
                forms.add(last10)
        return forms

    async def initialize(self) -> None:
        """Connects to Redis with a capped connection pool (SSL for Render/Redis Cloud)."""
        redis_kwargs = {
            "encoding": "utf-8",
            "decode_responses": True,
            "socket_timeout": 10.0,
            "socket_connect_timeout": 10.0,
            "socket_keepalive": True,
            "health_check_interval": 30,
        }

        # If connecting via rediss:// (Render/Upstash SSL), configure ssl_cert_reqs
        if self.redis_url.startswith("rediss://"):
            redis_kwargs["ssl_cert_reqs"] = ssl.CERT_NONE

        try:
            # Blocking pool: when all connections are busy a command waits for one instead of
            # opening more than the Redis plan allows
            pool = aioredis.BlockingConnectionPool.from_url(
                self.redis_url, max_connections=REDIS_MAX_CONNECTIONS, timeout=10, **redis_kwargs
            )
            self.redis = aioredis.Redis.from_pool(pool)
            await self.redis.ping()
            logger.info(
                "Connected to Redis at %s (max %d connections)",
                self.redis_url.split("@")[-1],  # no password in logs
                REDIS_MAX_CONNECTIONS,
            )
            try:
                await self.redis.delete(REDIS_PRESENCE_KEY)  # stale entries of the old presence set
            except Exception:
                pass
            self._cluster_listener_task = asyncio.create_task(self._listen_cluster())
        except Exception as e:
            logger.warning(
                "Redis connection failed (%s). Running in single-node in-memory fallback mode.",
                e,
            )
            self.redis = None

    async def shutdown(self) -> None:
        """Gracefully closes all sockets and Redis connection."""
        if self._cluster_listener_task:
            self._cluster_listener_task.cancel()
        if self.redis:
            await self.redis.aclose()
        self.active_connections.clear()
        self.id_aliases.clear()
        self.user_aliases.clear()
        self.linked.clear()
        self.linked_aliases.clear()
        self.presence_peers.clear()
        self.presence_watchers.clear()
        self._presence_written.clear()

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
    # Presence keys in Redis (shared by all servers)
    # -------------------------------------------------------------------------
    @staticmethod
    def _presence_keys(ids: Iterable[Optional[str]]) -> list[str]:
        unique = dict.fromkeys(str(i).strip() for i in ids if i)
        return [f"{PRESENCE_KEY_PREFIX}{i}" for i in unique if i]

    async def _write_presence(self, user_id: str, aliases: Iterable[str]) -> None:
        """Marks the user online for PRESENCE_TTL_SECONDS under every alias."""
        alias_set = frozenset(a for a in aliases if a)
        self._presence_written[user_id] = (time.monotonic(), alias_set)
        if not self.redis or not alias_set:
            return
        try:
            pipe = self.redis.pipeline(transaction=False)
            for key in self._presence_keys(alias_set):
                pipe.set(key, self.node_id, ex=PRESENCE_TTL_SECONDS)
            await pipe.execute()
        except Exception as e:
            logger.error("Error writing presence for %s: %s", user_id, e)

    async def presence_remove(self, aliases: Iterable[Optional[str]], user_id: Optional[str] = None) -> None:
        """Marks these ids offline on every server."""
        if user_id:
            self._presence_written.pop(user_id, None)
        keys = self._presence_keys(aliases)
        if not self.redis or not keys:
            return
        try:
            await self.redis.delete(*keys)
        except Exception as e:
            logger.error("Error clearing presence: %s", e)

    async def _redis_online(self, ids: Iterable[str]) -> set[str]:
        """Which of these ids have a live presence key (set by any server)."""
        if not self.redis:
            return set()
        unique = [i for i in dict.fromkeys(str(x).strip() for x in ids if x) if i]
        online: set[str] = set()
        for start in range(0, len(unique), 500):
            part = unique[start:start + 500]
            values = await self.redis.mget([f"{PRESENCE_KEY_PREFIX}{i}" for i in part])
            online.update(i for i, v in zip(part, values) if v)
        return online

    # -------------------------------------------------------------------------
    # Who receives whose presence
    # -------------------------------------------------------------------------
    def _owner_of(self, user_id: str) -> str:
        uid = str(user_id).strip()
        if uid in self.active_connections or uid in self.linked:
            return uid
        return self.id_aliases.get(uid) or self._linked_owner(uid) or self.id_aliases.get(self._canonical_id(uid)) or uid

    def watch_presence(self, user_id: str, peer_ids: Iterable[Optional[str]]) -> None:
        """From now on `user_id` gets presence updates for these people (their chats, a chat they opened)."""
        owner = self._owner_of(user_id)
        if owner not in self.active_connections and owner not in self.linked:
            return  # not connected to this server: nothing to send them
        current = self.presence_peers.setdefault(owner, set())
        for form in self._id_forms(peer_ids) - current:
            self.presence_watchers.setdefault(form, set()).add(owner)
            current.add(form)

    def _forget_presence_peers(self, user_id: str) -> None:
        """The user has no socket left on this server: stop sending them presence."""
        for form in self.presence_peers.pop(user_id, set()):
            owners = self.presence_watchers.get(form)
            if owners is not None:
                owners.discard(user_id)
                if not owners:
                    self.presence_watchers.pop(form, None)

    # -------------------------------------------------------------------------
    # Connection Lifecycle
    # -------------------------------------------------------------------------
    async def connect(self, user_id: str, websocket: WebSocket) -> None:
        """Accepts the phone app's WebSocket connection and marks the user online."""
        await websocket.accept()

        if user_id in self.active_connections:
            old_ws = self.active_connections[user_id]
            if old_ws != websocket:
                try:
                    await old_ws.close()
                except Exception:
                    pass

        self.active_connections[user_id] = websocket
        aliases = self.register_aliases(user_id, [user_id])
        self.mark_active(user_id)
        self.set_device_active(user_id, PHONE_DEVICE, True)

        # Presence for every server (frames reach this server through the shared listener)
        await self._write_presence(user_id, aliases)

        # Tell the people who chat with this user
        await self.broadcast_presence(user_id, is_online=True)
        logger.info("User connected: %s (Active local sockets: %d)", user_id, len(self.active_connections))

        # (The "who's online" snapshot is sent by the gateway, limited to this user's contacts)

    async def disconnect(self, user_id: str, websocket: Optional[WebSocket] = None) -> None:
        """Removes the phone's local connection.

        Ignores stale closes after a reconnect so the user is not marked offline
        while a newer socket is already live.
        """
        current = self.active_connections.get(user_id)
        if websocket is not None and current is not None and current is not websocket:
            logger.info("Ignoring stale disconnect for reconnecting user %s", user_id)
            return

        if user_id in self.active_connections:
            del self.active_connections[user_id]

        aliases = self._aliases_for(user_id)
        self.user_aliases.pop(user_id, None)
        for alias in aliases:
            if self.id_aliases.get(alias) == user_id:
                self.id_aliases.pop(alias, None)
        still_in_use = self.set_device_active(user_id, PHONE_DEVICE, False)
        if self._linked_sockets(user_id):
            # Their browser is still open: online only if they're using it
            logger.info("Phone socket closed for %s; linked browser still connected", user_id)
            if not still_in_use:
                await self.show_idle(user_id, aliases)
            return
        self.away.difference_update(aliases or [user_id])
        self.away.discard(user_id)
        self._forget_presence_peers(user_id)
        await self.presence_remove(aliases, user_id=user_id)

        # Broadcast offline presence update with last seen timestamp
        now_iso = datetime.now(timezone.utc).isoformat()
        await self.broadcast_presence(user_id, is_online=False, last_seen=now_iso, aliases=aliases)
        logger.info("User disconnected: %s (Active local sockets: %d)", user_id, len(self.active_connections))

    # -------------------------------------------------------------------------
    # Linked browsers (Flick Web)
    # -------------------------------------------------------------------------
    def _linked_owner(self, user_id: str) -> Optional[str]:
        uid = str(user_id or "").strip()
        if not uid:
            return None
        if uid in self.linked:
            return uid
        for key in (uid, self._canonical_id(uid), self.id_aliases.get(uid)):
            owner = self.linked_aliases.get(key) if key else None
            if owner and owner in self.linked:
                return owner
        return None

    def _linked_sockets(self, user_id: str) -> list[WebSocket]:
        owner = self._linked_owner(user_id)
        return list(self.linked.get(owner, {}).keys()) if owner else []

    async def connect_linked(self, user_id: str, websocket: WebSocket, session_id: str, aliases: list[str]) -> None:
        """Registers a linked browser's socket. Unlike the phone's, it doesn't replace other sockets."""
        await websocket.accept()
        sockets = self.linked.setdefault(user_id, {})
        sockets[websocket] = session_id
        alias_set = set()
        for alias in [user_id, *aliases]:
            if alias:
                alias_set.update({str(alias), self._canonical_id(str(alias))})
        for alias in alias_set:
            self.linked_aliases[alias] = user_id
        # The tab reports right away whether it's in use (presence_online / presence_offline);
        # until then an open tab alone doesn't make anyone look online
        if not self.in_use(user_id):
            self.mark_away(user_id)
        else:
            await self._write_presence(user_id, alias_set | set(self._aliases_for(user_id)))
        logger.info("Linked browser connected for %s (%d open)", user_id, len(sockets))

    async def disconnect_linked(self, user_id: str, websocket: WebSocket) -> None:
        sockets = self.linked.get(user_id)
        if not sockets or websocket not in sockets:
            return
        session_id = sockets.pop(websocket, None)
        still_in_use = self.set_device_active(user_id, session_id, False) if session_id else self.in_use(user_id)
        if sockets:
            if not still_in_use:
                await self.show_idle(user_id)  # the tab they were using closed; the others are idle
            return
        self.linked.pop(user_id, None)
        aliases = [a for a, owner in self.linked_aliases.items() if owner == user_id]
        for alias in aliases:
            self.linked_aliases.pop(alias, None)
        if self.active_connections.get(user_id) is not None:
            if not still_in_use:
                await self.show_idle(user_id, aliases)  # the phone is connected but in the background
            return
        self.away.difference_update(aliases)
        self.away.discard(user_id)
        self._forget_presence_peers(user_id)
        await self.presence_remove(aliases, user_id=user_id)
        await self.broadcast_presence(
            user_id, is_online=False, last_seen=datetime.now(timezone.utc).isoformat(), aliases=aliases
        )

    async def close_linked_session(self, user_id: str, session_id: str, code: int = 4401) -> None:
        """Closes the open socket(s) of a browser that was logged out."""
        for ws, sid in list(self.linked.get(user_id, {}).items()):
            if sid == session_id:
                try:
                    await ws.close(code=code, reason="Logged out")
                except Exception:
                    pass
                await self.disconnect_linked(user_id, ws)

    async def send_to_my_devices(
        self, user_id: str, message_payload: dict[str, Any], exclude: Optional[WebSocket] = None
    ) -> None:
        """Sends a frame to the user's own other devices (phone and browsers), e.g. a message they
        just sent from one of them, so every device shows the same conversation."""
        payload_str = json.dumps(message_payload)
        for ws in self._find_all_local_ws(user_id):
            if ws is exclude:
                continue
            try:
                await ws.send_text(payload_str)
            except Exception as e:
                self._remove_dead_socket(ws, f"own-device sync failure: {e}")
        if message_payload.get("type") == "message_sent_sync":
            # They wrote to someone (maybe a new chat): they see that person's online status from now on
            receiver = (message_payload.get("data") or {}).get("receiver_id")
            if receiver:
                self.watch_presence(user_id, [receiver])
        await self._publish_to_user(user_id, message_payload)

    # -------------------------------------------------------------------------
    # Messaging & Routing
    # -------------------------------------------------------------------------
    def _find_all_local_ws(self, user_id: str, include_linked: bool = True) -> list[WebSocket]:
        """Finds all active local WebSocket connections matching exact ID or phone digits.

        Ids of 10+ digits are looked up through the alias index (every connection registers its
        last 10 digits), so this doesn't go through every open connection on each message.
        """
        matched: list[WebSocket] = []
        uid_str = str(user_id).strip()
        canon = self._canonical_id(uid_str)
        for key in (uid_str, self.id_aliases.get(uid_str), canon, self.id_aliases.get(canon)):
            ws = self.active_connections.get(key) if key else None
            if ws is not None and ws not in matched:
                matched.append(ws)

        digits = "".join(filter(str.isdigit, uid_str))
        if not matched and 8 <= len(digits) < 10:
            # A short or partial number: compare digits with every connection (rare)
            for conn_id, ws in list(self.active_connections.items()):
                conn_digits = "".join(filter(str.isdigit, str(conn_id)))
                if conn_digits and (digits.endswith(conn_digits) or conn_digits.endswith(digits)):
                    matched.append(ws)
        if include_linked:
            matched.extend(ws for ws in self._linked_sockets(uid_str) if ws not in matched)
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
        for owner, sockets in list(self.linked.items()):
            if ws in sockets:
                sockets.pop(ws, None)
                dead_keys.append(f"{owner}#linked")
                if not sockets:
                    self.linked.pop(owner, None)
        if dead_keys:
            logger.info("Cleaned up dead socket for %s (reason: %s)", dead_keys, reason or "send_failure")

    async def _publish_to_user(self, user_id: str, message_payload: dict[str, Any]) -> None:
        """Hands a frame to the other servers; the one holding the user's sockets delivers it."""
        if not self.redis:
            return
        try:
            await self.redis.publish(
                f"{REDIS_USER_CHANNEL_PREFIX}{self._canonical_id(user_id)}",
                json.dumps({"origin_node": self.node_id, "payload": message_payload}),
            )
        except Exception as e:
            logger.error("Redis publish error for %s: %s", user_id, e)

    async def send_personal_message(self, user_id: str, message_payload: dict[str, Any]) -> bool:
        """
        Routes a payload directly to the user if on this local node,
        and publishes to user's canonical Redis channel for other cluster nodes.
        Avoids duplicate local delivery. Returns True if the user is connected somewhere.
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
            if message_payload.get("type") == "chat_message":
                # Someone new wrote to them: they see that person's online status from now on
                sender = (message_payload.get("data") or {}).get("sender_id")
                if sender:
                    self.watch_presence(user_id, [sender])

        # 2. Distributed routing through Redis (one publish to the user's canonical channel)
        await self._publish_to_user(user_id, message_payload)
        if not delivered and self.redis:
            try:
                delivered = bool(await self._redis_online(self._id_forms([user_id])))
            except Exception:
                pass
        return delivered

    async def _listen_cluster(self) -> None:
        """The one pub/sub connection of this server: frames for users connected here, and presence
        events from other servers. Reconnects by itself if Redis drops the connection."""
        backoff = 1.0
        while self.redis:
            pubsub = self.redis.pubsub()
            try:
                await pubsub.psubscribe(f"{REDIS_USER_CHANNEL_PREFIX}*")
                await pubsub.subscribe(REDIS_PRESENCE_CHANNEL)
                backoff = 1.0
                while True:
                    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=PUBSUB_POLL_SECONDS)
                    if not message:
                        continue
                    if message.get("type") == "pmessage":
                        await self._deliver_cluster_frame(str(message.get("channel") or ""), message.get("data"))
                    elif message.get("type") == "message" and message.get("channel") == REDIS_PRESENCE_CHANNEL:
                        await self._deliver_cluster_presence(message.get("data"))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Redis pub/sub listener error (%s); reconnecting in %.0fs", e, backoff)
            finally:
                try:
                    await pubsub.aclose()
                except Exception:
                    pass
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    async def _deliver_cluster_frame(self, channel: str, raw_data: Any) -> None:
        """A frame another server published for a user: delivered here if they're connected here."""
        target = channel[len(REDIS_USER_CHANNEL_PREFIX):]
        try:
            parsed = json.loads(raw_data)
            if isinstance(parsed, dict) and parsed.get("origin_node") == self.node_id:
                return  # this server already delivered it to its own sockets
            payload_data = parsed.get("payload", parsed) if isinstance(parsed, dict) else parsed
            payload_str = json.dumps(payload_data)
        except Exception:
            payload_data, payload_str = None, raw_data
        sockets = self._find_all_local_ws(target)
        for ws in sockets:
            try:
                await ws.send_text(payload_str)
            except Exception as e:
                logger.warning("PubSub local delivery failed for %s: %s", target, e)
                self._remove_dead_socket(ws, f"pubsub delivery failure: {e}")
        if sockets and isinstance(payload_data, dict) and payload_data.get("type") == "chat_message":
            sender = (payload_data.get("data") or {}).get("sender_id")
            if sender:
                self.watch_presence(target, [sender])

    async def _deliver_cluster_presence(self, raw_data: Any) -> None:
        try:
            parsed = json.loads(raw_data)
            if parsed.get("origin_node") == self.node_id:
                return
            frame = parsed.get("payload") or parsed
            skip_id = (frame.get("data") or {}).get("user_id")
            await self._fanout_presence_local(frame, skip_user_id=skip_id)
        except Exception:
            pass

    # -------------------------------------------------------------------------
    # Presence & Broadcasts
    # -------------------------------------------------------------------------
    def _is_local_id(self, value: str) -> bool:
        return (
            value in self.active_connections
            or value in self.id_aliases
            or value in self.linked
            or value in self.linked_aliases
        )

    async def get_online_user_ids(self, among: Optional[Iterable[str]] = None) -> list[str]:
        """Which of `among` are online (on any server). Without `among`: who is online on this server."""
        if among is None:
            ids: set[str] = set(self.active_connections.keys())
            for alias_set in self.user_aliases.values():
                ids.update(alias_set)
            ids.update(self.linked.keys())
            ids.update(self.linked_aliases.keys())
        else:
            wanted = {str(i) for i in among if i}
            ids = {i for i in wanted if self._is_local_id(i)}
            rest = wanted - ids
            if rest and self.redis:
                try:
                    ids.update(await self._redis_online(rest))
                except Exception:
                    pass
        # Backgrounded apps still have a socket for a few seconds but count as offline
        return [i for i in ids if not self.is_away(i)]

    async def send_online_snapshot(self, websocket: WebSocket, visible_ids: Optional[set] = None) -> None:
        """Who's online, limited to `visible_ids` (the user's contacts) so nobody can harvest numbers."""
        online = await self.get_online_user_ids(visible_ids)
        await websocket.send_text(json.dumps({
            "type": "online_users_snapshot",
            "data": {"online_user_ids": online},
        }))

    def mark_away(self, user_id: str) -> None:
        self.away.update(self._id_forms(self._aliases_for(user_id) or [user_id]))
        self.away.add(user_id)
        self._presence_written.pop(user_id, None)  # coming back writes the presence keys again at once

    def mark_active(self, user_id: str) -> None:
        self.away.difference_update(self._id_forms(self._aliases_for(user_id) or [user_id]))
        self.away.discard(user_id)

    def set_device_active(self, user_id: str, device: str, active: bool) -> bool:
        """Records whether one of the user's devices is in use. Returns True if any of them still is."""
        devices = self.active_devices.setdefault(user_id, set())
        if active:
            devices.add(device)
        else:
            devices.discard(device)
        if not devices:
            self.active_devices.pop(user_id, None)
            return False
        return True

    def in_use(self, user_id: str) -> bool:
        return bool(self.active_devices.get(user_id))

    async def show_idle(self, user_id: str, aliases: Optional[list] = None) -> None:
        """Devices still connected but none in use: show them offline (last seen now)."""
        if self.is_away(user_id):
            return
        self.mark_away(user_id)
        await self.broadcast_presence(
            user_id, is_online=False, last_seen=datetime.now(timezone.utc).isoformat(), aliases=aliases
        )

    def is_away(self, *ids: Optional[str]) -> bool:
        """mark_away stores every form of the ids (with their last 10 digits), so this is a few lookups."""
        if not self.away:
            return False
        for i in ids:
            if not i:
                continue
            s = str(i)
            if s in self.away or self.id_aliases.get(s) in self.away:
                return True
            last10 = self._last10(s)
            if last10 and last10 in self.away:
                return True
        return False

    async def refresh_presence(self, user_id: str) -> None:
        """Keeps the user's presence keys alive while a socket is in use (called on every ping)."""
        if not user_id or self.is_away(user_id):
            return
        aliases = frozenset(self._aliases_for(user_id))
        written = self._presence_written.get(user_id)
        if written and aliases <= written[1] and time.monotonic() - written[0] < PRESENCE_REFRESH_SECONDS:
            return
        await self._write_presence(user_id, aliases)

    async def is_user_online(self, user_id: str, phone_number: Optional[str] = None) -> bool:
        """Checks if a user is currently online on any node with phone digit matching."""
        if not user_id and not phone_number:
            return False
        if self.is_away(user_id, phone_number):
            return False
        if user_id and self._find_local_ws(user_id):
            return True
        if phone_number and self._find_local_ws(phone_number):
            return True
        if self.redis:
            try:
                return bool(await self._redis_online(self._id_forms([user_id, phone_number])))
            except Exception:
                pass
        return False

    def is_last_seen_hidden(self, user_id: str) -> bool:
        return user_id in self.last_seen_hidden or self.id_aliases.get(user_id) in self.last_seen_hidden

    async def _fanout_presence_local(self, frame: dict[str, Any], skip_user_id: Optional[str] = None) -> None:
        """Sends a presence change to the sockets on this server of the people who chat with that user."""
        data = frame.get("data") or {}
        subject = self._id_forms([data.get("user_id"), *(data.get("aliases") or [])])
        owners: set[str] = set()
        for form in subject:
            owners.update(self.presence_watchers.get(form, ()))
        if not owners:
            return
        payload = json.dumps(frame)
        # People who hide their own last seen can't see anyone else's either (online still shows)
        payload_no_last_seen = (
            json.dumps({**frame, "data": {**data, "last_seen": None}}) if data.get("last_seen") else payload
        )
        skip_ids = set()
        if skip_user_id:
            skip_ids.update(self._aliases_for(skip_user_id))
            skip_ids.add(str(skip_user_id))
        for owner in owners:
            if owner in skip_ids:
                continue
            sockets = []
            phone_ws = self.active_connections.get(owner)
            if phone_ws is not None:
                sockets.append(phone_ws)
            sockets.extend(self.linked.get(owner, {}).keys())
            text = payload_no_last_seen if self.is_last_seen_hidden(owner) else payload
            for ws in sockets:
                try:
                    await ws.send_text(text)
                except Exception as e:
                    self._remove_dead_socket(ws, f"presence fanout failure: {e}")

    async def broadcast_presence(
        self,
        user_id: str,
        is_online: bool,
        last_seen: Optional[str] = None,
        aliases: Optional[list[str]] = None,
    ) -> None:
        """Notifies the people who chat with this user (on every server) about a presence change."""
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
