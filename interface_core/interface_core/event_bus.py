"""
Interface Core — Event Bus
Internal pub/sub using ZeroMQ. All phases communicate through this bus.
Falls back to a pure-Python in-process queue if ZeroMQ is unavailable.
"""

from __future__ import annotations
import asyncio
import json
import logging
import threading
from typing import Any, Callable, Dict, List, Optional

from .models import Event, EventType

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# In-process fallback (always works)
# ─────────────────────────────────────────────

class InProcessEventBus:
    """
    Thread-safe pub/sub bus backed by asyncio queues.
    Used in tests and when ZeroMQ is not available.
    """

    def __init__(self) -> None:
        self._subscribers: Dict[Optional[EventType], List[asyncio.Queue]] = {}
        self._lock = threading.RLock()

    def subscribe(self, event_type: Optional[EventType] = None) -> asyncio.Queue:
        """Return a queue that receives events. None = subscribe to everything."""
        q: asyncio.Queue = asyncio.Queue(maxsize=4096)
        with self._lock:
            self._subscribers.setdefault(event_type, []).append(q)
        return q

    def publish(self, event: Event) -> None:
        with self._lock:
            targets: List[asyncio.Queue] = (
                self._subscribers.get(event.type, []) +
                self._subscribers.get(None, [])
            )
        for q in targets:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                logger.warning("EventBus queue full, dropping event %s", event.type)

    async def publish_async(self, event: Event) -> None:
        self.publish(event)

    async def next_event(self, queue: asyncio.Queue, timeout: float = 1.0) -> Optional[Event]:
        try:
            return await asyncio.wait_for(queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None


# ─────────────────────────────────────────────
# ZeroMQ bus (preferred when zmq is available)
# ─────────────────────────────────────────────

try:
    import zmq
    import zmq.asyncio

    class ZMQEventBus:
        """
        ZeroMQ PUB/SUB bus.
        Publisher binds on pub_addr; subscribers connect to sub_addr.
        Topics are EventType string values.
        """

        def __init__(self, pub_addr: str, sub_addr: str) -> None:
            self._ctx       = zmq.asyncio.Context()
            self._pub_sock  = self._ctx.socket(zmq.PUB)
            self._pub_sock.bind(pub_addr)
            self._sub_addr  = sub_addr
            self._pub_addr  = pub_addr
            logger.info("ZMQ EventBus PUB bound on %s", pub_addr)

        def _new_sub(self, topic: str = "") -> zmq.asyncio.Socket:
            sock = self._ctx.socket(zmq.SUB)
            sock.connect(self._sub_addr)
            sock.setsockopt_string(zmq.SUBSCRIBE, topic)
            return sock

        def publish(self, event: Event) -> None:
            topic = event.type.value
            payload = event.json()
            self._pub_sock.send_string(f"{topic} {payload}", flags=zmq.NOBLOCK)

        async def publish_async(self, event: Event) -> None:
            topic = event.type.value
            payload = event.json()
            await self._pub_sock.send_string(f"{topic} {payload}")

        def subscribe(self, event_type: Optional[EventType] = None) -> asyncio.Queue:
            """Spawn a background reader that feeds an asyncio.Queue."""
            topic = event_type.value if event_type else ""
            sock  = self._new_sub(topic)
            q: asyncio.Queue = asyncio.Queue(maxsize=4096)

            async def _reader():
                while True:
                    msg = await sock.recv_string()
                    _, body = msg.split(" ", 1)
                    try:
                        event = Event.parse_raw(body)
                        await q.put(event)
                    except Exception as exc:
                        logger.debug("ZMQ parse error: %s", exc)

            # schedule as background task (caller must be inside an event loop)
            asyncio.ensure_future(_reader())
            return q

        async def next_event(self, queue: asyncio.Queue, timeout: float = 1.0) -> Optional[Event]:
            try:
                return await asyncio.wait_for(queue.get(), timeout=timeout)
            except asyncio.TimeoutError:
                return None

        def close(self) -> None:
            self._pub_sock.close()
            self._ctx.term()

    _ZMQ_AVAILABLE = True
except ImportError:
    _ZMQ_AVAILABLE = False


# ─────────────────────────────────────────────
# Factory
# ─────────────────────────────────────────────

def make_event_bus(pub_addr: str = "tcp://127.0.0.1:5555",
                   sub_addr: str = "tcp://127.0.0.1:5555") -> Any:
    if _ZMQ_AVAILABLE:
        try:
            bus = ZMQEventBus(pub_addr, sub_addr)
            logger.info("Using ZeroMQ event bus")
            return bus
        except Exception as exc:
            logger.warning("ZMQ unavailable (%s), falling back to in-process bus", exc)
    logger.info("Using in-process event bus")
    return InProcessEventBus()
