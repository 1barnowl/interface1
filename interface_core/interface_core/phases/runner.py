"""
Interface Core — Bridge Runner (Improvement #1)
Actual data-movement loop per live bridge.

Each BridgeRunner owns one asyncio task that:
  1. Reads from the producer transport (HTTP poll, file tail, or in-memory queue)
  2. Passes the payload through the adapter transform pipeline
  3. Writes the result to the consumer transport (HTTP POST or in-memory queue)
  4. Records latency + outcome in the BridgeRuntime

Supported transports right now:
  - REST producer  → HTTP GET poll at emission_rate_hz
  - FILE producer  → async file tail (line-by-line)
  - MEMORY         → asyncio.Queue for programmatic / test use
  - REST consumer  → HTTP POST
  - MEMORY consumer→ asyncio.Queue

Adding a new transport = subclass ProducerTransport or ConsumerTransport.
"""

from __future__ import annotations
import asyncio
import json
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Transport abstractions
# ─────────────────────────────────────────────

class ProducerTransport(ABC):
    """Yields one message at a time from the producer."""

    @abstractmethod
    async def read(self) -> Optional[Any]:
        """Return next payload, or None if nothing available yet. Never blocks forever."""
        ...

    async def close(self) -> None:
        pass


class ConsumerTransport(ABC):
    """Delivers one transformed message to the consumer."""

    @abstractmethod
    async def write(self, payload: Any) -> bool:
        """Deliver payload. Returns True on success."""
        ...

    async def close(self) -> None:
        pass


# ─────────────────────────────────────────────
# REST transport (poll / POST)
# ─────────────────────────────────────────────

class RESTProducerTransport(ProducerTransport):
    """
    Polls a REST endpoint at the node's emission rate.
    Uses aiohttp if available; falls back to urllib for zero-dep operation.
    """

    def __init__(self, endpoint: str, rate_hz: float = 1.0) -> None:
        self._endpoint  = endpoint
        self._interval  = 1.0 / max(rate_hz, 0.01)
        self._last_poll = 0.0
        self._session: Optional[Any] = None

    async def _ensure_session(self) -> None:
        if self._session is None:
            try:
                import aiohttp
                self._session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=5)
                )
            except ImportError:
                self._session = "_urllib"

    async def read(self) -> Optional[Any]:
        now = time.monotonic()
        if now - self._last_poll < self._interval:
            await asyncio.sleep(max(0, self._interval - (now - self._last_poll)))
        self._last_poll = time.monotonic()

        await self._ensure_session()
        try:
            if self._session == "_urllib":
                import urllib.request
                loop = asyncio.get_event_loop()
                def _fetch():
                    with urllib.request.urlopen(self._endpoint, timeout=5) as r:
                        return json.loads(r.read())
                return await loop.run_in_executor(None, _fetch)
            else:
                async with self._session.get(self._endpoint) as r:
                    if r.status == 200:
                        return await r.json(content_type=None)
        except Exception as exc:
            logger.debug("REST poll failed %s: %s", self._endpoint, exc)
        return None

    async def close(self) -> None:
        if self._session and self._session != "_urllib":
            await self._session.close()


class RESTConsumerTransport(ConsumerTransport):
    """POSTs transformed payloads to a REST endpoint."""

    def __init__(self, endpoint: str) -> None:
        self._endpoint = endpoint
        self._session: Optional[Any] = None

    async def _ensure_session(self) -> None:
        if self._session is None:
            try:
                import aiohttp
                self._session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=5)
                )
            except ImportError:
                self._session = "_urllib"

    async def write(self, payload: Any) -> bool:
        await self._ensure_session()
        body = json.dumps(payload).encode() if isinstance(payload, dict) else payload
        try:
            if self._session == "_urllib":
                import urllib.request
                req = urllib.request.Request(
                    self._endpoint, data=body,
                    headers={"Content-Type": "application/json"}, method="POST"
                )
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, urllib.request.urlopen, req)
                return True
            else:
                async with self._session.post(
                    self._endpoint, data=body,
                    headers={"Content-Type": "application/json"}
                ) as r:
                    return r.status < 400
        except Exception as exc:
            logger.debug("REST POST failed %s: %s", self._endpoint, exc)
            return False

    async def close(self) -> None:
        if self._session and self._session != "_urllib":
            await self._session.close()


# ─────────────────────────────────────────────
# File tail transport
# ─────────────────────────────────────────────

class FileProducerTransport(ProducerTransport):
    """
    Async file tail: emits one line at a time, sleeping when EOF is reached.
    Handles JSON lines and plain text.
    """

    def __init__(self, path: str, poll_interval_s: float = 0.2) -> None:
        self._path     = path
        self._interval = poll_interval_s
        self._fh       = None
        self._pos      = 0

    async def read(self) -> Optional[Any]:
        loop = asyncio.get_event_loop()

        def _tail():
            import os
            try:
                if self._fh is None:
                    self._fh = open(self._path, "r", errors="ignore")
                    self._fh.seek(0, 2)           # seek to end on open
                    self._pos = self._fh.tell()
                line = self._fh.readline()
                if not line:
                    return None
                self._pos = self._fh.tell()
                line = line.strip()
                if not line:
                    return None
                try:
                    return json.loads(line)
                except Exception:
                    return {"line": line}
            except Exception as exc:
                logger.debug("File tail error %s: %s", self._path, exc)
                return None

        result = await loop.run_in_executor(None, _tail)
        if result is None:
            await asyncio.sleep(self._interval)
        return result

    async def close(self) -> None:
        if self._fh:
            self._fh.close()
            self._fh = None


# ─────────────────────────────────────────────
# In-memory queue transport (for tests + programmatic use)
# ─────────────────────────────────────────────

class MemoryProducerTransport(ProducerTransport):
    """Reads from an asyncio.Queue. Used for tests and embedded producers."""

    def __init__(self, queue: Optional[asyncio.Queue] = None) -> None:
        self._q = queue or asyncio.Queue(maxsize=4096)

    @property
    def queue(self) -> asyncio.Queue:
        return self._q

    async def read(self) -> Optional[Any]:
        try:
            return await asyncio.wait_for(self._q.get(), timeout=0.1)
        except asyncio.TimeoutError:
            return None


class MemoryConsumerTransport(ConsumerTransport):
    """Writes to an asyncio.Queue. Used for tests and embedded consumers."""

    def __init__(self, queue: Optional[asyncio.Queue] = None) -> None:
        self._q = queue or asyncio.Queue(maxsize=4096)

    @property
    def queue(self) -> asyncio.Queue:
        return self._q

    async def write(self, payload: Any) -> bool:
        try:
            self._q.put_nowait(payload)
            return True
        except asyncio.QueueFull:
            return False


# ─────────────────────────────────────────────
# Transport factory
# ─────────────────────────────────────────────

def make_producer_transport(node: Any) -> ProducerTransport:
    """Select the best-fit transport for a producer node."""
    from ..models import Protocol
    from .transports import (
        KafkaProducerTransport, MQTTProducerTransport, SocketProducerTransport,
        WebSocketProducerTransport, AMQPProducerTransport,
        ZMQProducerTransport,
    )
    proto    = node.protocol
    endpoint = node.endpoint
    rate     = node.contract.emission_rate_hz or 1.0

    # Endpoint scheme takes priority over protocol tag
    if endpoint.startswith("memory://"):
        return MemoryProducerTransport()
    if proto == Protocol.REST:
        return RESTProducerTransport(endpoint, rate)
    if proto == Protocol.FILE:
        return FileProducerTransport(endpoint)
    if proto == Protocol.KAFKA:
        return KafkaProducerTransport(endpoint)
    if proto == Protocol.MQTT:
        return MQTTProducerTransport(endpoint)
    if proto == Protocol.SOCKET:
        return SocketProducerTransport(endpoint)
    if proto == Protocol.GRPC:
        from .grpc_transport import GRPCProducerTransport
        return GRPCProducerTransport(endpoint, rate)
    if proto == Protocol.WEBSOCKET:
        return WebSocketProducerTransport(endpoint)
    if proto == Protocol.AMQP:
        return AMQPProducerTransport(endpoint)
    if proto == Protocol.ZEROMQ:
        return ZMQProducerTransport(endpoint)
    # Default: in-memory queue (inject() API or tests)
    return MemoryProducerTransport()


def make_consumer_transport(node: Any) -> ConsumerTransport:
    """Select the best-fit transport for a consumer node."""
    from ..models import Protocol
    from .transports import (
        KafkaConsumerTransport, MQTTConsumerTransport, SocketConsumerTransport,
        WebSocketConsumerTransport, AMQPConsumerTransport,
        ZMQConsumerTransport,
    )
    proto    = node.protocol
    endpoint = node.endpoint

    if endpoint.startswith("memory://"):
        return MemoryConsumerTransport()
    if proto == Protocol.REST:
        return RESTConsumerTransport(endpoint)
    if proto == Protocol.KAFKA:
        return KafkaConsumerTransport(endpoint)
    if proto == Protocol.MQTT:
        return MQTTConsumerTransport(endpoint)
    if proto == Protocol.SOCKET:
        return SocketConsumerTransport(endpoint)
    if proto == Protocol.GRPC:
        from .grpc_transport import GRPCConsumerTransport
        return GRPCConsumerTransport(endpoint)
    if proto == Protocol.WEBSOCKET:
        return WebSocketConsumerTransport(endpoint)
    if proto == Protocol.AMQP:
        return AMQPConsumerTransport(endpoint)
    if proto == Protocol.ZEROMQ:
        return ZMQConsumerTransport(endpoint)
    # Default: in-memory queue
    return MemoryConsumerTransport()


# ─────────────────────────────────────────────
# BridgeRunner — the actual data-movement loop
# ─────────────────────────────────────────────

class BridgeRunner:
    """
    One asyncio task per live bridge.
    Reads from producer → transforms → writes to consumer.
    Delegates all resilience decisions to BridgeRuntime.
    """

    def __init__(
        self,
        bridge_id:          str,
        runtime:            Any,      # BridgeRuntime from lifecycle.py
        producer_transport: ProducerTransport,
        consumer_transport: ConsumerTransport,
        telemetry:          Any = None,  # Telemetry instance
    ) -> None:
        self._id         = bridge_id
        self._runtime    = runtime
        self._producer   = producer_transport
        self._consumer   = consumer_transport
        self._telemetry  = telemetry
        self._running    = False
        self._task: Optional[asyncio.Task] = None
        self._msg_count  = 0
        self._err_count  = 0

    def start(self) -> asyncio.Task:
        """Start the I/O loop. Works from both sync and async contexts."""
        self._running = True
        loop = None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            pass
        if loop and loop.is_running():
            self._task = loop.create_task(self._loop(), name=f"bridge-{self._id}")
        else:
            # Sync context (tests): schedule via the module-level loop
            self._task = asyncio.ensure_future(self._loop(), loop=asyncio.get_event_loop())
        logger.info("BridgeRunner started for bridge %s", self._id)
        return self._task

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._producer.close()
        await self._consumer.close()
        logger.info("BridgeRunner stopped for bridge %s", self._id)

    async def _loop(self) -> None:
        while self._running:
            try:
                # 1. Read from producer
                t0      = time.monotonic()
                payload = await self._producer.read()

                if payload is None:
                    # Nothing available — yield control and retry
                    await asyncio.sleep(0.01)
                    continue

                # 2. Transform via adapter (through runtime for resilience)
                msg_id  = _msg_id(payload)
                result  = self._runtime.process(payload, msg_id=msg_id)

                latency_ms = (time.monotonic() - t0) * 1000
                self._runtime.record_latency(latency_ms)

                if result is None:
                    # Dropped by circuit breaker, rate limiter, or idempotency guard
                    self._err_count += 1
                    continue

                # 3. Deliver to consumer
                ok = await self._consumer.write(result)
                if ok:
                    self._msg_count += 1
                    self._runtime.circuit.record(True)
                    if self._telemetry:
                        self._telemetry.record_message(
                            self._id,
                            self._runtime.bridge.producer_id,
                            self._runtime.bridge.consumer_id,
                            latency_ms,
                            success=True,
                        )
                else:
                    self._err_count += 1
                    self._runtime.circuit.record(False)
                    if self._telemetry:
                        self._telemetry.record_message(
                            self._id,
                            self._runtime.bridge.producer_id,
                            self._runtime.bridge.consumer_id,
                            latency_ms,
                            success=False,
                        )
                    logger.debug("Bridge %s consumer write failed", self._id)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("Bridge %s loop error: %s", self._id, exc)
                self._runtime.circuit.record(False)
                # Exponential backoff: 100ms base, doubles each failure, cap 30s
                import math
                from ..config import settings
                base     = settings.retry_base_ms / 1000.0
                ceiling  = settings.retry_ceil_ms  / 1000.0
                failures = self._err_count
                delay    = min(base * (2 ** min(failures, 10)), ceiling)
                jitter   = delay * 0.1 * (hash(self._id) % 10 / 10.0)
                await asyncio.sleep(delay + jitter)

    @property
    def stats(self) -> Dict[str, Any]:
        return {
            "bridge_id": self._id,
            "messages":  self._msg_count,
            "errors":    self._err_count,
            "circuit":   self._runtime.circuit.state,
            "latency_p50_ms": self._runtime.latency_p50_ms,
            "latency_p99_ms": self._runtime.latency_p99_ms,
        }


def _msg_id(payload: Any) -> Optional[str]:
    """Extract or generate a message ID for idempotency checking."""
    if isinstance(payload, dict):
        for key in ("id", "msg_id", "message_id", "uuid", "seq", "sequence"):
            if key in payload:
                return str(payload[key])
    return None
