"""
Interface Core — Fan-Out and Fan-In Topologies

Fan-out: one producer feeds N consumers through independent adapters.
Fan-in:  N producers feed one consumer through a normalizing adapter.

These are wired at synthesis time when the matching engine finds
a producer with multiple eligible consumers (fan-out) or a consumer
with multiple eligible producers (fan-in).
"""

from __future__ import annotations
import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Fan-Out Runner
# ─────────────────────────────────────────────

class FanOutRunner:
    """
    Reads from one producer transport and writes to N consumer transports
    in parallel.

    Each consumer has its own adapter (field mapping / protocol shim may differ).
    Failures in one consumer do not stop delivery to others.
    """

    def __init__(
        self,
        bridge_ids:           List[str],
        runtime:              Any,             # BridgeRuntime for the producer
        producer_transport:   Any,
        consumer_adapters:    List[Tuple[Any, Any]],  # [(consumer_transport, adapter), ...]
        telemetry:            Any = None,
    ) -> None:
        self._ids           = bridge_ids
        self._runtime       = runtime
        self._producer      = producer_transport
        self._consumers     = consumer_adapters   # [(transport, adapter)]
        self._telemetry     = telemetry
        self._running       = False
        self._task: Optional[asyncio.Task] = None
        self._msg_count     = 0
        self._err_count     = 0

    def start(self) -> asyncio.Task:
        self._running = True
        self._task    = asyncio.ensure_future(self._loop())
        logger.info(
            "FanOutRunner started  producer→%d consumers  bridges=%s",
            len(self._consumers), ",".join(self._ids),
        )
        return self._task

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._producer.close()
        for transport, _ in self._consumers:
            await transport.close()

    async def _loop(self) -> None:
        while self._running:
            try:
                t0      = time.monotonic()
                payload = await self._producer.read()
                if payload is None:
                    await asyncio.sleep(0.01)
                    continue

                latency_ms = (time.monotonic() - t0) * 1000

                # Deliver to all consumers in parallel
                results = await asyncio.gather(
                    *[self._deliver(payload, transport, adapter, bid)
                      for (transport, adapter), bid
                      in zip(self._consumers, self._ids)],
                    return_exceptions=True,
                )
                ok_count = sum(1 for r in results if r is True)
                if ok_count > 0:
                    self._msg_count += 1
                else:
                    self._err_count += 1

                if self._telemetry and self._ids:
                    self._telemetry.record_message(
                        self._ids[0], "fan-out", "multi",
                        latency_ms, success=(ok_count > 0)
                    )

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("FanOutRunner error: %s", exc)
                self._err_count += 1
                await asyncio.sleep(0.5)

    async def _deliver(
        self, payload: Any, transport: Any, adapter: Any, bridge_id: str
    ) -> bool:
        try:
            transformed = adapter.transform(payload)
            if transformed is None:
                return False
            ok = await transport.write(transformed)
            if not ok:
                logger.debug("FanOut: consumer write failed for bridge %s", bridge_id)
            return ok
        except Exception as exc:
            logger.debug("FanOut: delivery error for bridge %s: %s", bridge_id, exc)
            return False

    @property
    def stats(self) -> Dict[str, Any]:
        return {
            "type":       "fan_out",
            "bridge_ids": self._ids,
            "consumers":  len(self._consumers),
            "messages":   self._msg_count,
            "errors":     self._err_count,
        }


# ─────────────────────────────────────────────
# Fan-In Runner
# ─────────────────────────────────────────────

class FanInRunner:
    """
    Reads from N producer transports and merges into one consumer.

    Each producer has its own adapter. Messages are tagged with the source
    producer ID so the consumer can distinguish provenance.
    Merging is round-robin with a small buffer per producer.
    """

    def __init__(
        self,
        bridge_ids:             List[str],
        producer_transports:    List[Tuple[str, Any, Any]],  # [(producer_id, transport, adapter)]
        consumer_transport:     Any,
        runtime:                Any,
        telemetry:              Any = None,
    ) -> None:
        self._ids           = bridge_ids
        self._producers     = producer_transports
        self._consumer      = consumer_transport
        self._runtime       = runtime
        self._telemetry     = telemetry
        self._running       = False
        self._task: Optional[asyncio.Task] = None
        self._msg_count     = 0
        self._err_count     = 0
        # Per-producer asyncio queues for merged delivery
        self._queues: Dict[str, asyncio.Queue] = {
            pid: asyncio.Queue(maxsize=512)
            for pid, _, _ in producer_transports
        }

    def start(self) -> asyncio.Task:
        self._running = True
        # One reader task per producer + one merger task
        reader_tasks = [
            asyncio.ensure_future(self._read_producer(pid, transport, adapter))
            for pid, transport, adapter in self._producers
        ]
        merger_task = asyncio.ensure_future(self._merge_loop())
        self._task  = asyncio.ensure_future(
            asyncio.gather(*reader_tasks, merger_task, return_exceptions=True)
        )
        logger.info(
            "FanInRunner started  %d producers→consumer  bridges=%s",
            len(self._producers), ",".join(self._ids),
        )
        return self._task

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        for _, transport, _ in self._producers:
            await transport.close()
        await self._consumer.close()

    async def _read_producer(self, producer_id: str, transport: Any, adapter: Any) -> None:
        q = self._queues[producer_id]
        while self._running:
            try:
                payload = await transport.read()
                if payload is None:
                    await asyncio.sleep(0.01)
                    continue
                transformed = adapter.transform(payload)
                if transformed is None:
                    continue
                # Tag with source
                if isinstance(transformed, dict):
                    transformed["_ic_source_producer"] = producer_id
                try:
                    q.put_nowait(transformed)
                except asyncio.QueueFull:
                    pass
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.debug("FanIn reader %s error: %s", producer_id, exc)
                await asyncio.sleep(0.1)

    async def _merge_loop(self) -> None:
        """Round-robin drain all producer queues into the consumer."""
        producer_ids = [pid for pid, _, _ in self._producers]
        while self._running:
            any_data = False
            for pid in producer_ids:
                q = self._queues[pid]
                while not q.empty():
                    try:
                        payload = q.get_nowait()
                        t0 = time.monotonic()
                        ok = await self._consumer.write(payload)
                        latency_ms = (time.monotonic() - t0) * 1000
                        if ok:
                            self._msg_count += 1
                            any_data = True
                        else:
                            self._err_count += 1
                        if self._telemetry and self._ids:
                            self._telemetry.record_message(
                                self._ids[0], pid, "fan-in",
                                latency_ms, success=ok,
                            )
                    except asyncio.QueueEmpty:
                        break
            if not any_data:
                await asyncio.sleep(0.02)

    @property
    def stats(self) -> Dict[str, Any]:
        return {
            "type":      "fan_in",
            "bridge_ids": self._ids,
            "producers": len(self._producers),
            "messages":  self._msg_count,
            "errors":    self._err_count,
        }
