"""
Interface Core — Lifecycle & Drift Watch Phases
Monitors active bridges (health, reconnect, teardown) and handles contract drift.
"""

from __future__ import annotations
import asyncio
import logging
import time
from typing import Any, Dict, Optional, Tuple

from ..config import settings
from ..models import Bridge, BridgeState, Event, EventType
from ..registry import CapabilityGraph, DriftLog, NodeRegistry

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Circuit Breaker
# ─────────────────────────────────────────────

class CircuitBreaker:
    """
    Sliding-window circuit breaker per bridge.
    States: CLOSED → OPEN (tripped) → HALF_OPEN (probe) → CLOSED
    """

    CLOSED    = "closed"
    OPEN      = "open"
    HALF_OPEN = "half_open"

    def __init__(
        self,
        threshold: float = 0.40,
        window_s:  float = 10.0,
        halfopen_s: float = 30.0,
    ) -> None:
        self._threshold   = threshold
        self._window_s    = window_s
        self._halfopen_s  = halfopen_s
        self._state       = self.CLOSED
        self._events: list = []    # (timestamp, success: bool)
        self._tripped_at: Optional[float] = None

    @property
    def state(self) -> str:
        if self._state == self.OPEN:
            if time.time() - (self._tripped_at or 0) >= self._halfopen_s:
                self._state = self.HALF_OPEN
        return self._state

    def record(self, success: bool) -> None:
        now = time.time()
        self._events.append((now, success))
        # Prune old events
        cutoff = now - self._window_s
        self._events = [(t, s) for t, s in self._events if t >= cutoff]
        # Evaluate — use property so OPEN→HALF_OPEN time transition fires first
        current = self.state
        if current == self.HALF_OPEN:
            if success:
                self._state = self.CLOSED
            else:
                self._state      = self.OPEN
                self._tripped_at = now
        elif current == self.CLOSED:
            total   = len(self._events)
            if total >= 5:  # min sample
                failures    = sum(1 for _, s in self._events if not s)
                error_rate  = failures / total
                if error_rate >= self._threshold:
                    self._state      = self.OPEN
                    self._tripped_at = now

    def is_open(self) -> bool:
        return self.state == self.OPEN

    def allow_probe(self) -> bool:
        return self.state == self.HALF_OPEN


# ─────────────────────────────────────────────
# Idempotency Guard (bloom-filter-like dedup)
# ─────────────────────────────────────────────

class IdempotencyGuard:
    """
    TTL-based message ID deduplication using a dict (simple; replace with
    Bloom filter for high-throughput deployments).
    """

    def __init__(self, ttl_s: int = 30) -> None:
        self._seen: Dict[str, float] = {}
        self._ttl  = ttl_s

    def is_duplicate(self, msg_id: str) -> bool:
        self._evict()
        if msg_id in self._seen:
            return True
        self._seen[msg_id] = time.time()
        return False

    def _evict(self) -> None:
        cutoff = time.time() - self._ttl
        expired = [k for k, v in self._seen.items() if v < cutoff]
        for k in expired:
            del self._seen[k]


# ─────────────────────────────────────────────
# Bridge runtime (per-bridge I/O loop skeleton)
# ─────────────────────────────────────────────

class BridgeRuntime:
    """
    Manages the active I/O loop for a single bridge.
    In a full deployment this wires up the actual transport read/write.
    Here we track metrics and expose the transform pipeline.
    """

    def __init__(self, bridge: Bridge, adapter: Any) -> None:
        self.bridge   = bridge
        self.adapter  = adapter
        self.circuit  = CircuitBreaker(
            threshold  = settings.circuit_error_threshold,
            window_s   = settings.circuit_window_s,
            halfopen_s = settings.circuit_halfopen_s,
        )
        self.idem     = IdempotencyGuard(settings.idempotency_ttl_s)
        self._msg_buf: list = []
        self._buf_max = settings.buffer_size
        self._msg_count = 0
        self._err_count = 0
        self._lat_samples: list = []   # ms

    def process(self, payload: Any, msg_id: Optional[str] = None) -> Optional[Any]:
        """Transform and route one message. Returns output or None on drop/error."""
        if self.circuit.is_open():
            self._buf_msg(payload)
            return None
        if msg_id and self.idem.is_duplicate(msg_id):
            return None
        try:
            result = self.adapter.transform(payload)
            self.circuit.record(True)
            self._msg_count += 1
            return result
        except Exception as exc:
            self.circuit.record(False)
            self._err_count += 1
            logger.debug("Bridge %s transform error: %s", self.bridge.id, exc)
            return None

    def _buf_msg(self, payload: Any) -> None:
        if len(self._msg_buf) < self._buf_max:
            self._msg_buf.append(payload)

    def drain_buffer(self) -> list:
        buf, self._msg_buf = self._msg_buf, []
        return buf

    def record_latency(self, ms: float) -> None:
        self._lat_samples.append(ms)
        if len(self._lat_samples) > 1000:
            self._lat_samples = self._lat_samples[-1000:]

    @property
    def error_rate(self) -> float:
        total = self._msg_count + self._err_count
        return (self._err_count / total) if total > 0 else 0.0

    @property
    def latency_p99_ms(self) -> float:
        if not self._lat_samples:
            return 0.0
        s = sorted(self._lat_samples)
        return s[int(len(s) * 0.99)]

    @property
    def latency_p50_ms(self) -> float:
        if not self._lat_samples:
            return 0.0
        s = sorted(self._lat_samples)
        return s[len(s) // 2]


# ─────────────────────────────────────────────
# Lifecycle Phase
# ─────────────────────────────────────────────

class LifecyclePhase:
    """
    Polls every lifecycle_interval_ms:
      - Collects metrics per bridge
      - Tears down unhealthy bridges
      - Removes nodes that have gone silent
      - Triggers adapter optimisation on latency degradation
    """

    def __init__(
        self,
        registry:        NodeRegistry,
        graph:           CapabilityGraph,
        active_bridges:  Dict,
        bridge_queue:    Any,
        event_bus:       Any,
        matching_phase:  Any,
    ) -> None:
        self._registry       = registry
        self._graph          = graph
        self._active_bridges = active_bridges
        self._bq             = bridge_queue
        self._bus            = event_bus
        self._matching       = matching_phase
        self._running        = False
        self._runtimes: Dict[str, BridgeRuntime] = {}
        # Node gone detection: node_id → last_seen
        self._NODE_STALE_S = 30.0

    def attach_runtime(self, key: Tuple, runtime: BridgeRuntime) -> None:
        self._runtimes[key[0] + ":" + key[1]] = runtime

    async def run(self) -> None:
        self._running = True
        logger.info("LifecyclePhase started")
        while self._running:
            await asyncio.sleep(settings.lifecycle_interval_ms / 1000.0)
            await self._tick()

    async def stop(self) -> None:
        self._running = False

    async def _tick(self) -> None:
        now  = time.time()
        dead: list = []

        for key, (bridge, adapter) in list(self._active_bridges.items()):
            producer = self._registry.get(bridge.producer_id)
            consumer = self._registry.get(bridge.consumer_id)

            # Node gone?
            p_gone = (producer is None or
                      now - producer.last_seen > self._NODE_STALE_S)
            c_gone = (consumer is None or
                      now - consumer.last_seen > self._NODE_STALE_S)

            if p_gone or c_gone:
                missing = bridge.producer_id if p_gone else bridge.consumer_id
                logger.warning("TEARDOWN %s: node %s gone", bridge.id, missing)
                dead.append((key, "node-gone"))
                if p_gone and producer:
                    self._registry.remove(bridge.producer_id)
                    self._graph.remove_node(bridge.producer_id)
                if c_gone and consumer:
                    self._registry.remove(bridge.consumer_id)
                    self._graph.remove_node(bridge.consumer_id)
                continue

            # Check runtime metrics
            rt_key = key[0] + ":" + key[1]
            rt     = self._runtimes.get(rt_key)
            if rt and rt.error_rate > settings.lifecycle_error_threshold:
                if bridge.retry_count < settings.reconnect_max:
                    bridge.retry_count += 1
                    logger.warning("Bridge %s degraded (err=%.0f%%), retry %d",
                                   bridge.id, rt.error_rate * 100, bridge.retry_count)
                else:
                    logger.error("Bridge %s failed after %d reconnects, tearing down",
                                 bridge.id, bridge.retry_count)
                    dead.append((key, "persistent-errors"))
                    continue

            # Latency degradation → log (adapter tuning hook)
            if rt and bridge.latency_baseline > 0:
                if rt.latency_p50_ms > bridge.latency_baseline * settings.latency_degradation_factor:
                    logger.warning("Bridge %s latency degraded p50=%.1fms baseline=%.1fms",
                                   bridge.id, rt.latency_p50_ms, bridge.latency_baseline)
            elif rt and rt._msg_count >= 100 and bridge.latency_baseline == 0:
                bridge.latency_baseline = rt.latency_p50_ms
                logger.info("Bridge %s latency baseline set to %.1fms",
                            bridge.id, bridge.latency_baseline)

        # Teardown dead bridges
        for key, reason in dead:
            await self._teardown(key, reason)

    async def _teardown(self, key: Tuple, reason: str) -> None:
        item = self._active_bridges.pop(key, None)
        if not item:
            return
        bridge, _ = item
        bridge.state = BridgeState.TORN_DOWN

        # Record outcome for history
        producer = self._registry.get(bridge.producer_id)
        consumer = self._registry.get(bridge.consumer_id)
        if producer and consumer and self._matching:
            success = reason not in ("persistent-errors",)
            self._matching.record_bridge_outcome(
                producer.protocol.value, consumer.protocol.value, success
            )

        await self._bus.publish_async(Event(
            type=EventType.BRIDGE_TORN,
            bridge_id=bridge.id,
            payload={"reason": reason},
        ))

        # Re-queue for re-evaluation if teardown was not due to node disappearing
        if reason != "node-gone":
            from ..models import BridgeCandidate, MatchScore
            candidate = BridgeCandidate(
                producer_id=bridge.producer_id,
                consumer_id=bridge.consumer_id,
                score=MatchScore(composite=0.5),
            )
            await self._bq.repush(candidate)


# ─────────────────────────────────────────────
# Drift Watch Phase
# ─────────────────────────────────────────────

class DriftWatchPhase:
    """
    Every drift_watch_interval_ms:
      - Finds nodes whose latest contract snapshot has delta >= threshold
      - For each affected bridge, re-synthesizes the adapter and hot-reloads
    """

    def __init__(
        self,
        registry:        NodeRegistry,
        drift_log:       DriftLog,
        active_bridges:  Dict,
        event_bus:       Any,
        ml_matcher:      Any,
        synthesis_queue: Any,
        bridge_queue:    Any,
    ) -> None:
        self._registry       = registry
        self._drift_log      = drift_log
        self._active_bridges = active_bridges
        self._bus            = event_bus
        self._matcher        = ml_matcher
        self._sq             = synthesis_queue
        self._bq             = bridge_queue
        self._running        = False

    async def run(self) -> None:
        self._running = True
        logger.info("DriftWatchPhase started")
        while self._running:
            await asyncio.sleep(settings.drift_watch_interval_ms / 1000.0)
            await self._tick()

    async def stop(self) -> None:
        self._running = False

    async def _tick(self) -> None:
        drifted = self._drift_log.drifted_nodes(settings.contract_drift_delta)
        for node_id in drifted:
            affected = [
                (key, bridge, adapter)
                for key, (bridge, adapter) in self._active_bridges.items()
                if key[0] == node_id or key[1] == node_id
            ]
            for key, bridge, old_adapter in affected:
                await self._reload(key, bridge, old_adapter, node_id)

    async def _reload(self, key, bridge, old_adapter, drifted_node_id: str) -> None:
        from ..adapters.base import _synthesize_adapter as _sa, _validate_adapter as _va
        producer = self._registry.get(bridge.producer_id)
        consumer = self._registry.get(bridge.consumer_id)
        if not producer or not consumer:
            return

        loop    = asyncio.get_event_loop()
        new_adapter = await loop.run_in_executor(
            None, _sa, producer, consumer, self._matcher
        )

        if new_adapter and _va(new_adapter, producer, consumer):
            # Hot-reload: atomically swap adapter in-place
            self._active_bridges[key] = (bridge, new_adapter)
            logger.info("ADAPTER_RELOADED bridge %s (drift on %s)", bridge.id, drifted_node_id)
            await self._bus.publish_async(Event(
                type=EventType.ADAPTER_RELOADED,
                bridge_id=bridge.id,
                payload={"drifted_node": drifted_node_id},
            ))
        else:
            # Re-synthesize from scratch
            logger.warning("Reload failed for bridge %s — tearing down", bridge.id)
            self._active_bridges.pop(key, None)
            await self._bus.publish_async(Event(
                type=EventType.BRIDGE_TORN,
                bridge_id=bridge.id,
                payload={"reason": "drift-reload-failed"},
            ))
            from ..models import BridgeCandidate, MatchScore
            candidate = BridgeCandidate(
                producer_id=bridge.producer_id,
                consumer_id=bridge.consumer_id,
                score=MatchScore(composite=0.5),
            )
            await self._bq.repush(candidate)
