"""
Interface Core — Lifecycle & Drift Watch Phases
Monitors active bridges (health, reconnect, teardown) and handles contract drift.

active_bridges value is now a 3-tuple: (Bridge, Adapter, BridgeRuntime | None)
The runtime slot is None for bridges restored from store before a runner is attached.
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
        cutoff = now - self._window_s
        self._events = [(t, s) for t, s in self._events if t >= cutoff]
        # Use property so OPEN→HALF_OPEN time transition fires first
        current = self.state
        if current == self.HALF_OPEN:
            if success:
                self._state = self.CLOSED
            else:
                self._state      = self.OPEN
                self._tripped_at = now
        elif current == self.CLOSED:
            total = len(self._events)
            if total >= 5:
                failures   = sum(1 for _, s in self._events if not s)
                error_rate = failures / total
                if error_rate >= self._threshold:
                    self._state      = self.OPEN
                    self._tripped_at = now

    def is_open(self) -> bool:
        return self.state == self.OPEN

    def allow_probe(self) -> bool:
        return self.state == self.HALF_OPEN


# ─────────────────────────────────────────────
# Idempotency Guard
# ─────────────────────────────────────────────

class IdempotencyGuard:
    """TTL-based message ID deduplication."""

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
        cutoff  = time.time() - self._ttl
        expired = [k for k, v in self._seen.items() if v < cutoff]
        for k in expired:
            del self._seen[k]


# ─────────────────────────────────────────────
# Bridge Runtime (per-bridge state + metrics)
# ─────────────────────────────────────────────

class BridgeRuntime:
    """
    Resilience wrapper around a single bridge's adapter.
    Owned by the BridgeRunner I/O loop; monitored by LifecyclePhase.
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
        self._buf_max       = settings.buffer_size
        self._msg_count     = 0
        self._err_count     = 0
        self._lat_samples: list = []

    def process(self, payload: Any, msg_id: Optional[str] = None) -> Optional[Any]:
        """Transform one message. Returns output or None on drop/error."""
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
    Polls every lifecycle_interval_ms.
    active_bridges values are 3-tuples: (Bridge, Adapter, BridgeRuntime|None)
    """

    def __init__(
        self,
        registry:        NodeRegistry,
        graph:           CapabilityGraph,
        active_bridges:  Dict,
        bridge_queue:    Any,
        event_bus:       Any,
        matching_phase:  Any,
        store:           Any = None,   # BridgeStore — for persistence on teardown
        runners:         Any = None,   # shared runners dict from InterfaceCore
    ) -> None:
        self._registry       = registry
        self._graph          = graph
        self._active_bridges = active_bridges
        self._bq             = bridge_queue
        self._bus            = event_bus
        self._matching       = matching_phase
        self._store          = store
        self._runners        = runners if runners is not None else {}
        self._pg             = None    # set by main after construction
        self._telemetry      = None    # set by main after construction
        self._running        = False
        self._NODE_STALE_S   = float(settings.node_stale_s)

    async def run(self) -> None:
        self._running = True
        logger.info("LifecyclePhase started")
        _consecutive_errors = 0
        _MAX_CONSECUTIVE    = 10
        while self._running:
            await asyncio.sleep(settings.lifecycle_interval_ms / 1000.0)
            try:
                await self._tick()
                _consecutive_errors = 0   # reset on success
            except asyncio.CancelledError:
                break
            except Exception as exc:
                _consecutive_errors += 1
                logger.error("LifecyclePhase._tick error (%d/%d): %s",
                             _consecutive_errors, _MAX_CONSECUTIVE, exc)
                if _consecutive_errors >= _MAX_CONSECUTIVE:
                    logger.critical(
                        "LIFECYCLE PHASE FATAL: %d consecutive errors — "
                        "stopping to prevent silent corrupt state. "
                        "Restart the daemon to recover.",
                        _consecutive_errors,
                    )
                    self._running = False
                    return

    async def stop(self) -> None:
        self._running = False

    async def _tick(self) -> None:
        now  = time.time()
        dead: list = []

        for key, entry in list(self._active_bridges.items()):
            # Support both old 2-tuple and new 3-tuple during migration
            bridge  = entry[0]
            adapter = entry[1]
            rt      = entry[2] if len(entry) > 2 else None

            producer = self._registry.get(bridge.producer_id)
            consumer = self._registry.get(bridge.consumer_id)

            # Node gone?
            p_gone = producer is None or now - producer.last_seen > self._NODE_STALE_S
            c_gone = consumer is None or now - consumer.last_seen > self._NODE_STALE_S

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

            # Update telemetry with runtime metrics (buffer fill, circuit state)
            if rt:
                telemetry = getattr(self, '_telemetry', None)
                if telemetry:
                    buf_fill = len(rt._msg_buf) / max(rt._buf_max, 1)
                    telemetry.update_bridge_metrics(
                        bridge.id, buf_fill, rt.circuit.is_open()
                    )

            # Check runtime health
            if rt and rt.error_rate > settings.lifecycle_error_threshold:
                if bridge.retry_count < settings.reconnect_max:
                    bridge.retry_count += 1
                    logger.warning("Bridge %s degraded (err=%.0f%%), retry %d",
                                   bridge.id, rt.error_rate * 100, bridge.retry_count)
                else:
                    logger.error(
                        "Bridge %s failed after %d reconnects, tearing down"
                        "  err_rate=%.1f%%",
                        bridge.id, bridge.retry_count,
                        rt.error_rate * 100,
                    )
                    dead.append((key, "persistent-errors"))
                    continue

            # Latency baseline / degradation check
            if rt and bridge.latency_baseline > 0:
                if rt.latency_p50_ms > bridge.latency_baseline * settings.latency_degradation_factor:
                    logger.warning("Bridge %s latency degraded p50=%.1fms baseline=%.1fms",
                                   bridge.id, rt.latency_p50_ms, bridge.latency_baseline)
            elif rt and rt._msg_count >= 100 and bridge.latency_baseline == 0:
                bridge.latency_baseline = rt.latency_p50_ms
                logger.info("Bridge %s baseline=%.1fms", bridge.id, bridge.latency_baseline)

        for key, reason in dead:
            await self._teardown(key, reason)

    async def _teardown(self, key: Tuple, reason: str) -> None:
        entry = self._active_bridges.pop(key, None)
        if not entry:
            return
        bridge = entry[0]
        bridge.state = BridgeState.TEARING_DOWN
        logger.debug("Bridge %s state: TEARING_DOWN", bridge.id)

        logger.info(
            "BRIDGE_TORN %s  %s→%s  reason=%s",
            bridge.id, bridge.producer_id, bridge.consumer_id, reason
        )

        # Reweight the capability graph edge so the pair re-scores
        # fresh on the next matching cycle rather than using stale weight
        self._graph.reweight_edges(bridge.producer_id, new_weight=0.0)

        # Stop the I/O runner if one exists
        run_key = f"{key[0]}:{key[1]}"
        runner  = self._runners.pop(run_key, None)
        if runner:
            asyncio.create_task(runner.stop())

        bridge.state = BridgeState.TORN_DOWN
        logger.debug("Bridge %s state: TORN_DOWN", bridge.id)

        # Improvement #2: delete from persistent store
        if self._store:
            asyncio.create_task(self._store.delete(bridge.producer_id, bridge.consumer_id))
        if self._pg:
            asyncio.create_task(self._pg.delete_bridge(bridge.producer_id, bridge.consumer_id))
            # Durably record BRIDGE_TORN in audit log
            asyncio.create_task(self._pg.write_audit(
                'BRIDGE_TORN',
                bridge.producer_id, bridge.consumer_id,
                0.0,
                f'reason={reason} bridge_id={bridge.id}',
            ))

        # Record outcome
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
    Every drift_watch_interval_ms: re-synthesizes adapters for drifted nodes.
    Handles 3-tuple active_bridges entries.
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
                (key, entry[0], entry[1])
                for key, entry in self._active_bridges.items()
                if key[0] == node_id or key[1] == node_id
            ]
            for key, bridge, old_adapter in affected:
                await self._reload(key, bridge, old_adapter, node_id)

    async def _reload(self, key, bridge, old_adapter, drifted_node_id: str) -> None:
        from ..phases.synthesis import _synthesize_adapter, _validate_adapter
        producer = self._registry.get(bridge.producer_id)
        consumer = self._registry.get(bridge.consumer_id)
        if not producer or not consumer:
            return

        loop        = asyncio.get_event_loop()
        new_adapter = await loop.run_in_executor(
            None, _synthesize_adapter, producer, consumer, self._matcher
        )

        if new_adapter and _validate_adapter(new_adapter, producer, consumer):
            # Preserve the existing runtime; swap only the adapter
            existing = self._active_bridges.get(key)
            rt = existing[2] if existing and len(existing) > 2 else None
            if rt:
                rt.adapter = new_adapter   # update adapter in-place
            self._active_bridges[key] = (bridge, new_adapter, rt)
            logger.info("ADAPTER_RELOADED bridge %s (drift on %s)", bridge.id, drifted_node_id)
            await self._bus.publish_async(Event(
                type=EventType.ADAPTER_RELOADED,
                bridge_id=bridge.id,
                payload={"drifted_node": drifted_node_id},
            ))
        else:
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
