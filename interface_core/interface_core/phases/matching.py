"""
Interface Core — Matching Phase
Scores every producer/consumer pair and pushes viable candidates onto the bridge queue.
"""

from __future__ import annotations
import asyncio
import logging
import math
import time
from typing import Any, Dict, List, Optional, Tuple

from ..config import PROTOCOL_COMPAT_MATRIX, settings
from ..models import (
    BridgeCandidate, Event, EventType, MatchScore, Node, NodeRole
)
from ..registry import CapabilityGraph, NodeRegistry

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Bridge History (win/loss record per protocol pair)
# ─────────────────────────────────────────────

class BridgeHistory:
    """Tracks success/failure counts for (producer_protocol, consumer_protocol) pairs."""

    def __init__(self) -> None:
        self._counts: Dict[Tuple[str, str], Tuple[int, int]] = {}  # (ok, fail)

    def record(self, producer_protocol: str, consumer_protocol: str, success: bool) -> None:
        key = (producer_protocol, consumer_protocol)
        ok, fail = self._counts.get(key, (0, 0))
        if success:
            self._counts[key] = (ok + 1, fail)
        else:
            self._counts[key] = (ok, fail + 1)

    def success_rate(self, producer_protocol: str, consumer_protocol: str) -> float:
        key = (producer_protocol, consumer_protocol)
        ok, fail = self._counts.get(key, (0, 0))
        total = ok + fail
        if total == 0:
            return 0.5   # no history → neutral
        return ok / total


# ─────────────────────────────────────────────
# Scoring
# ─────────────────────────────────────────────

def _estimate_latency_score(producer: Node, consumer: Node) -> float:
    """
    Returns a 0-1 score where 1 = rates are well-matched.
    Mismatched rates (e.g. 1000 Hz producer → 1 Hz consumer) score lower.
    """
    p_rate = producer.contract.emission_rate_hz or 1.0
    c_rate = consumer.contract.emission_rate_hz or p_rate   # assume consumer can keep up
    if p_rate == 0:
        return 1.0
    ratio  = min(p_rate, c_rate) / max(p_rate, c_rate)
    # log scale: ratio of 0.1 → ~0.5 score
    return max(0.0, min(1.0, 0.5 + 0.5 * math.log10(max(ratio, 1e-4) * 10000) / 4))


def score_pair(
    producer:   Node,
    consumer:   Node,
    ml_matcher: Any,
    history:    BridgeHistory,
    policy_engine: Any,
    weights:    Dict[str, float],
) -> MatchScore:
    # 1. Semantic similarity
    p_emb = producer.embedding or ml_matcher.embed(producer.contract)
    c_emb = consumer.embedding or ml_matcher.embed(consumer.contract)
    sem_sim = ml_matcher.similarity(p_emb, c_emb)

    # 2. Protocol compatibility
    proto_compat = PROTOCOL_COMPAT_MATRIX.get(
        producer.protocol.value, {}
    ).get(consumer.protocol.value, 0.2)

    # 3. Latency score
    lat_score = _estimate_latency_score(producer, consumer)

    # 4. Risk (from policy engine; lower raw risk → higher score)
    raw_risk  = policy_engine.quick_risk(producer, consumer)  # 0-1
    risk_score = 1.0 - raw_risk                                # invert for composite

    # 5. History
    hist_score = history.success_rate(producer.protocol.value, consumer.protocol.value)

    # Confidence penalty for low-confidence contracts
    conf_penalty = min(producer.contract.confidence, consumer.contract.confidence)

    composite = conf_penalty * (
        weights.get("semantic_similarity", 0.40) * sem_sim +
        weights.get("protocol_compat",     0.20) * proto_compat +
        weights.get("latency_score",       0.15) * lat_score +
        weights.get("risk_score",          0.15) * risk_score +
        weights.get("history_score",       0.10) * hist_score
    )

    return MatchScore(
        semantic_similarity=sem_sim,
        protocol_compat=proto_compat,
        latency_score=lat_score,
        risk_score=risk_score,
        history_score=hist_score,
        composite=composite,
    )


# ─────────────────────────────────────────────
# Priority Queue
# ─────────────────────────────────────────────

class BridgeQueue:
    """
    Max-heap priority queue ordered by composite score.

    Improvement #5 — Score decay:
    Candidates sitting in the queue accrue staleness.  When popped, a decay
    factor of 0.95^(minutes_waiting) is applied.  Candidates that decay below
    the minimum threshold are silently discarded rather than being passed to
    the policy phase, keeping the queue clean after ecosystem changes.
    """

    DECAY_RATE      = 0.95     # per minute waiting
    MIN_AFTER_DECAY = 0.40     # discard below this after decay (below hard threshold)

    def __init__(self) -> None:
        import heapq
        self._heap:       list = []
        self._lock              = asyncio.Lock()
        self._counter:    int  = 0
        # Dedup set: (producer_id, consumer_id) pairs currently in the heap.
        # Prevents _match_all from flooding the queue with duplicates on every tick.
        self._in_flight:  set  = set()

    async def push(self, candidate: BridgeCandidate) -> None:
        import heapq
        pair = (candidate.producer_id, candidate.consumer_id)
        async with self._lock:
            if pair in self._in_flight:
                return   # same pair already queued — skip duplicate
            heapq.heappush(self._heap, (
                -candidate.score.composite,
                self._counter,
                candidate,
            ))
            self._in_flight.add(pair)
            self._counter += 1

    async def pop(self) -> Optional[BridgeCandidate]:
        """
        Pop the highest-scored non-stale candidate.
        Applies time-based decay; discards candidates that decay below MIN_AFTER_DECAY.
        Clears the in_flight entry whether the candidate is returned or discarded.
        """
        import heapq
        async with self._lock:
            while self._heap:
                _, _, candidate = heapq.heappop(self._heap)
                pair = (candidate.producer_id, candidate.consumer_id)
                self._in_flight.discard(pair)   # free slot regardless of decay outcome

                minutes_waiting = (time.time() - candidate.created_at) / 60.0
                decay_factor    = self.DECAY_RATE ** minutes_waiting
                decayed_score   = candidate.score.composite * decay_factor

                if decayed_score < self.MIN_AFTER_DECAY:
                    logger.debug(
                        "QUEUE_DECAY dropped %s->%s (score=%.3f decayed=%.3f wait=%.1fm)",
                        candidate.producer_id, candidate.consumer_id,
                        candidate.score.composite, decayed_score, minutes_waiting,
                    )
                    continue   # discard and try next

                if decay_factor < 0.999:
                    candidate.score.composite = decayed_score
                    logger.debug(
                        "QUEUE_DECAY applied %s->%s: %.3f -> %.3f (%.1fm)",
                        candidate.producer_id, candidate.consumer_id,
                        decayed_score / decay_factor, decayed_score, minutes_waiting,
                    )
                return candidate
            return None

    async def repush(self, candidate: BridgeCandidate) -> None:
        """Re-queue with a fresh created_at (resets decay) and clears in_flight first."""
        import dataclasses
        # Must clear in_flight so push() accepts the re-entry
        async with self._lock:
            self._in_flight.discard((candidate.producer_id, candidate.consumer_id))
        refreshed = dataclasses.replace(candidate, created_at=time.time())
        await self.push(refreshed)

    def contains(self, producer_id: str, consumer_id: str) -> bool:
        """O(1) check: is this pair already queued?"""
        return (producer_id, consumer_id) in self._in_flight

    def empty(self) -> bool:
        return len(self._heap) == 0

    def __len__(self) -> int:
        return len(self._heap)


# ─────────────────────────────────────────────
# Matching Phase
# ─────────────────────────────────────────────

class MatchingPhase:
    """
    Triggers on NEW_NODE, CONTRACT_DRIFT events, and every matching_interval_ms.
    Scores all unconnected producer/consumer pairs and pushes viable candidates.
    """

    def __init__(
        self,
        registry:      NodeRegistry,
        graph:         CapabilityGraph,
        bridge_queue:  BridgeQueue,
        active_bridges: Dict,
        event_bus:     Any,
        ml_matcher:    Any,
        policy_engine: Any,
    ) -> None:
        self._registry       = registry
        self._graph          = graph
        self._bridge_queue   = bridge_queue
        self._active_bridges = active_bridges
        self._bus            = event_bus
        self._matcher        = ml_matcher
        self._policy         = policy_engine
        self._history        = BridgeHistory()
        self._running        = False
        # Subscribe to node events
        self._event_queue    = event_bus.subscribe(EventType.NEW_NODE)
        self._drift_queue    = event_bus.subscribe(EventType.CONTRACT_DRIFT)

    async def run(self) -> None:
        self._running = True
        logger.info("MatchingPhase started")
        # Run event-driven and periodic loops concurrently
        await asyncio.gather(
            self._event_loop(),
            self._periodic_loop(),
        )

    async def stop(self) -> None:
        self._running = False

    async def _event_loop(self) -> None:
        """Poll both event queues concurrently so neither blocks the other."""
        while self._running:
            results = await asyncio.gather(
                self._bus.next_event(self._event_queue, timeout=0.1),
                self._bus.next_event(self._drift_queue,  timeout=0.1),
                return_exceptions=True,
            )
            if any(r and not isinstance(r, Exception) for r in results):
                await self._match_all()

    async def _periodic_loop(self) -> None:
        while self._running:
            await asyncio.sleep(settings.matching_interval_ms / 1000.0)
            await self._match_all()

    async def _match_all(self) -> None:
        producers = self._registry.producers()
        consumers = self._registry.consumers()

        if not producers or not consumers:
            return

        weights = settings.score_weights

        # ── Semantic pre-filter ────────────────────────────────────────────────
        # Before scoring all O(n²) pairs, skip pairs that are obviously
        # incompatible — this reduces calls to the (expensive) score_pair.
        #
        # Pre-filter rules (all O(1) per pair):
        #   1. Protocol hard-incompatible (not in compat matrix)
        #   2. Tag overlap: if both have semantic_tags and share zero tags, skip
        #   3. Confidence too low: producer or consumer contract confidence < 0.2
        #
        # Pre-filter is conservative — when in doubt, score_pair is called.
        from ..config import PROTOCOL_COMPAT_MATRIX
        _compat = PROTOCOL_COMPAT_MATRIX

        def _pre_filter(p, c) -> bool:
            """Return True to SKIP this pair (obviously incompatible)."""
            # Rule 1: protocol hard-incompatible
            p_targets = _compat.get(p.protocol.value, [])
            if p_targets and c.protocol.value not in p_targets:
                return True   # skip

            # Rule 2: zero tag overlap when both have non-empty tags
            p_tags = set(p.contract.semantic_tags or [])
            c_tags = set(c.contract.semantic_tags or [])
            if p_tags and c_tags and not (p_tags & c_tags):
                # Allow a small exception: generic/process/socket tags are broad
                broad = {'generic', 'socket', 'process', 'data', 'json', 'stream'}
                if not (p_tags & broad) and not (c_tags & broad):
                    return True   # skip — no semantic overlap

            # Rule 3: contract confidence too low to be useful
            if (p.contract.confidence or 1.0) < 0.2:
                return True
            if (c.contract.confidence or 1.0) < 0.2:
                return True

            return False   # do not skip — proceed to full scoring

        skipped = 0
        scored  = 0

        for producer in producers:
            for consumer in consumers:
                if producer.id == consumer.id:
                    continue
                key = (producer.id, consumer.id)
                if key in self._active_bridges:
                    continue
                # Skip pairs already sitting in the queue (O(1) via in_flight set)
                if self._bridge_queue.contains(producer.id, consumer.id):
                    continue

                # Pre-filter: skip obviously incompatible pairs
                if _pre_filter(producer, consumer):
                    skipped += 1
                    continue

                scored += 1
                score = score_pair(
                    producer, consumer,
                    self._matcher, self._history, self._policy, weights
                )

                if score.composite >= settings.min_composite_score:
                    # Add potential edge to graph
                    self._graph.add_edge(producer.id, consumer.id, score.composite)

                    candidate = BridgeCandidate(
                        producer_id=producer.id,
                        consumer_id=consumer.id,
                        score=score,
                    )
                    await self._bridge_queue.push(candidate)
                    logger.debug(
                        "CANDIDATE %s→%s  score=%.3f  "
                        "sem=%.2f proto=%.2f lat=%.2f risk=%.2f hist=%.2f",
                        producer.id, consumer.id, score.composite,
                        score.semantic_similarity,
                        score.protocol_compat,
                        score.latency_score,
                        score.risk_score,
                        score.history_score,
                    )

        if skipped > 0 or scored > 0:
            logger.debug(
                "MATCHING  pairs_scored=%d  pre_filtered=%d  total=%d",
                scored, skipped, scored + skipped,
            )

        # ── Fan-out detection ──────────────────────────────────────────────────
        # Log when one producer has multiple consumers queued — signals FanOutRunner
        # opportunity for the synthesis phase to create parallel bridges.
        producer_queue_counts: dict = {}
        for (pid, _cid) in list(self._bridge_queue._in_flight):
            producer_queue_counts[pid] = producer_queue_counts.get(pid, 0) + 1
        for pid, count in producer_queue_counts.items():
            if count >= 2:
                logger.info(
                    "FAN-OUT OPPORTUNITY  producer=%s  consumers_queued=%d",
                    pid[:12], count,
                )

    def record_bridge_outcome(
        self, producer_protocol: str, consumer_protocol: str, success: bool
    ) -> None:
        self._history.record(producer_protocol, consumer_protocol, success)
