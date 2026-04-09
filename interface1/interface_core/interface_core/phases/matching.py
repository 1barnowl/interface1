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
    """Max-heap priority queue ordered by composite score."""

    def __init__(self) -> None:
        import heapq
        self._heap:  list  = []
        self._lock         = asyncio.Lock()
        self._counter: int = 0   # tiebreaker

    async def push(self, candidate: BridgeCandidate) -> None:
        import heapq
        async with self._lock:
            # Negate score for max-heap via min-heap
            heapq.heappush(self._heap, (
                -candidate.score.composite,
                self._counter,
                candidate,
            ))
            self._counter += 1

    async def pop(self) -> Optional[BridgeCandidate]:
        import heapq
        async with self._lock:
            if not self._heap:
                return None
            _, _, candidate = heapq.heappop(self._heap)
            return candidate

    async def repush(self, candidate: BridgeCandidate) -> None:
        await self.push(candidate)

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
        while self._running:
            for q in [self._event_queue, self._drift_queue]:
                event = await self._bus.next_event(q, timeout=0.1)
                if event:
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

        for producer in producers:
            for consumer in consumers:
                if producer.id == consumer.id:
                    continue
                key = (producer.id, consumer.id)
                if key in self._active_bridges:
                    continue

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
                        "CANDIDATE %s→%s score=%.3f",
                        producer.id, consumer.id, score.composite
                    )

    def record_bridge_outcome(
        self, producer_protocol: str, consumer_protocol: str, success: bool
    ) -> None:
        self._history.record(producer_protocol, consumer_protocol, success)
