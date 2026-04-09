"""
Interface Core — Synthesis Phase
Pops approved candidates, synthesizes adapters, instantiates live bridges.
"""

from __future__ import annotations
import asyncio
import hashlib
import hmac
import logging
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from ..config import settings
from ..adapters.base import (
    BaseAdapter, FieldMappingAdapter, GenericJSONPassthroughAdapter,
    lookup_template, wrap_backpressure,
)
from ..models import (
    AdapterSpec, Bridge, BridgeState, Event, EventType,
    FieldMapping, Node, Protocol
)
from ..registry import NodeRegistry
from .matching import BridgeQueue

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Synthesis helpers
# ─────────────────────────────────────────────

def _synthesize_adapter(
    producer: Node,
    consumer: Node,
    ml_matcher: Any,
) -> Optional[BaseAdapter]:
    """
    Three-layer synthesis:
      1. Protocol shim (template registry)
      2. Field mapping (ML-generated alignment)
      3. Backpressure wrapper
    """
    # Layer 1: protocol shim
    template_cls = lookup_template(producer.protocol, consumer.protocol)

    # Layer 2: ML field alignment
    mappings: List[FieldMapping] = ml_matcher.align_fields(
        producer.contract, consumer.contract
    )

    spec = AdapterSpec(
        producer_protocol=producer.protocol,
        consumer_protocol=consumer.protocol,
        field_mappings=mappings,
        backpressure_mode="token_bucket",
    )

    # Build the actual adapter object
    if template_cls is not None:
        # Use protocol-specific template; field mappings applied on top
        base_adapter: BaseAdapter = template_cls(spec)
        if mappings:
            # Stack field mapper after protocol shim
            fm_adapter = FieldMappingAdapter(spec)
            class _Stacked(BaseAdapter):
                def transform(self, payload: Any) -> Any:
                    return fm_adapter.transform(base_adapter.transform(payload))
            adapter: BaseAdapter = _Stacked(spec)
        else:
            adapter = base_adapter
    elif mappings:
        adapter = FieldMappingAdapter(spec)
    else:
        # No template, no field mappings → generic passthrough
        adapter = GenericJSONPassthroughAdapter(spec)

    # Layer 3: backpressure
    p_rate = producer.contract.emission_rate_hz or 0.0
    c_rate = consumer.contract.emission_rate_hz or p_rate
    adapter = wrap_backpressure(adapter, p_rate, c_rate)

    return adapter


def _validate_adapter(
    adapter: BaseAdapter, producer: Node, consumer: Node
) -> bool:
    """Smoke-test: run a synthetic payload through the adapter."""
    # Build a minimal sample from producer schema
    sample: Dict = {}
    for field in producer.contract.schema_fields[:5]:
        if field.type == "int":
            sample[field.name] = 42
        elif field.type == "float":
            sample[field.name] = 3.14
        elif field.type == "bool":
            sample[field.name] = True
        else:
            sample[field.name] = "test"

    try:
        result = adapter.transform(sample)
        return result is not None
    except Exception as exc:
        logger.debug("Adapter validation failed: %s", exc)
        return False


def _sign_provenance(
    producer_id: str, consumer_id: str, adapter_version: str, secret: str
) -> str:
    payload = f"{producer_id}:{consumer_id}:{adapter_version}:{time.time():.0f}"
    return hmac.new(
        secret.encode(), payload.encode(), hashlib.sha256
    ).hexdigest()


# ─────────────────────────────────────────────
# Synthesis Queue (simple asyncio queue fed by PolicyPhase)
# ─────────────────────────────────────────────

class SynthesisQueue:
    def __init__(self) -> None:
        self._q: asyncio.Queue = asyncio.Queue(maxsize=1024)

    async def push(self, candidate: Any) -> None:
        await self._q.put(candidate)

    async def pop(self, timeout: float = 1.0) -> Optional[Any]:
        try:
            return await asyncio.wait_for(self._q.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    def empty(self) -> bool:
        return self._q.empty()


# ─────────────────────────────────────────────
# Policy Phase (sits between matching queue and synthesis queue)
# ─────────────────────────────────────────────

class PolicyPhase:
    """Pops from bridge_queue, runs policy, pushes approved to synthesis_queue."""

    def __init__(
        self,
        bridge_queue:    BridgeQueue,
        synthesis_queue: SynthesisQueue,
        policy_engine:   Any,
        event_bus:       Any,
    ) -> None:
        self._bq      = bridge_queue
        self._sq      = synthesis_queue
        self._policy  = policy_engine
        self._bus     = event_bus
        self._running = False

    async def run(self) -> None:
        self._running = True
        logger.info("PolicyPhase started")
        while self._running:
            if self._bq.empty():
                await asyncio.sleep(0.05)
                continue
            candidate = await self._bq.pop()
            if candidate is None:
                continue
            await self._evaluate(candidate)

    async def stop(self) -> None:
        self._running = False

    async def _evaluate(self, candidate: Any) -> None:
        from ..models import PolicyVerdict
        result = await self._policy.evaluate(candidate)

        if result.verdict == PolicyVerdict.REJECT:
            return   # audit already written by policy engine

        if result.verdict == PolicyVerdict.STAGE:
            approved = await self._policy.request_operator_approval(
                candidate, timeout=settings.operator_approval_timeout_s
            )
            if not approved:
                return

        # APPROVE
        await self._sq.push(candidate)


# ─────────────────────────────────────────────
# Synthesis Phase
# ─────────────────────────────────────────────

class SynthesisPhase:
    """
    Pops approved candidates from synthesis_queue.
    Synthesizes + validates adapter.
    Instantiates Bridge and adds to active_bridges.
    """

    def __init__(
        self,
        synthesis_queue:  SynthesisQueue,
        registry:         NodeRegistry,
        active_bridges:   Dict,
        event_bus:        Any,
        ml_matcher:       Any,
        telemetry:        Any,
    ) -> None:
        self._sq             = synthesis_queue
        self._registry       = registry
        self._active_bridges = active_bridges
        self._bus            = event_bus
        self._matcher        = ml_matcher
        self._telemetry      = telemetry
        self._running        = False

    async def run(self) -> None:
        self._running = True
        logger.info("SynthesisPhase started")
        while self._running:
            candidate = await self._sq.pop(timeout=0.5)
            if candidate is None:
                continue
            await self._synthesize(candidate)

    async def stop(self) -> None:
        self._running = False

    async def _synthesize(self, candidate: Any) -> None:
        producer = self._registry.get(candidate.producer_id)
        consumer = self._registry.get(candidate.consumer_id)

        if not producer or not consumer:
            logger.warning("Synthesis: node(s) gone for candidate %s→%s",
                           candidate.producer_id, candidate.consumer_id)
            return

        retries = 0
        adapter: Optional[BaseAdapter] = None

        while adapter is None and retries < settings.synthesis_max_retries:
            loop    = asyncio.get_event_loop()
            adapter = await loop.run_in_executor(
                None, _synthesize_adapter, producer, consumer, self._matcher
            )
            if adapter is None or not _validate_adapter(adapter, producer, consumer):
                adapter = None
                retries += 1
                logger.debug("Synthesis attempt %d failed for %s→%s",
                             retries, candidate.producer_id, candidate.consumer_id)

        if adapter is None:
            logger.error("SYNTHESIS_FAILED %s→%s after %d retries",
                         candidate.producer_id, candidate.consumer_id, retries)
            await self._bus.publish_async(Event(
                type=EventType.SYNTHESIS_FAILED,
                payload={"producer_id": candidate.producer_id,
                         "consumer_id": candidate.consumer_id},
            ))
            return

        bridge_id = str(uuid.uuid4())[:8]
        provenance = _sign_provenance(
            candidate.producer_id, candidate.consumer_id,
            adapter.spec.version, settings.secret_key
        )

        bridge = Bridge(
            id=bridge_id,
            producer_id=candidate.producer_id,
            consumer_id=candidate.consumer_id,
            adapter=adapter.spec,
            state=BridgeState.LIVE,
            provenance_key=provenance,
        )

        key = (candidate.producer_id, candidate.consumer_id)
        self._active_bridges[key] = (bridge, adapter)

        logger.info("BRIDGE_LIVE %s  %s→%s  [%s→%s]",
                    bridge_id,
                    candidate.producer_id, candidate.consumer_id,
                    producer.protocol.value, consumer.protocol.value)

        await self._bus.publish_async(Event(
            type=EventType.BRIDGE_LIVE,
            bridge_id=bridge_id,
            payload={
                "producer_id": candidate.producer_id,
                "consumer_id": candidate.consumer_id,
                "score":       candidate.score.composite,
            },
        ))

        if self._telemetry:
            self._telemetry.register_bridge(bridge)
