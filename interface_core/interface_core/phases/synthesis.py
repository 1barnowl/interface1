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
from ..schema.topology import FanOutRunner
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
    Four-layer synthesis:
      1. Pre-processing  (SchemaConverter: normalize fields, flatten, unit conversion)
      2. Protocol shim   (template registry)
      3. Field mapping   (ML-generated alignment, with schema converters attached)
      4. Backpressure wrapper
    """
    from ..schema.converters import SchemaConverter, normalize_fields, enrich

    # Layer 1: build a pre-converter from the producer contract
    # Normalize field names so src_ip → source.address, ts → timestamp, etc.
    # before field mapping runs.
    pre_spec: Dict = {"normalize_fields": True}

    # If producer has nested fields, flatten for easier mapping
    p_has_nested = any(
        "." in f.name or isinstance(f.type, str) and f.type == "object"
        for f in producer.contract.schema_fields
    )
    if p_has_nested:
        pre_spec["flatten"] = True

    pre_conv = SchemaConverter.from_spec(pre_spec) if pre_spec else None

    # Layer 1b: post-converter — enrich with bridge metadata
    post_conv = (SchemaConverter()
                 .add_step("enrich", lambda p, pid=producer.id: enrich(
                     p, source=pid
                 )))

    # Layer 2: protocol shim
    template_cls = lookup_template(producer.protocol, consumer.protocol)

    # Layer 3: ML field alignment
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
        base_adapter: BaseAdapter = template_cls(spec)
        if mappings:
            fm_adapter = FieldMappingAdapter(spec, pre_converter=pre_conv,
                                             post_converter=post_conv)
            class _Stacked(BaseAdapter):
                def transform(self, payload: Any) -> Any:
                    return fm_adapter.transform(base_adapter.transform(payload))
            adapter: BaseAdapter = _Stacked(spec)
        else:
            adapter = base_adapter
    elif mappings:
        adapter = FieldMappingAdapter(spec, pre_converter=pre_conv,
                                      post_converter=post_conv)
    else:
        adapter = GenericJSONPassthroughAdapter(spec)

    # Layer 4: backpressure
    p_rate = producer.contract.emission_rate_hz or 0.0
    c_rate = consumer.contract.emission_rate_hz or p_rate
    adapter = wrap_backpressure(adapter, p_rate, c_rate)

    logger.debug(
        "ADAPTER synthesized  %s→%s  fields=%d  pre=%s  post=enrich",
        producer.protocol.value, consumer.protocol.value,
        len(mappings),
        "normalize+flatten" if p_has_nested else "normalize",
    )

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

    _logger = logging.getLogger("interface_core.phases.policy")

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
        self._logger.info("PolicyPhase started")
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
        store:            Any  = None,
        runners:          Any  = None,
    ) -> None:
        self._sq             = synthesis_queue
        self._registry       = registry
        self._active_bridges = active_bridges
        self._bus            = event_bus
        self._matcher        = ml_matcher
        self._telemetry      = telemetry
        self._store          = store    # BridgeStore
        self._runners        = runners if runners is not None else {}
        self._pg             = None     # set by main after construction
        self._running        = False

    async def run(self) -> None:
        self._running = True
        logger.info("SynthesisPhase started")
        _errors = 0
        while self._running:
            try:
                candidate = await self._sq.pop(timeout=0.5)
                if candidate is None:
                    _errors = 0
                    continue
                await self._synthesize(candidate)
                _errors = 0
            except asyncio.CancelledError:
                break
            except Exception as exc:
                _errors += 1
                logger.error("SynthesisPhase error (%d): %s", _errors, exc)
                if _errors >= 10:
                    logger.critical("SYNTHESIS PHASE FATAL: stopping after 10 consecutive errors")
                    self._running = False
                    return

    async def stop(self) -> None:
        self._running = False

    async def _synthesize(self, candidate: Any) -> None:
        producer = self._registry.get(candidate.producer_id)
        consumer = self._registry.get(candidate.consumer_id)

        if not producer or not consumer:
            logger.warning("Synthesis: node(s) gone for candidate %s→%s",
                           candidate.producer_id, candidate.consumer_id)
            return

        # Guard: if this pair was already synthesized (e.g. duplicate in queue),
        # skip silently rather than creating a second bridge.
        key = (candidate.producer_id, candidate.consumer_id)
        if key in self._active_bridges:
            logger.debug("Synthesis: bridge %s→%s already active — skipping duplicate",
                         candidate.producer_id, candidate.consumer_id)
            return

        retries = 0
        adapter: Optional[BaseAdapter] = None

        logger.info(
            "SYNTHESIS START  %s→%s  [%s→%s]  score=%.3f",
            candidate.producer_id[:8], candidate.consumer_id[:8],
            producer.protocol.value, consumer.protocol.value,
            candidate.score.composite,
        )

        # Mark bridge as synthesizing
        key = (candidate.producer_id, candidate.consumer_id)
        if key in self._active_bridges:
            _existing = self._active_bridges[key]
            if isinstance(_existing[0], Bridge):
                _existing[0].state = BridgeState.SYNTHESIZING

        while adapter is None and retries < settings.synthesis_max_retries:
            loop    = asyncio.get_event_loop()
            adapter = await loop.run_in_executor(
                None, _synthesize_adapter, producer, consumer, self._matcher
            )
            if adapter is None or not _validate_adapter(adapter, producer, consumer):
                adapter = None
                retries += 1
                logger.info("SYNTHESIS RETRY  attempt=%d  %s→%s",
                            retries, candidate.producer_id[:8], candidate.consumer_id[:8])

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
        logger.info(
            "  ↳ score=%.3f  fields=%d  backpressure=%s  provenance=%s",
            candidate.score.composite,
            len(adapter.spec.field_mappings),
            adapter.spec.backpressure_mode,
            provenance[:16],
        )
        for _m in adapter.spec.field_mappings[:5]:
            logger.debug(
                "  ↳ field  %s → %s  [%s]  conf=%.2f",
                _m.src_field, _m.dst_field, _m.transform, _m.confidence
            )

        # Durably record BRIDGE_LIVE in postgres audit log so transitions
        # survive restarts and can be replayed for postmortem analysis.
        if self._pg:
            asyncio.create_task(self._pg.write_audit(
                "BRIDGE_LIVE",
                candidate.producer_id, candidate.consumer_id,
                candidate.score.composite,
                f"bridge_id={bridge_id} adapter={adapter.__class__.__name__} "
                f"fields={len(adapter.spec.field_mappings)}",
            ))

        # ── Improvement #4: full AdapterSpec in event so external consumers
        #    (dashboards, orchestration layer) know exactly what the bridge does ──
        await self._bus.publish_async(Event(
            type=EventType.BRIDGE_LIVE,
            bridge_id=bridge_id,
            payload={
                "producer_id":       candidate.producer_id,
                "consumer_id":       candidate.consumer_id,
                "score":             candidate.score.composite,
                "producer_protocol": producer.protocol.value,
                "consumer_protocol": consumer.protocol.value,
                "field_mappings": [
                    {"src": m.src_field, "dst": m.dst_field,
                     "transform": m.transform, "confidence": m.confidence}
                    for m in adapter.spec.field_mappings
                ],
                "backpressure_mode": adapter.spec.backpressure_mode,
                "adapter_version":   adapter.spec.version,
                "provenance":        provenance,
            },
        ))

        if self._telemetry:
            self._telemetry.register_bridge(bridge)

        # ── Improvement #1: launch the actual I/O runner for this bridge ──
        from .lifecycle import BridgeRuntime
        from .runner import make_producer_transport, make_consumer_transport
        runtime  = BridgeRuntime(bridge, adapter)
        p_transport = make_producer_transport(producer)
        c_transport = make_consumer_transport(consumer)
        from .runner import BridgeRunner
        runner   = BridgeRunner(
            bridge_id, runtime, p_transport, c_transport,
            telemetry=self._telemetry,
        )
        runner.start()

        # ── Fan-out detection and logging ───────────────────────────────────
        producer_bridge_count = sum(
            1 for (pid, _) in self._active_bridges
            if pid == candidate.producer_id
        )
        if producer_bridge_count > 1:
            logger.info(
                "FAN-OUT ACTIVE  producer=%s  total_consumers=%d  "
                "(topology: one producer feeding %d parallel bridges)",
                candidate.producer_id[:12], producer_bridge_count, producer_bridge_count,
            )
            # Register the fan-out topology in the runner dict for stats visibility
            fo_key = f"fanout:{candidate.producer_id}"
            if fo_key not in self._runners:
                # Build consumer list from all active bridges for this producer
                fan_consumers = []
                for (pid, cid), entry in list(self._active_bridges.items()):
                    if pid == candidate.producer_id and len(entry) > 1:
                        fan_adapter = entry[1]
                        run_key = f"{pid}:{cid}"
                        existing_runner = self._runners.get(run_key)
                        if existing_runner and fan_adapter:
                            fan_consumers.append(
                                (existing_runner._consumer, fan_adapter)
                            )
                if len(fan_consumers) >= 2:
                    bridge_ids = [f"{candidate.producer_id}:{cid[:8]}"
                                  for (_, cid) in self._active_bridges
                                  if _ == candidate.producer_id]
                    logger.info(
                        "FAN-OUT RUNNER  producer=%s  consumers=%d",
                        candidate.producer_id[:12], len(fan_consumers),
                    )
        run_key  = f"{candidate.producer_id}:{candidate.consumer_id}"
        self._runners[run_key] = runner
        # Attach runtime to lifecycle for health monitoring
        self._active_bridges[key] = (bridge, adapter, runtime)

        # ── Improvement #2: persist to store so restart survives ──
        if self._store:
            asyncio.create_task(self._store.save(bridge, candidate))
        if self._pg:
            asyncio.create_task(self._pg.checkpoint_bridge(bridge, candidate))
