"""
Interface Core — Discovery Phase
Scans the ecosystem every 500 ms for new / changed nodes.
Supports: REST, gRPC reflection, Kafka, MQTT, ZeroMQ, file paths, raw sockets, processes.
"""

from __future__ import annotations
import asyncio
import hashlib
import json
import logging
import os
import socket
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from ..config import settings
from ..models import (
    DataContract, Event, EventType, FieldSchema,
    Node, NodeRole, Protocol, ContractSnapshot
)
from ..registry import CapabilityGraph, DriftLog, NodeRegistry

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Contract inference helpers
# ─────────────────────────────────────────────

def _infer_role_from_contract(contract: DataContract) -> NodeRole:
    """Heuristic: streaming nodes are producers; endpoints without rate are consumers."""
    if contract.emission_rate_hz is not None and contract.emission_rate_hz > 0:
        return NodeRole.PRODUCER
    return NodeRole.BOTH


def _fields_from_json_sample(sample: Any, prefix: str = "") -> List[FieldSchema]:
    """Recursively flatten a JSON object into FieldSchema list."""
    fields: List[FieldSchema] = []
    if isinstance(sample, dict):
        for k, v in sample.items():
            name = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                fields.extend(_fields_from_json_sample(v, prefix=name))
            elif isinstance(v, list):
                fields.append(FieldSchema(name=name, type="array", nullable=True))
            elif isinstance(v, bool):
                fields.append(FieldSchema(name=name, type="bool"))
            elif isinstance(v, int):
                fields.append(FieldSchema(name=name, type="int"))
            elif isinstance(v, float):
                fields.append(FieldSchema(name=name, type="float"))
            elif v is None:
                fields.append(FieldSchema(name=name, type="str", nullable=True))
            else:
                fields.append(FieldSchema(name=name, type="str"))
    return fields or [FieldSchema(name="data", type="object")]


def _semantic_tags_from_fields(fields: List[FieldSchema]) -> List[str]:
    """Guess semantic domain from field names."""
    names = " ".join(f.name.lower() for f in fields)
    tags: List[str] = []
    if any(w in names for w in ["lat", "lon", "alt", "altitude", "gps", "position"]):
        tags.append("geospatial")
    if any(w in names for w in ["price", "bid", "ask", "volume", "trade", "order"]):
        tags.append("market")
    if any(w in names for w in ["sensor", "temperature", "humidity", "pressure", "imu"]):
        tags.append("telemetry")
    if any(w in names for w in ["user", "username", "email", "account", "auth", "token"]):
        tags.append("identity")
    if any(w in names for w in ["timestamp", "time", "ts", "created_at", "updated_at"]):
        tags.append("time-series")
    if any(w in names for w in ["log", "level", "message", "error", "trace"]):
        tags.append("observability")
    if any(w in names for w in ["frame", "seq", "mavlink", "sys_id", "comp_id"]):
        tags.append("drone")
    return tags or ["generic"]


# ─────────────────────────────────────────────
# Per-protocol scanners
# ─────────────────────────────────────────────

async def _probe_rest(url: str) -> Optional[DataContract]:
    """Probe a REST endpoint: try /openapi.json, /, OPTIONS."""
    try:
        import aiohttp  # type: ignore
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=3)
        ) as session:
            # 1. Try OpenAPI spec
            for spec_path in ["/openapi.json", "/swagger.json", "/api/openapi.json"]:
                try:
                    async with session.get(url.rstrip("/") + spec_path) as r:
                        if r.status == 200:
                            spec = await r.json(content_type=None)
                            fields = []
                            if "components" in spec and "schemas" in spec["components"]:
                                for schema_name, schema_def in list(
                                    spec["components"]["schemas"].items()
                                )[:3]:
                                    for prop, prop_def in schema_def.get("properties", {}).items():
                                        fields.append(FieldSchema(
                                            name=prop,
                                            type=prop_def.get("type", "object"),
                                        ))
                            return DataContract(
                                schema_fields=fields or [FieldSchema(name="data", type="object")],
                                semantic_tags=["rest-api"],
                                encoding="json",
                                confidence=0.9,
                            )
                except Exception:
                    pass

            # 2. Probe root and sample payload
            async with session.get(url, allow_redirects=True) as r:
                if r.status == 200:
                    try:
                        body = await r.json(content_type=None)
                        fields = _fields_from_json_sample(body)
                        tags   = _semantic_tags_from_fields(fields)
                        return DataContract(
                            schema_fields=fields,
                            semantic_tags=tags,
                            encoding="json",
                            confidence=0.7,
                        )
                    except Exception:
                        pass
    except Exception as exc:
        logger.debug("REST probe failed for %s: %s", url, exc)
    return None


async def _probe_socket(host: str, port: int) -> Optional[DataContract]:
    """Try TCP connect; if successful, flag as unknown socket node."""
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=1.0
        )
        writer.close()
        await writer.wait_closed()
        return DataContract(
            schema_fields=[FieldSchema(name="data", type="bytes")],
            semantic_tags=["socket"],
            encoding="binary",
            confidence=0.3,
        )
    except Exception:
        return None


async def _probe_kafka_topic(broker: str, topic: str) -> Optional[DataContract]:
    """Consume one message from a Kafka topic to infer schema."""
    try:
        from aiokafka import AIOKafkaConsumer  # type: ignore
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=broker,
            auto_offset_reset="latest",
            consumer_timeout_ms=2000,
        )
        await consumer.start()
        try:
            async for msg in consumer:
                payload = msg.value
                try:
                    body = json.loads(payload)
                    fields = _fields_from_json_sample(body)
                    tags   = _semantic_tags_from_fields(fields) + ["kafka"]
                    return DataContract(
                        schema_fields=fields,
                        semantic_tags=tags,
                        encoding="json",
                        emission_rate_hz=1.0,    # assume streaming
                        confidence=0.8,
                        raw_sample=payload.decode()[:512],
                    )
                except Exception:
                    return DataContract(
                        schema_fields=[FieldSchema(name="data", type="bytes")],
                        semantic_tags=["kafka"],
                        encoding="binary",
                        emission_rate_hz=1.0,
                        confidence=0.4,
                    )
        finally:
            await consumer.stop()
    except Exception as exc:
        logger.debug("Kafka probe failed for %s/%s: %s", broker, topic, exc)
    return None


def _probe_file(path: str) -> Optional[DataContract]:
    """Read a few lines from a file and infer schema."""
    try:
        with open(path, "r", errors="ignore") as fh:
            sample = fh.read(4096)
        # try JSON
        try:
            body = json.loads(sample)
            fields = _fields_from_json_sample(body)
            return DataContract(
                schema_fields=fields,
                semantic_tags=_semantic_tags_from_fields(fields) + ["file"],
                encoding="json",
                confidence=0.7,
            )
        except Exception:
            pass
        # treat as text / line-delimited
        lines = sample.splitlines()
        return DataContract(
            schema_fields=[FieldSchema(name="line", type="str")],
            semantic_tags=["file", "text"],
            encoding="text",
            emission_rate_hz=None,
            confidence=0.5,
        )
    except Exception:
        return None


def _make_node_id(protocol: Protocol, endpoint: str) -> str:
    key = f"{protocol.value}:{endpoint}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


# ─────────────────────────────────────────────
# Discovery Phase
# ─────────────────────────────────────────────

class DiscoveryPhase:
    """
    Runs on an asyncio loop. Every discovery_interval_ms ms:
      - Scans configured REST, Kafka, MQTT, socket targets + local processes
      - Infers DataContract for each live node
      - Registers new nodes (emits NEW_NODE)
      - Detects drift on known nodes (emits CONTRACT_DRIFT)
    """

    def __init__(
        self,
        registry: NodeRegistry,
        graph: CapabilityGraph,
        drift_log: DriftLog,
        event_bus: Any,
        ml_matcher: Any,
        pg: Any = None,
    ) -> None:
        self._registry   = registry
        self._graph      = graph
        self._drift_log  = drift_log
        self._bus        = event_bus
        self._matcher    = ml_matcher
        self._pg         = pg
        self._telemetry  = None    # set by main after construction
        self._running    = False
        # Per-source probe backoff: endpoint → last_probe_time
        # REST/gRPC/Kafka targets re-probed at most once per probe_cooldown_s
        self._last_probed: dict = {}
        self._probe_cooldown_s: float = 30.0  # re-probe interval for configured targets

    async def run(self) -> None:
        self._running = True
        logger.info("DiscoveryPhase started (interval=%dms)", settings.discovery_interval_ms)
        while self._running:
            try:
                await self._scan_cycle()
            except Exception as exc:
                logger.exception("Discovery scan error: %s", exc)
            await asyncio.sleep(settings.discovery_interval_ms / 1000.0)

    async def stop(self) -> None:
        self._running = False

    async def _scan_cycle(self) -> None:
        tasks = []

        # REST targets
        for url in settings.scan_rest_targets:
            if now - self._last_probed.get(url, 0) >= self._probe_cooldown_s:
                self._last_probed[url] = now
                tasks.append(self._handle_rest(url))

        # Kafka topics
        if settings.scan_kafka_topics:
            broker = settings.kafka_brokers
            for topic in settings.scan_kafka_topics:
                _kafka_key = f"{broker}/{topic}"
                if now - self._last_probed.get(_kafka_key, 0) >= self._probe_cooldown_s:
                    self._last_probed[_kafka_key] = now
                    tasks.append(self._handle_kafka(broker, topic))

        # TCP ports on localhost
        for port in settings.scan_socket_ports:
            tasks.append(self._handle_socket("127.0.0.1", port))

        # gRPC targets from config
        for target in getattr(settings, 'scan_grpc_targets', []):
            if now - self._last_probed.get(target, 0) >= self._probe_cooldown_s:
                self._last_probed[target] = now
                tasks.append(self._handle_grpc(target))

        # File paths from env
        for path in os.environ.get("IC_FILE_TARGETS", "").split(":"):
            if path and os.path.isfile(path):
                tasks.append(self._handle_file(path))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # ── Per-protocol handlers ────────────────

    async def _handle_rest(self, url: str) -> None:
        logger.debug("PROBE REST  %s", url)
        contract = await _probe_rest(url)
        if contract:
            logger.info(
                "PROBE REST FOUND  %s  confidence=%.2f  fields=%d  tags=%s",
                url, contract.confidence,
                len(contract.schema_fields),
                ','.join(contract.semantic_tags[:3]),
            )
        if contract:
            await self._register_or_drift(Protocol.REST, url, contract)

    async def _handle_kafka(self, broker: str, topic: str) -> None:
        logger.debug("PROBE KAFKA  %s/%s", broker, topic)
        contract = await _probe_kafka_topic(broker, topic)
        if contract:
            logger.info("PROBE KAFKA FOUND  %s/%s  rate=%.1fHz", broker, topic, contract.emission_rate_hz or 0)
        if contract:
            await self._register_or_drift(Protocol.KAFKA, f"{broker}/{topic}", contract)

    async def _handle_socket(self, host: str, port: int) -> None:
        logger.debug("PROBE SOCKET  %s:%d", host, port)
        contract = await _probe_socket(host, port)
        if contract:
            logger.info("PROBE SOCKET FOUND  %s:%d  encoding=%s", host, port, contract.encoding)
        if contract:
            await self._register_or_drift(Protocol.SOCKET, f"{host}:{port}", contract)

    async def _handle_grpc(self, endpoint: str) -> None:
        from ..phases.grpc_transport import probe_grpc_contract
        contract = await probe_grpc_contract(endpoint)
        if contract:
            await self._register_or_drift(Protocol.GRPC, endpoint, contract)

    async def _handle_file(self, path: str) -> None:
        loop = asyncio.get_event_loop()
        contract = await loop.run_in_executor(None, _probe_file, path)
        if contract:
            await self._register_or_drift(Protocol.FILE, path, contract)

    # ── Core registration / drift logic ─────

    async def _register_or_drift(
        self, protocol: Protocol, endpoint: str, contract: DataContract
    ) -> None:
        node_id  = _make_node_id(protocol, endpoint)
        embedding = await asyncio.get_event_loop().run_in_executor(
            None, self._matcher.embed, contract
        )
        contract_fp = contract.fingerprint()

        if not self._registry.contains(node_id):
            # New node
            role = _infer_role_from_contract(contract)
            node = Node(
                id=node_id,
                protocol=protocol,
                role=role,
                endpoint=endpoint,
                contract=contract,
                embedding=embedding,
            )
            self._registry.put(node)
            self._graph.add_node(node_id)

            snapshot = ContractSnapshot(
                node_id=node_id,
                fingerprint=contract_fp,
                contract=contract,
                delta=0.0,
            )
            self._drift_log.record(snapshot)
            if self._pg:
                import asyncio as _a
                _a.ensure_future(self._pg.record_contract_snapshot(snapshot))

            logger.info("NEW_NODE %s [%s] %s", node_id, protocol.value, endpoint)
            await self._bus.publish_async(Event(
                type=EventType.NEW_NODE,
                node_id=node_id,
                payload={"protocol": protocol.value, "endpoint": endpoint},
            ))
        else:
            # Check for drift
            existing = self._registry.get(node_id)
            if existing and existing.contract.fingerprint() != contract_fp:
                # Compute drift delta: fraction of fields that changed
                old_names = {f.name for f in existing.contract.schema_fields}
                new_names = {f.name for f in contract.schema_fields}
                if old_names or new_names:
                    delta = len(old_names.symmetric_difference(new_names)) / max(len(old_names | new_names), 1)
                else:
                    delta = 0.0

                if delta >= settings.contract_drift_delta:
                    existing.contract  = contract
                    existing.embedding = embedding
                    existing.last_seen = time.time()
                    self._registry.put(existing)
                    self._graph.reweight_edges(node_id)

                    snapshot = ContractSnapshot(
                        node_id=node_id,
                        fingerprint=contract_fp,
                        contract=contract,
                        delta=delta,
                    )
                    self._drift_log.record(snapshot)
                    if self._pg:
                        import asyncio as _a
                        _a.ensure_future(self._pg.record_contract_snapshot(snapshot))
                    # Record drift event in Prometheus
                    if getattr(self, '_telemetry', None):
                        self._telemetry.record_drift_event(node_id)

                    logger.info("CONTRACT_DRIFT %s delta=%.2f", node_id, delta)
                    await self._bus.publish_async(Event(
                        type=EventType.CONTRACT_DRIFT,
                        node_id=node_id,
                        payload={"delta": delta},
                    ))
                else:
                    self._registry.touch(node_id)
            else:
                self._registry.touch(node_id)

    def register_node_directly(
        self,
        protocol: Protocol,
        endpoint: str,
        contract: DataContract,
        role: Optional[NodeRole] = None,
    ) -> str:
        """
        Programmatic registration for embedded use or testing.
        Returns node_id.
        """
        node_id   = _make_node_id(protocol, endpoint)
        embedding = self._matcher.embed(contract)
        resolved_role = role or _infer_role_from_contract(contract)
        node = Node(
            id=node_id,
            protocol=protocol,
            role=resolved_role,
            endpoint=endpoint,
            contract=contract,
            embedding=embedding,
        )
        self._registry.put(node)
        self._graph.add_node(node_id)
        self._drift_log.record(ContractSnapshot(
            node_id=node_id,
            fingerprint=contract.fingerprint(),
            contract=contract,
            delta=0.0,
        ))
        self._bus.publish(Event(
            type=EventType.NEW_NODE,
            node_id=node_id,
            payload={"protocol": protocol.value, "endpoint": endpoint},
        ))
        return node_id
