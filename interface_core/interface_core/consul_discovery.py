"""
Interface Core — Consul Service Discovery
Queries Consul's catalog API to auto-discover registered services as nodes.

On every discovery cycle:
  1. GET /v1/catalog/services  → list of service names
  2. GET /v1/catalog/service/<name>  → instances with address, port, tags, meta
  3. Probe each instance for protocol fingerprint (REST/gRPC/MQTT/etc.)
  4. Feed inferred Node into the main NodeRegistry via the existing
     _register_or_drift() pathway

This means any service that self-registers with Consul — regardless of language
or framework — is automatically discovered and eligible for bridging.

Requires: aiohttp (already in requirements for REST probing)
Falls back gracefully when Consul is unreachable.
"""

from __future__ import annotations
import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from .config import settings
from .models import DataContract, FieldSchema, NodeRole, Protocol

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Protocol fingerprinter
# ─────────────────────────────────────────────

def _fingerprint_protocol(
    tags: List[str], meta: Dict[str, str], port: int
) -> Tuple[Protocol, str]:
    """
    Infer protocol + endpoint from Consul service tags and meta.

    Convention (matches common Consul registration patterns):
      tags: ["grpc"]          → Protocol.GRPC
      tags: ["mqtt"]          → Protocol.MQTT
      tags: ["kafka"]         → Protocol.KAFKA
      tags: ["websocket"]     → Protocol.WEBSOCKET
      meta: {"protocol": "x"} → wins over tags
      default                  → Protocol.REST (HTTP)
    """
    tags_lower = {t.lower() for t in (tags or [])}
    proto_meta = (meta or {}).get("protocol", "").lower()

    raw = proto_meta or next(
        (t for t in ["grpc", "mqtt", "kafka", "websocket", "amqp", "zeromq"]
         if t in tags_lower),
        "rest",
    )

    proto_map = {
        "grpc":      Protocol.GRPC,
        "mqtt":      Protocol.MQTT,
        "kafka":     Protocol.KAFKA,
        "websocket": Protocol.WEBSOCKET,
        "amqp":      Protocol.AMQP,
        "zeromq":    Protocol.ZEROMQ,
        "rest":      Protocol.REST,
        "http":      Protocol.REST,
    }
    protocol = proto_map.get(raw, Protocol.REST)

    # Build endpoint string
    address = meta.get("address") if meta else None
    scheme_map = {
        Protocol.GRPC:      "grpc",
        Protocol.MQTT:      "mqtt",
        Protocol.KAFKA:     "kafka",
        Protocol.WEBSOCKET: "ws",
        Protocol.AMQP:      "amqp",
        Protocol.ZEROMQ:    "tcp",
        Protocol.REST:      "http",
    }
    scheme   = scheme_map.get(protocol, "http")
    host     = address or "localhost"
    endpoint = f"{scheme}://{host}:{port}"

    return protocol, endpoint


def _contract_from_consul_meta(
    service_name: str,
    tags: List[str],
    meta: Dict[str, str],
) -> DataContract:
    """
    Build a DataContract from Consul service metadata.

    Fields come from meta keys matching 'field.*':
      meta["field.price"]   = "float"
      meta["field.symbol"]  = "str"
    Tags become semantic_tags.
    Emission rate from meta["rate_hz"] if present.
    """
    fields: List[FieldSchema] = []
    for key, val in (meta or {}).items():
        if key.startswith("field."):
            field_name = key[6:]   # strip "field."
            fields.append(FieldSchema(name=field_name, type=val or "object"))

    if not fields:
        # Generic contract from service name
        fields = [FieldSchema(name="data", type="object")]

    # Semantic tags from Consul tags (strip protocol tag, keep domain tags)
    protocol_tags = {"grpc", "mqtt", "kafka", "websocket", "amqp", "zeromq", "rest", "http"}
    semantic_tags = [t for t in (tags or []) if t.lower() not in protocol_tags]
    if not semantic_tags:
        semantic_tags = [service_name.lower().replace("-", "_")]

    rate = None
    try:
        rate = float(meta.get("rate_hz", "")) if meta else None
    except (ValueError, TypeError):
        pass

    role_meta = (meta or {}).get("role", "both").lower()
    confidence = float((meta or {}).get("confidence", "0.85"))

    return DataContract(
        schema_fields    = fields,
        semantic_tags    = semantic_tags,
        encoding         = (meta or {}).get("encoding", "json"),
        emission_rate_hz = rate,
        confidence       = min(1.0, max(0.1, confidence)),
    )


def _role_from_meta(meta: Dict[str, str]) -> NodeRole:
    role_str = (meta or {}).get("role", "both").lower()
    return {
        "producer": NodeRole.PRODUCER,
        "consumer": NodeRole.CONSUMER,
        "both":     NodeRole.BOTH,
    }.get(role_str, NodeRole.BOTH)


# ─────────────────────────────────────────────
# Consul HTTP client (minimal, no dependency)
# ─────────────────────────────────────────────

class ConsulClient:
    """
    Minimal async Consul HTTP client.
    Uses aiohttp if available; falls back to urllib.
    """

    def __init__(self, host: str, port: int) -> None:
        self._base    = f"http://{host}:{port}/v1"
        self._session = None

    async def _get(self, path: str) -> Optional[Any]:
        url = self._base + path
        try:
            import aiohttp
            if self._session is None:
                self._session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=3)
                )
            async with self._session.get(url) as r:
                if r.status == 200:
                    return await r.json()
        except ImportError:
            # urllib fallback
            import json, urllib.request
            loop = asyncio.get_event_loop()
            def _fetch():
                with urllib.request.urlopen(url, timeout=3) as r:
                    return json.loads(r.read())
            try:
                return await loop.run_in_executor(None, _fetch)
            except Exception:
                return None
        except Exception as exc:
            logger.debug("Consul GET %s failed: %s", path, exc)
            return None

    async def catalog_services(self) -> Dict[str, List[str]]:
        """Returns {service_name: [tag, ...]}"""
        result = await self._get("/catalog/services")
        return result or {}

    async def catalog_service(self, name: str) -> List[Dict]:
        """Returns list of service instances."""
        result = await self._get(f"/catalog/service/{name}")
        return result or []

    async def health_passing(self, name: str) -> List[Dict]:
        """Returns only healthy instances."""
        result = await self._get(f"/health/service/{name}?passing=true")
        return result or []

    async def close(self) -> None:
        if self._session:
            try:
                await self._session.close()
            except Exception:
                pass
            self._session = None


# ─────────────────────────────────────────────
# ConsulDiscovery scanner
# ─────────────────────────────────────────────

class ConsulDiscovery:
    """
    Polls Consul every discovery_interval_ms and registers/updates nodes
    for every healthy service found in the catalog.

    Integrates with the main DiscoveryPhase by calling register_or_drift
    through the shared registry + event bus.
    """

    def __init__(
        self,
        host:        str,
        port:        int,
        registry:    Any,
        graph:       Any,
        drift_log:   Any,
        event_bus:   Any,
        ml_matcher:  Any,
        pg:          Any = None,
    ) -> None:
        self._client     = ConsulClient(host, port)
        self._registry   = registry
        self._graph      = graph
        self._drift_log  = drift_log
        self._bus        = event_bus
        self._matcher    = ml_matcher
        self._pg         = pg
        self._running    = False
        self._known:     Dict[str, float] = {}   # node_id → last_seen

    async def run(self) -> None:
        self._running = True
        host_port = self._client._base.split("//")[1].split("/")[0]
        parts     = host_port.rsplit(":", 1)
        host_str  = parts[0]
        port_str  = parts[1] if len(parts) > 1 else "?"
        logger.info("ConsulDiscovery started (host=%s port=%s)", host_str, port_str)
        while self._running:
            try:
                await self._scan()
            except Exception as exc:
                logger.debug("ConsulDiscovery scan error: %s", exc)
            await asyncio.sleep(settings.discovery_interval_ms / 1000.0)

    async def stop(self) -> None:
        self._running = False
        await self._client.close()

    async def _scan(self) -> None:
        services = await self._client.catalog_services()
        if not services:
            return

        # Filter out Consul's own service
        service_names = [s for s in services if s != "consul"]

        for name in service_names:
            instances = await self._client.health_passing(name)
            for inst in instances:
                await self._process_instance(name, inst)

    async def _process_instance(self, service_name: str, inst: Dict) -> None:
        """Convert one Consul service instance into a registered node."""
        try:
            # Consul health API nests differently than catalog API
            service = inst.get("Service", inst)
            address = service.get("Address") or inst.get("Node", {}).get("Address", "127.0.0.1")
            port    = service.get("Port", 80)
            tags    = service.get("Tags") or []
            meta    = service.get("Meta") or {}

            protocol, endpoint = _fingerprint_protocol(tags, meta, port)
            contract            = _contract_from_consul_meta(service_name, tags, meta)
            role                = _role_from_meta(meta)

            # Use the discovery phase helper to register/update
            from .phases.discovery import _make_node_id, _infer_role_from_contract
            from .models import Node, ContractSnapshot, Event, EventType
            import time as _t

            node_id    = _make_node_id(protocol, endpoint)
            embedding  = await asyncio.get_event_loop().run_in_executor(
                None, self._matcher.embed, contract
            )
            contract_fp = contract.fingerprint()

            self._known[node_id] = _t.time()

            if not self._registry.contains(node_id):
                node = Node(
                    id        = node_id,
                    protocol  = protocol,
                    role      = role,
                    endpoint  = endpoint,
                    contract  = contract,
                    embedding = embedding,
                    metadata  = {
                        "consul_service": service_name,
                        "consul_tags":    tags,
                        "consul_meta":    meta,
                    },
                )
                self._registry.put(node)
                self._graph.add_node(node_id)

                snap = ContractSnapshot(
                    node_id=node_id, fingerprint=contract_fp,
                    contract=contract, delta=0.0,
                )
                self._drift_log.record(snap)
                if self._pg:
                    asyncio.ensure_future(self._pg.record_contract_snapshot(snap))

                logger.info(
                    "CONSUL_NODE %s [%s] %s (service=%s)",
                    node_id, protocol.value, endpoint, service_name
                )
                await self._bus.publish_async(Event(
                    type=EventType.NEW_NODE,
                    node_id=node_id,
                    payload={
                        "protocol":       protocol.value,
                        "endpoint":       endpoint,
                        "consul_service": service_name,
                    },
                ))
            else:
                # Touch last_seen
                self._registry.touch(node_id)

        except Exception as exc:
            logger.debug("ConsulDiscovery._process_instance error: %s", exc)
