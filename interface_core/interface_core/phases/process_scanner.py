"""
Interface Core — Process Scanner
Scans the local process table to auto-discover running services.

On every discovery cycle:
  1. List all processes with open TCP listening sockets (via psutil or /proc)
  2. For each listening port, attempt protocol fingerprinting:
       - Probe for HTTP/REST   (GET /)
       - Probe for gRPC        (channel_ready)
       - Probe for generic TCP (connect + banner)
  3. Skip well-known system ports (< 1024) unless explicitly included
  4. Register discovered endpoints as nodes via the existing registry

This is the "it's actively looking for bridging opportunity" feature.
Zero configuration required — any service started on the machine becomes
visible to Interface Core automatically.

Requires: psutil (pip install psutil) for cross-platform process scanning.
Falls back to /proc/net/tcp on Linux if psutil is absent.
"""

from __future__ import annotations
import asyncio
import logging
import socket
import struct
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from ..config import settings
from ..models import DataContract, FieldSchema, NodeRole, Protocol

logger = logging.getLogger(__name__)

# Ports to always skip
_SKIP_PORTS: Set[int] = {
    22,    # SSH
    25,    # SMTP
    53,    # DNS
    67, 68,# DHCP
    111,   # RPC portmapper
    123,   # NTP
    137, 138, 139, 445,  # NetBIOS/SMB
    389, 636,  # LDAP
    631,   # CUPS
    3306,  # MySQL (discovered separately via DATABASE protocol)
    5432,  # Postgres (same)
}

_GRPC_DEFAULT_PORTS   = {50051, 50052, 50053, 50054}
_HTTP_DEFAULT_PORTS   = {8000, 8080, 8081, 8443, 3000, 4000, 5000, 7000, 9000, 9001}
_KAFKA_DEFAULT_PORTS  = {9092, 9093, 9094}
_MQTT_DEFAULT_PORTS   = {1883, 8883}
_AMQP_DEFAULT_PORTS   = {5672, 5671}


# ─────────────────────────────────────────────
# Port lister
# ─────────────────────────────────────────────

def _listening_ports_psutil() -> List[Tuple[int, str]]:
    """Returns [(port, pid_name)] for all TCP LISTEN sockets."""
    try:
        import psutil
        ports = []
        for conn in psutil.net_connections(kind="tcp"):
            if conn.status == "LISTEN" and conn.laddr:
                port = conn.laddr.port
                if port < 1024 and port not in _HTTP_DEFAULT_PORTS:
                    continue
                name = ""
                if conn.pid:
                    try:
                        name = psutil.Process(conn.pid).name()
                    except Exception:
                        pass
                ports.append((port, name))
        return ports
    except Exception as exc:
        logger.debug("psutil scan failed: %s", exc)
        return []


def _listening_ports_proc() -> List[Tuple[int, str]]:
    """Linux /proc/net/tcp fallback — no dependencies."""
    ports = []
    try:
        with open("/proc/net/tcp") as f:
            for line in f.readlines()[1:]:   # skip header
                parts = line.strip().split()
                if len(parts) < 4:
                    continue
                state = parts[3]
                if state != "0A":            # 0A = TCP_LISTEN
                    continue
                local = parts[1]
                _, port_hex = local.split(":")
                port = int(port_hex, 16)
                if port < 1024 and port not in _HTTP_DEFAULT_PORTS:
                    continue
                if port in _SKIP_PORTS:
                    continue
                ports.append((port, ""))
    except Exception as exc:
        logger.debug("/proc/net/tcp read failed: %s", exc)
    return ports


def list_listening_ports() -> List[Tuple[int, str]]:
    """Returns [(port, process_name)] — tries psutil first, /proc fallback."""
    ports = _listening_ports_psutil()
    if not ports:
        ports = _listening_ports_proc()
    return [(p, n) for p, n in ports if p not in _SKIP_PORTS]


# ─────────────────────────────────────────────
# Protocol fingerprinter
# ─────────────────────────────────────────────

async def _probe_http(host: str, port: int) -> Optional[DataContract]:
    """Try HTTP GET / and parse the response."""
    url = f"http://{host}:{port}"
    try:
        import aiohttp
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=2)
        ) as session:
            async with session.get(url) as r:
                ct = r.headers.get("content-type", "")
                if r.status < 500:
                    if "json" in ct:
                        try:
                            body   = await r.json(content_type=None)
                            fields = _json_to_fields(body)
                            return DataContract(
                                schema_fields=fields,
                                semantic_tags=_tags_from_fields(fields) + ["rest", "process"],
                                encoding="json",
                                confidence=0.75,
                            )
                        except Exception:
                            pass
                    return DataContract(
                        schema_fields=[FieldSchema(name="data", type="object")],
                        semantic_tags=["rest", "process"],
                        encoding="json" if "json" in ct else "text",
                        confidence=0.5,
                    )
    except ImportError:
        # urllib fallback
        try:
            import urllib.request
            loop = asyncio.get_event_loop()
            def _fetch():
                req = urllib.request.Request(url, headers={"User-Agent": "interface-core/1.0"})
                with urllib.request.urlopen(req, timeout=2) as r:
                    return r.read(2048)
            body_bytes = await asyncio.wait_for(
                loop.run_in_executor(None, _fetch), timeout=3.0
            )
            return DataContract(
                schema_fields=[FieldSchema(name="data", type="object")],
                semantic_tags=["rest", "process"],
                encoding="json",
                confidence=0.45,
            )
        except Exception:
            pass
    except Exception:
        pass
    return None


async def _probe_tcp_banner(host: str, port: int) -> Optional[DataContract]:
    """Connect to TCP port and read the banner (first 256 bytes)."""
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=1.5
        )
        try:
            banner = await asyncio.wait_for(reader.read(256), timeout=1.0)
        except asyncio.TimeoutError:
            banner = b""
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

        if not banner:
            # Port is open but no banner — still register as unknown socket
            return DataContract(
                schema_fields=[FieldSchema(name="data", type="bytes")],
                semantic_tags=["socket", "process"],
                encoding="binary",
                confidence=0.3,
            )

        # Try to identify from banner
        banner_str = banner.decode("utf-8", errors="replace").lower()
        if banner_str.startswith("{") or "\"" in banner_str[:20]:
            return DataContract(
                schema_fields=[FieldSchema(name="data", type="object")],
                semantic_tags=["socket", "json", "process"],
                encoding="json",
                confidence=0.55,
            )
        # MQTT CONNACK has specific byte pattern
        if len(banner) >= 4 and banner[0] == 0x20 and banner[1] == 0x02:
            return DataContract(
                schema_fields=[FieldSchema(name="data", type="object")],
                semantic_tags=["mqtt", "process"],
                encoding="binary",
                confidence=0.7,
            )

        return DataContract(
            schema_fields=[FieldSchema(name="data", type="bytes")],
            semantic_tags=["socket", "process"],
            encoding="binary",
            confidence=0.35,
        )
    except (ConnectionRefusedError, asyncio.TimeoutError, OSError):
        return None
    except Exception as exc:
        logger.debug("TCP banner probe %s:%d failed: %s", host, port, exc)
        return None


def _infer_protocol_from_port(port: int) -> Protocol:
    if port in _GRPC_DEFAULT_PORTS:   return Protocol.GRPC
    if port in _HTTP_DEFAULT_PORTS:   return Protocol.REST
    if port in _KAFKA_DEFAULT_PORTS:  return Protocol.KAFKA
    if port in _MQTT_DEFAULT_PORTS:   return Protocol.MQTT
    if port in _AMQP_DEFAULT_PORTS:   return Protocol.AMQP
    return Protocol.SOCKET


def _json_to_fields(obj: Any) -> List[FieldSchema]:
    if not isinstance(obj, dict):
        return [FieldSchema(name="data", type="object")]
    fields = []
    for k, v in list(obj.items())[:10]:
        if isinstance(v, bool):     t = "bool"
        elif isinstance(v, int):    t = "int"
        elif isinstance(v, float):  t = "float"
        elif isinstance(v, list):   t = "array"
        elif isinstance(v, dict):   t = "object"
        else:                       t = "str"
        fields.append(FieldSchema(name=k, type=t))
    return fields or [FieldSchema(name="data", type="object")]


def _tags_from_fields(fields: List[FieldSchema]) -> List[str]:
    names = " ".join(f.name.lower() for f in fields)
    tags  = []
    if any(w in names for w in ["lat", "lon", "alt", "gps"]):       tags.append("geospatial")
    if any(w in names for w in ["price", "bid", "ask", "volume"]):  tags.append("market")
    if any(w in names for w in ["temp", "sensor", "imu", "pressure"]): tags.append("telemetry")
    if any(w in names for w in ["user", "email", "token", "auth"]):  tags.append("identity")
    if any(w in names for w in ["log", "level", "trace", "error"]):  tags.append("observability")
    return tags or ["generic"]


# ─────────────────────────────────────────────
# ProcessScanner
# ─────────────────────────────────────────────

class ProcessScanner:
    """
    Scans local process table and TCP listening sockets.
    For each found port, probes protocol and registers a node.
    """

    HOST = "127.0.0.1"

    def __init__(
        self,
        registry:   Any,
        graph:      Any,
        drift_log:  Any,
        event_bus:  Any,
        ml_matcher: Any,
        pg:         Any = None,
        exclude_ports: Optional[Set[int]] = None,
    ) -> None:
        self._registry   = registry
        self._graph      = graph
        self._drift_log  = drift_log
        self._bus        = event_bus
        self._matcher    = ml_matcher
        self._pg         = pg
        # Dynamically exclude ports the daemon itself owns so it doesn't
        # discover and try to bridge its own endpoints.
        from ..config import settings as _s
        _own_ports = set()
        for addr in [_s.zmq_pub_addr, _s.zmq_sub_addr]:
            try: _own_ports.add(int(addr.split(':')[-1]))
            except (ValueError, IndexError): pass
        _own_ports.add(getattr(_s, 'api_port', 8000))
        _own_ports.add(getattr(_s, 'prometheus_port', 9090))
        self._exclude    = (exclude_ports or set()) | _SKIP_PORTS | _own_ports
        self._seen_ports: Dict[int, float] = {}   # port → last confirmed time
        self._running    = False

    async def run(self) -> None:
        self._running = True
        logger.info("ProcessScanner started")
        while self._running:
            try:
                await self._scan()
            except Exception as exc:
                logger.debug("ProcessScanner error: %s", exc)
            await asyncio.sleep(settings.discovery_interval_ms / 1000.0)

    async def stop(self) -> None:
        self._running = False

    # Cooldown in seconds before re-probing a known port
    PROBE_COOLDOWN_S: float = 30.0

    async def _scan(self) -> None:
        ports = list_listening_ports()
        if not ports:
            return

        now   = time.time()
        tasks = []
        for port, proc_name in ports:
            if port in self._exclude:
                continue
            # Skip ports probed recently (cooldown prevents 500ms hammering)
            last = self._seen_ports.get(port, 0.0)
            if now - last < self.PROBE_COOLDOWN_S:
                continue
            tasks.append(self._probe_port(port, proc_name))
        if tasks:
            logger.debug("ProcessScanner probing %d new/stale ports", len(tasks))
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _probe_port(self, port: int, proc_name: str) -> None:
        """Probe one port and register/update the node."""
        hint_proto = _infer_protocol_from_port(port)

        # Try protocol-specific probe first, then generic TCP banner
        contract: Optional[DataContract] = None

        if hint_proto in (Protocol.REST, Protocol.GRPC):
            contract = await _probe_http(self.HOST, port)

        if contract is None:
            contract = await _probe_tcp_banner(self.HOST, port)

        if contract is None:
            return   # port not reachable

        # Refine protocol from contract tags if banner gave better info
        if "mqtt" in contract.semantic_tags:    hint_proto = Protocol.MQTT
        elif "grpc" in contract.semantic_tags:  hint_proto = Protocol.GRPC
        elif "json" in contract.semantic_tags:  hint_proto = Protocol.REST

        endpoint = f"{self.HOST}:{port}"
        if hint_proto == Protocol.REST:
            endpoint = f"http://{self.HOST}:{port}"

        # Tag with process name if available
        if proc_name:
            contract.semantic_tags = list(set(contract.semantic_tags + [proc_name.lower()]))
            contract.metadata = getattr(contract, 'metadata', None) or {}

        await self._register(hint_proto, endpoint, contract, port)

    async def _register(
        self,
        protocol: Protocol,
        endpoint: str,
        contract: DataContract,
        port: int,
    ) -> None:
        from ..phases.discovery import _make_node_id
        from ..models import Node, ContractSnapshot, Event, EventType

        node_id    = _make_node_id(protocol, endpoint)
        embedding  = await asyncio.get_event_loop().run_in_executor(
            None, self._matcher.embed, contract
        )
        contract_fp = contract.fingerprint()
        now         = time.time()

        self._seen_ports[port] = now

        if not self._registry.contains(node_id):
            node = Node(
                id        = node_id,
                protocol  = protocol,
                role      = NodeRole.BOTH,   # unknown — let matching decide
                endpoint  = endpoint,
                contract  = contract,
                embedding = embedding,
                metadata  = {"discovered_by": "process_scanner", "port": port},
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
                "PROCESS_SCAN found %s [%s] port=%d",
                node_id, protocol.value, port,
            )
            await self._bus.publish_async(Event(
                type=EventType.NEW_NODE,
                node_id=node_id,
                payload={
                    "protocol":    protocol.value,
                    "endpoint":    endpoint,
                    "source":      "process_scanner",
                    "port":        port,
                },
            ))
        else:
            self._registry.touch(node_id)

    @property
    def seen_port_count(self) -> int:
        return len(self._seen_ports)
