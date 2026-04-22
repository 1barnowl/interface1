"""
Interface Core — autonomous middleware glue daemon.

Quick start:
    import asyncio
    from interface_core import InterfaceCore

    async def main():
        core = InterfaceCore()
        pid  = core.register_node("rest", "http://src/data",
                                  fields={"price": "float", "sym": "str"},
                                  tags=["market"], role="producer", emission_rate_hz=5.0)
        cid  = core.register_node("rest", "http://dst/ingest",
                                  fields={"price": "float", "sym": "str"},
                                  tags=["market"], role="consumer")
        await core.start()
        await core.wait()

    asyncio.run(main())
"""

from .main import InterfaceCore

from .models import (
    # Core domain objects
    Bridge,
    BridgeCandidate,
    BridgeMetrics,
    BridgeState,
    DataContract,
    Event,
    EventType,
    FieldMapping,
    FieldSchema,
    MatchScore,
    Node,
    NodeRole,
    PolicyVerdict,
    Protocol,
    ContractSnapshot,
    AdapterSpec,
)

from .store import BridgeStore
from .schema.converters import (
    SchemaConverter, json_to_csv, csv_to_json, xml_to_json,
    flatten_nested, unflatten_dotted, normalize_fields,
    convert_units, enrich, filter_severity, keep_fields, drop_fields,
    suppress_empty, suppress_noise,
)
from .schema.security import SecurityPolicy, TrustZone, HardRefuseChecker, AllowlistChecker
from .schema.topology import FanOutRunner, FanInRunner
from .consul_discovery import ConsulDiscovery
from .phases.process_scanner import ProcessScanner
from .phases.grpc_transport import GRPCProducerTransport, GRPCConsumerTransport

from .phases.runner import (
    MemoryProducerTransport,
    MemoryConsumerTransport,
    RESTProducerTransport,
    RESTConsumerTransport,
    FileProducerTransport,
)

from .phases.lifecycle import (
    BridgeRuntime,
    CircuitBreaker,
    IdempotencyGuard,
)

from .phases.matching import BridgeQueue, BridgeHistory
from .phases.policy import PolicyEngine
from .registry import NodeRegistry, CapabilityGraph, DriftLog
from .event_bus import InProcessEventBus, make_event_bus
from .config import settings, PROTOCOL_COMPAT_MATRIX
from .logging_setup import setup_logging, print_banner, print_status_table

__all__ = [
    # Daemon
    "InterfaceCore",
    # Models
    "Bridge", "BridgeCandidate", "BridgeMetrics", "BridgeState",
    "DataContract", "Event", "EventType", "FieldMapping", "FieldSchema",
    "MatchScore", "Node", "NodeRole", "PolicyVerdict", "Protocol",
    "ContractSnapshot", "AdapterSpec",
    # Persistence
    "BridgeStore",
    # Schema
    "SchemaConverter", "json_to_csv", "csv_to_json", "xml_to_json",
    "flatten_nested", "unflatten_dotted", "normalize_fields",
    "convert_units", "enrich", "filter_severity",
    "keep_fields", "drop_fields", "suppress_empty", "suppress_noise",
    # Security
    "SecurityPolicy", "TrustZone", "HardRefuseChecker", "AllowlistChecker",
    # Topology
    "FanOutRunner", "FanInRunner",
    "ConsulDiscovery", "ProcessScanner",
    "GRPCProducerTransport", "GRPCConsumerTransport",
    # Transports
    "MemoryProducerTransport", "MemoryConsumerTransport",
    "RESTProducerTransport", "RESTConsumerTransport", "FileProducerTransport",
    # Resilience
    "BridgeRuntime", "CircuitBreaker", "IdempotencyGuard",
    # Phases
    "BridgeQueue", "BridgeHistory", "PolicyEngine",
    # Registry
    "NodeRegistry", "CapabilityGraph", "DriftLog",
    # Events
    "InProcessEventBus", "make_event_bus",
    # Config
    "settings", "PROTOCOL_COMPAT_MATRIX",
    "setup_logging", "print_banner", "print_status_table",
]
