"""
Interface Core — Data Models
All core domain objects as stdlib dataclasses.
Pydantic is supported if installed; falls back to dataclasses otherwise.
"""

from __future__ import annotations
import dataclasses
import time
import hashlib
import hmac
import json
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

# ── Pydantic shim ──────────────────────────────────────────────────────────
try:
    from pydantic import BaseModel, Field as _Field
    def Field(default=dataclasses.MISSING, **kwargs):
        if default is dataclasses.MISSING:
            return _Field(default_factory=kwargs.pop("default_factory", None) or (lambda: None), **kwargs)
        return _Field(default, **kwargs)
    _USE_PYDANTIC = True
except ImportError:
    # Pure stdlib fallback — models become regular dataclasses
    _USE_PYDANTIC = False
    class BaseModel:  # type: ignore
        """Minimal Pydantic BaseModel shim using dataclasses."""
        def __init_subclass__(cls, **kwargs):
            super().__init_subclass__(**kwargs)
            dataclasses.dataclass(cls)
        def json(self) -> str:
            return json.dumps(dataclasses.asdict(self), default=str)
        @classmethod
        def parse_raw(cls, raw: str):
            return cls(**json.loads(raw))
        class Config:
            arbitrary_types_allowed = True

    def Field(default=dataclasses.MISSING, default_factory=None, **kwargs):  # type: ignore
        if default_factory:
            return dataclasses.field(default_factory=default_factory)
        if default is dataclasses.MISSING:
            return dataclasses.field(default=None)
        return dataclasses.field(default=default)


# ─────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────

class Protocol(str, Enum):
    REST      = "rest"
    GRPC      = "grpc"
    WEBSOCKET = "websocket"
    MQTT      = "mqtt"
    KAFKA     = "kafka"
    ZEROMQ    = "zeromq"
    MAVLINK   = "mavlink"
    AMQP      = "amqp"
    FILE      = "file"
    SOCKET    = "socket"
    PROCESS   = "process"
    DATABASE  = "database"
    UNKNOWN   = "unknown"

class NodeRole(str, Enum):
    PRODUCER = "producer"
    CONSUMER = "consumer"
    BOTH     = "both"

class BridgeState(str, Enum):
    # ── Control-plane states (managed by matching/policy/synthesis) ──────────
    DISCOVERED   = "discovered"    # candidate pair first seen by matching
    SCORED       = "scored"        # pair scored; awaiting policy
    QUEUED       = "queued"        # in the bridge queue; awaiting policy eval
    POLICY_REVIEW = "policy_review" # staged for operator approval
    APPROVED     = "approved"      # policy passed; queued for synthesis
    SYNTHESIZING = "synthesizing"  # adapter being built

    # ── Data-plane states (managed by lifecycle/runner) ───────────────────────
    LIVE         = "live"          # runner active, data flowing
    DEGRADED     = "degraded"      # errors but below circuit threshold
    TRIPPED      = "tripped"       # circuit breaker open; paused
    TEARING_DOWN = "tearing_down"  # teardown in progress; draining buffer
    TORN_DOWN    = "torn_down"     # fully stopped

    # ── Legacy alias (keep existing code working) ────────────────────────────
    PENDING      = "pending"       # synonym for QUEUED (backward compat)

class PolicyVerdict(str, Enum):
    APPROVE = "approve"
    STAGE   = "stage"       # needs operator approval
    REJECT  = "reject"

class EventType(str, Enum):
    NEW_NODE        = "new_node"
    NODE_GONE       = "node_gone"
    CONTRACT_DRIFT  = "contract_drift"
    BRIDGE_LIVE     = "bridge_live"
    BRIDGE_TORN     = "bridge_torn"
    ADAPTER_RELOADED = "adapter_reloaded"
    SYNTHESIS_FAILED = "synthesis_failed"
    CIRCUIT_TRIP    = "circuit_trip"
    CIRCUIT_RECOVER = "circuit_recover"


# ─────────────────────────────────────────────
# Contract & Schema
# ─────────────────────────────────────────────

class FieldSchema(BaseModel):
    name:       str
    type:       str                          # str, int, float, bool, object, array, bytes
    nullable:   bool = False
    description: Optional[str] = None
    example:    Optional[Any]  = None

class DataContract(BaseModel):
    """Inferred schema + semantics of a node's data surface."""
    schema_fields:    List[FieldSchema] = Field(default_factory=list)
    semantic_tags:    List[str]         = Field(default_factory=list)   # e.g. ["telemetry","position","gps"]
    encoding:         str               = "json"                        # json, protobuf, avro, binary, text
    emission_rate_hz: Optional[float]   = None                         # msgs/sec; None = request/response
    confidence:       float             = 1.0                          # 0-1; low → flagged in matching
    raw_sample:       Optional[str]     = None                         # one raw payload for debug

    def fingerprint(self) -> str:
        payload = json.dumps(
            {"fields": [f.name for f in sorted(self.schema_fields, key=lambda x: x.name)],
             "tags": sorted(self.semantic_tags),
             "encoding": self.encoding},
            sort_keys=True
        )
        return hashlib.sha256(payload.encode()).hexdigest()[:16]


# ─────────────────────────────────────────────
# Node
# ─────────────────────────────────────────────

class Node(BaseModel):
    id:          str
    protocol:    Protocol
    role:        NodeRole
    endpoint:    str                          # uri / address / path
    contract:    DataContract
    embedding:   Optional[List[float]] = None  # 384-dim vector, populated by ML phase
    last_seen:   float = Field(default_factory=time.time)
    metadata:    Dict[str, Any] = Field(default_factory=dict)

    class Config:
        arbitrary_types_allowed = True


# ─────────────────────────────────────────────
# Bridge Candidate & Scoring
# ─────────────────────────────────────────────

class MatchScore(BaseModel):
    semantic_similarity:    float = 0.0   # cosine similarity of embeddings
    protocol_compat:        float = 0.0   # from compat matrix
    latency_score:          float = 0.0   # estimated latency fit 0-1
    risk_score:             float = 0.0   # policy risk (lower = better; inverted for composite)
    history_score:          float = 0.0   # past bridge success rate
    composite:              float = 0.0

class BridgeCandidate(BaseModel):
    producer_id: str
    consumer_id: str
    score:       MatchScore
    created_at:  float = Field(default_factory=time.time)

    @property
    def key(self) -> Tuple[str, str]:
        return (self.producer_id, self.consumer_id)


# ─────────────────────────────────────────────
# Adapter
# ─────────────────────────────────────────────

class FieldMapping(BaseModel):
    src_field:   str
    dst_field:   str
    transform:   str = "direct"   # direct | cast:<type> | rename | extract:<path> | flatten | enrich:<key>
    confidence:  float = 1.0

class AdapterSpec(BaseModel):
    producer_protocol: Protocol
    consumer_protocol: Protocol
    field_mappings:    List[FieldMapping] = Field(default_factory=list)
    backpressure_mode: str = "token_bucket"   # token_bucket | leaky_bucket | sliding_window
    version:           str = "0.1.0"
    compiled_code:     Optional[str] = None   # generated Python transform function


# ─────────────────────────────────────────────
# Bridge
# ─────────────────────────────────────────────

class BridgeMetrics(BaseModel):
    throughput_msg_s:  float = 0.0
    throughput_byte_s: float = 0.0
    error_rate:        float = 0.0
    latency_p50_ms:    float = 0.0
    latency_p99_ms:    float = 0.0
    buffer_fill:       float = 0.0    # 0-1
    circuit_trips:     int   = 0
    messages_total:    int   = 0

class Bridge(BaseModel):
    id:               str
    producer_id:      str
    consumer_id:      str
    adapter:          AdapterSpec
    state:            BridgeState = BridgeState.PENDING
    metrics:          BridgeMetrics = Field(default_factory=BridgeMetrics)
    latency_baseline: float = 0.0     # set after first 100 messages
    provenance_key:   Optional[str] = None
    created_at:       float = Field(default_factory=time.time)
    updated_at:       float = Field(default_factory=time.time)
    retry_count:      int = 0

    def sign_provenance(self, secret: bytes) -> str:
        payload = f"{self.producer_id}:{self.consumer_id}:{self.adapter.version}:{self.created_at}"
        return hmac.new(secret, payload.encode(), hashlib.sha256).hexdigest()

    class Config:
        arbitrary_types_allowed = True


# ─────────────────────────────────────────────
# Events
# ─────────────────────────────────────────────

class Event(BaseModel):
    type:      EventType
    node_id:   Optional[str]   = None
    bridge_id: Optional[str]   = None
    payload:   Dict[str, Any]  = Field(default_factory=dict)
    timestamp: float           = Field(default_factory=time.time)


# ─────────────────────────────────────────────
# Drift
# ─────────────────────────────────────────────

class ContractSnapshot(BaseModel):
    node_id:     str
    fingerprint: str
    contract:    DataContract
    timestamp:   float = Field(default_factory=time.time)
    delta:       float = 0.0   # 0-1; how much it changed vs previous
