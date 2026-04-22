"""
Interface Core — Adapters
Base adapter class + protocol-pair templates + code-gen for field mapping.
"""

from __future__ import annotations
import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from ..models import AdapterSpec, FieldMapping, Protocol

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Base Adapter
# ─────────────────────────────────────────────

class BaseAdapter(ABC):
    """
    All adapters implement transform(payload) → payload.
    Hot-reloadable: replace the instance atomically.
    """

    def __init__(self, spec: AdapterSpec) -> None:
        self.spec = spec

    @abstractmethod
    def transform(self, payload: Any) -> Any:
        """Convert producer payload to consumer-compatible form."""
        ...

    def validate(self, producer_sample: Any, consumer_sample: Any) -> bool:
        """Basic smoke-test: transform producer_sample and check it's not None."""
        try:
            result = self.transform(producer_sample)
            return result is not None
        except Exception:
            return False


# ─────────────────────────────────────────────
# Field-mapping transformer
# ─────────────────────────────────────────────

class FieldMappingAdapter(BaseAdapter):
    """
    Applies a list of FieldMappings to a dict payload.
    Handles: direct, rename, cast:<type>, extract:<path>, flatten, enrich:<literal>.

    Also supports optional pre- and post-processing SchemaConverter pipelines:
      pre_converter  — runs before field mapping  (normalize_fields, flatten, etc.)
      post_converter — runs after field mapping   (enrich, filter_severity, units, etc.)
    """

    _CAST = {
        "str":   str,
        "int":   int,
        "float": float,
        "bool":  bool,
    }

    def __init__(self, spec: AdapterSpec,
                 pre_converter=None, post_converter=None) -> None:
        super().__init__(spec)
        self._pre  = pre_converter   # SchemaConverter | None
        self._post = post_converter  # SchemaConverter | None

    def transform(self, payload: Any) -> Any:
        if not isinstance(payload, dict):
            try:
                payload = json.loads(payload) if isinstance(payload, (str, bytes)) else {"data": payload}
            except Exception:
                payload = {"data": payload}

        # ── Pre-processing (normalize field names, flatten, unit conversion, etc.) ──
        if self._pre is not None:
            payload = self._pre.apply(payload)
            if payload is None:
                return None   # pre-processor dropped the record

        out: Dict[str, Any] = {}
        for mapping in self.spec.field_mappings:
            value = payload.get(mapping.src_field)
            if value is None:
                continue

            xform = mapping.transform
            if xform == "direct" or xform.startswith("rename"):
                out[mapping.dst_field] = value
            elif xform.startswith("cast:"):
                cast_type = xform.split(":", 1)[1]
                caster    = self._CAST.get(cast_type, str)
                try:
                    out[mapping.dst_field] = caster(value)
                except (ValueError, TypeError):
                    # Use typed zero-value fallback and log the coercion failure
                    fallback = {"int": 0, "float": 0.0, "bool": False}.get(cast_type)
                    out[mapping.dst_field] = fallback if fallback is not None else value
                    import logging as _l
                    _l.getLogger(__name__).debug(
                        "cast:%s failed on field '%s' value=%r  using fallback=%r",
                        cast_type, mapping.src_field, value, out[mapping.dst_field]
                    )
            elif xform.startswith("extract:"):
                path   = xform.split(":", 1)[1].split(".")
                nested = value
                for part in path:
                    if isinstance(nested, dict):
                        nested = nested.get(part)
                    else:
                        nested = None
                        break
                out[mapping.dst_field] = nested
            elif xform == "flatten":
                if isinstance(value, dict):
                    for k, v in value.items():
                        out[f"{mapping.dst_field}_{k}"] = v
                else:
                    out[mapping.dst_field] = value
            elif xform.startswith("enrich:"):
                out[mapping.dst_field] = xform.split(":", 1)[1]
            else:
                out[mapping.dst_field] = value

        # Pass through any unmapped fields with low-confidence mappings
        mapped_srcs = {m.src_field for m in self.spec.field_mappings}
        for k, v in payload.items():
            if k not in mapped_srcs:
                out[k] = v

        # ── Post-processing (enrich, filter, severity, suppress noise, etc.) ──
        if self._post is not None:
            out = self._post.apply(out)

        return out


# ─────────────────────────────────────────────
# Protocol-pair shims (template library)
# ─────────────────────────────────────────────

class MQTTToKafkaAdapter(BaseAdapter):
    """Wraps MQTT message dict → Kafka-compatible dict."""
    def transform(self, payload: Any) -> Any:
        if isinstance(payload, (str, bytes)):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {"data": payload}
        return {
            "key": payload.get("topic", "unknown"),
            "value": json.dumps(payload).encode(),
            "headers": {"source": "mqtt"},
        }


class KafkaToRESTAdapter(BaseAdapter):
    """Unwraps Kafka record → plain dict for REST POST."""
    def transform(self, payload: Any) -> Any:
        if isinstance(payload, dict) and "value" in payload:
            try:
                return json.loads(payload["value"])
            except Exception:
                return {"data": payload["value"]}
        return payload


class MAVLinkToGRPCAdapter(BaseAdapter):
    """Converts a MAVLink message dict to a gRPC-compatible Protobuf-style dict."""
    def transform(self, payload: Any) -> Any:
        if not isinstance(payload, dict):
            return {"raw": str(payload)}
        return {
            "sys_id":    payload.get("sys_id", 0),
            "comp_id":   payload.get("comp_id", 0),
            "msg_id":    payload.get("msg_id", 0),
            "timestamp": payload.get("time_usec", 0),
            "fields":    {k: v for k, v in payload.items()
                          if k not in ("sys_id", "comp_id", "msg_id", "time_usec")},
        }


class RESTToZeroMQAdapter(BaseAdapter):
    """Serialises a REST response dict to a ZeroMQ-sendable bytes payload."""
    def transform(self, payload: Any) -> Any:
        if isinstance(payload, dict):
            return json.dumps(payload).encode()
        if isinstance(payload, (str,)):
            return payload.encode()
        return payload


class FileToKafkaAdapter(BaseAdapter):
    """Wraps a line/record from a file → Kafka value dict."""
    def transform(self, payload: Any) -> Any:
        return {
            "value": payload if isinstance(payload, bytes) else str(payload).encode(),
            "headers": {"source": "file"},
        }


class GenericJSONPassthroughAdapter(BaseAdapter):
    """Identity adapter for same-protocol / compatible-encoding pairs."""
    def transform(self, payload: Any) -> Any:
        return payload


# ─────────────────────────────────────────────
# Template registry
# ─────────────────────────────────────────────

TEMPLATE_REGISTRY: Dict[tuple, type] = {
    (Protocol.MQTT,      Protocol.KAFKA):     MQTTToKafkaAdapter,
    (Protocol.KAFKA,     Protocol.REST):      KafkaToRESTAdapter,
    (Protocol.MAVLINK,   Protocol.GRPC):      MAVLinkToGRPCAdapter,
    (Protocol.REST,      Protocol.ZEROMQ):    RESTToZeroMQAdapter,
    (Protocol.FILE,      Protocol.KAFKA):     FileToKafkaAdapter,
    # Catch-all: same encoding → passthrough
    (Protocol.REST,      Protocol.REST):      GenericJSONPassthroughAdapter,
    (Protocol.KAFKA,     Protocol.KAFKA):     GenericJSONPassthroughAdapter,
    (Protocol.WEBSOCKET, Protocol.WEBSOCKET): GenericJSONPassthroughAdapter,
}


def lookup_template(
    producer_protocol: Protocol, consumer_protocol: Protocol
) -> Optional[type]:
    """Return the best-fit adapter class for this protocol crossing."""
    return TEMPLATE_REGISTRY.get((producer_protocol, consumer_protocol))


# ─────────────────────────────────────────────
# Backpressure wrappers
# ─────────────────────────────────────────────

class TokenBucketAdapter(BaseAdapter):
    """Rate-limits output to consumer_rate msgs/sec using token bucket."""

    def __init__(self, inner: BaseAdapter, rate_hz: float) -> None:
        super().__init__(inner.spec)
        self._inner     = inner
        self._rate      = rate_hz
        self._tokens    = rate_hz
        self._last_tick = __import__("time").time()
        self._dropped   = 0

    def _refill(self) -> None:
        import time
        now    = time.time()
        delta  = now - self._last_tick
        self._tokens    = min(self._rate, self._tokens + delta * self._rate)
        self._last_tick = now

    def transform(self, payload: Any) -> Any:
        self._refill()
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return self._inner.transform(payload)
        self._dropped += 1
        return None   # signal: drop this message


def wrap_backpressure(
    adapter: BaseAdapter, producer_rate: float, consumer_rate: float
) -> BaseAdapter:
    """Select and apply the right backpressure wrapper."""
    if consumer_rate > 0 and producer_rate > consumer_rate * 1.5:
        return TokenBucketAdapter(adapter, consumer_rate)
    return adapter   # no wrapping needed
