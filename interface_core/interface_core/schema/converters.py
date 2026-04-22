"""
Interface Core — Schema Converters
Format conversion, field normalization, and unit transformation.

Converters are stateless functions that take a payload (dict, str, bytes)
and return a transformed payload. They are composable — the adapter
synthesis layer chains them based on the producer/consumer contract pair.

Supported conversions:
  json_to_csv          dict → CSV row string
  csv_to_json          CSV string → dict
  xml_to_json          XML string → dict
  flatten_nested       {a: {b: 1}} → {"a.b": 1}
  unflatten_dotted     {"a.b": 1} → {a: {b: 1}}
  normalize_fields     rename common field aliases to canonical names
  convert_units        bytes↔KB/MB/GB, C↔F, unix↔ISO-8601, severity levels
  add_enrichment       inject hostname, pid, timestamp, bridge_id, source
  filter_fields        keep/drop fields by name or pattern
  filter_severity      drop records below a severity threshold
  suppress_empty       drop records with all-empty/null values
"""

from __future__ import annotations
import csv
import io
import json
import logging
import os
import socket
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Union

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Format conversion
# ─────────────────────────────────────────────

def json_to_csv(payload: Any, field_order: Optional[List[str]] = None) -> str:
    """
    Convert a dict (or list of dicts) to a CSV string.
    field_order: explicit column order; defaults to sorted keys.
    """
    if isinstance(payload, (str, bytes)):
        try:
            payload = json.loads(payload)
        except Exception:
            return str(payload)

    records = payload if isinstance(payload, list) else [payload]
    if not records:
        return ""

    # Gather all keys
    all_keys: List[str] = field_order or sorted(
        {k for r in records if isinstance(r, dict) for k in r}
    )
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=all_keys, extrasaction="ignore")
    writer.writeheader()
    for rec in records:
        if isinstance(rec, dict):
            writer.writerow(rec)
    return buf.getvalue()


def csv_to_json(payload: Union[str, bytes]) -> List[Dict]:
    """
    Convert a CSV string (with header row) to a list of dicts.
    Returns a single dict when only one data row is present.
    """
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(payload.strip()))
    rows = [dict(row) for row in reader]
    return rows[0] if len(rows) == 1 else rows


def xml_to_json(payload: Union[str, bytes]) -> Dict:
    """
    Convert XML to a dict using stdlib xml.etree.ElementTree.
    Attributes become @attr keys; text becomes _text; children nest naturally.
    """
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8", errors="replace")
    import xml.etree.ElementTree as ET

    def _elem_to_dict(elem) -> Any:
        result: Dict[str, Any] = {}
        # Attributes
        if elem.attrib:
            result.update({f"@{k}": v for k, v in elem.attrib.items()})
        # Text
        text = (elem.text or "").strip()
        if text:
            result["_text"] = text
        # Children
        for child in elem:
            child_data = _elem_to_dict(child)
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if tag in result:
                existing = result[tag]
                if not isinstance(existing, list):
                    result[tag] = [existing]
                result[tag].append(child_data)
            else:
                result[tag] = child_data
        return result if result else text

    try:
        root = ET.fromstring(payload)
        tag  = root.tag.split("}")[-1] if "}" in root.tag else root.tag
        return {tag: _elem_to_dict(root)}
    except ET.ParseError as exc:
        logger.debug("xml_to_json parse error: %s", exc)
        return {"_raw_xml": payload}


# ─────────────────────────────────────────────
# Structure transformation
# ─────────────────────────────────────────────

def flatten_nested(payload: Dict, sep: str = ".", prefix: str = "") -> Dict:
    """
    Flatten a nested dict: {"a": {"b": 1, "c": 2}} → {"a.b": 1, "a.c": 2}
    """
    result: Dict = {}
    for k, v in payload.items():
        full_key = f"{prefix}{sep}{k}" if prefix else k
        if isinstance(v, dict):
            result.update(flatten_nested(v, sep=sep, prefix=full_key))
        elif isinstance(v, list) and v and isinstance(v[0], dict):
            for i, item in enumerate(v):
                result.update(flatten_nested(item, sep=sep, prefix=f"{full_key}[{i}]"))
        else:
            result[full_key] = v
    return result


def unflatten_dotted(payload: Dict, sep: str = ".") -> Dict:
    """
    Reconstruct nesting: {"a.b": 1, "a.c": 2} → {"a": {"b": 1, "c": 2}}
    """
    result: Dict = {}
    for key, value in payload.items():
        parts = key.split(sep)
        node = result
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = value
    return result


# ─────────────────────────────────────────────
# Field name normalization
# ─────────────────────────────────────────────

# Canonical alias map — common variants → standard name
_FIELD_ALIASES: Dict[str, str] = {
    # Network
    "portid":         "port",
    "port_id":        "port",
    "dst_port":       "destination.port",
    "src_port":       "source.port",
    "src_ip":         "source.address",
    "src_addr":       "source.address",
    "source_ip":      "source.address",
    "dst_ip":         "destination.address",
    "dst_addr":       "destination.address",
    "dest_ip":        "destination.address",
    "remote_addr":    "destination.address",
    "remote_ip":      "destination.address",

    # Time
    "ts":             "timestamp",
    "time":           "timestamp",
    "created":        "timestamp",
    "created_at":     "timestamp",
    "updated_at":     "timestamp",
    "event_time":     "timestamp",
    "log_time":       "timestamp",

    # Identity
    "uid":            "user.id",
    "userid":         "user.id",
    "user_id":        "user.id",
    "uname":          "user.name",
    "username":       "user.name",

    # Severity / level
    "lvl":            "severity",
    "level":          "severity",
    "log_level":      "severity",
    "loglevel":       "severity",
    "sev":            "severity",
    "priority":       "severity",

    # Message
    "msg":            "message",
    "log":            "message",
    "text":           "message",
    "body":           "message",
    "content":        "message",

    # Process
    "proc":           "process.name",
    "process":        "process.name",
    "cmd":            "process.name",
    "command":        "process.name",
    "pid":            "process.pid",

    # Temperature / sensors
    "temp":           "temperature",
    "temp_c":         "temperature_celsius",
    "temp_f":         "temperature_fahrenheit",
    "hum":            "humidity",
    "humidity_pct":   "humidity",
}


def normalize_fields(payload: Dict, aliases: Optional[Dict[str, str]] = None) -> Dict:
    """
    Rename fields using the canonical alias map.
    Custom aliases override the built-in map.
    """
    mapping = dict(_FIELD_ALIASES)
    if aliases:
        mapping.update(aliases)
    result = {}
    for k, v in payload.items():
        canonical = mapping.get(k.lower(), k)
        result[canonical] = v
    return result


# ─────────────────────────────────────────────
# Unit conversion
# ─────────────────────────────────────────────

_SEVERITY_MAP: Dict[str, int] = {
    "trace": 0, "debug": 1, "info": 2, "notice": 2,
    "warning": 3, "warn": 3, "error": 4, "err": 4,
    "critical": 5, "crit": 5, "fatal": 5, "emergency": 5,
    "0": 0, "1": 1, "2": 2, "3": 3, "4": 4, "5": 5,
}

_SEVERITY_LABEL: Dict[int, str] = {0: "trace", 1: "debug", 2: "info", 3: "warning", 4: "error", 5: "critical"}


def normalize_severity(value: Any) -> str:
    """Normalize any severity representation to a canonical label."""
    key = str(value).lower().strip()
    level = _SEVERITY_MAP.get(key, 2)
    return _SEVERITY_LABEL[level]


def bytes_to_human(value: Any) -> str:
    """Convert byte count to human-readable string (KB/MB/GB)."""
    try:
        b = float(value)
    except (ValueError, TypeError):
        return str(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(b) < 1024.0:
            return f"{b:.2f} {unit}"
        b /= 1024.0
    return f"{b:.2f} PB"


def celsius_to_fahrenheit(c: Any) -> float:
    return round(float(c) * 9 / 5 + 32, 4)


def fahrenheit_to_celsius(f: Any) -> float:
    return round((float(f) - 32) * 5 / 9, 4)


def unix_to_iso(ts: Any) -> str:
    """Convert a Unix timestamp (int or float) to ISO-8601 UTC string."""
    try:
        dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
        return dt.isoformat()
    except (ValueError, TypeError, OSError):
        return str(ts)


def iso_to_unix(ts: Any) -> float:
    """Convert an ISO-8601 string to a Unix timestamp."""
    ts_str = str(ts)
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(ts_str.replace("Z", "+00:00"), fmt)
            return dt.timestamp()
        except ValueError:
            continue
    try:
        return float(ts_str)
    except ValueError:
        return 0.0


def convert_units(payload: Dict, rules: Dict[str, str]) -> Dict:
    """
    Apply unit conversion rules to specific fields.

    rules format: {field_name: conversion_spec}
    conversion specs:
      "bytes_to_human"       — byte count → "1.23 MB"
      "c_to_f"               — Celsius → Fahrenheit
      "f_to_c"               — Fahrenheit → Celsius
      "unix_to_iso"          — Unix epoch → ISO-8601
      "iso_to_unix"          — ISO-8601 → Unix epoch
      "normalize_severity"   — any severity → canonical label
    """
    _CONVERTERS = {
        "bytes_to_human":     bytes_to_human,
        "c_to_f":             celsius_to_fahrenheit,
        "f_to_c":             fahrenheit_to_celsius,
        "unix_to_iso":        unix_to_iso,
        "iso_to_unix":        iso_to_unix,
        "normalize_severity": normalize_severity,
    }
    result = dict(payload)
    for field, spec in rules.items():
        if field in result and spec in _CONVERTERS:
            try:
                result[field] = _CONVERTERS[spec](result[field])
            except Exception as exc:
                logger.debug("convert_units failed on field %s spec %s: %s", field, spec, exc)
    return result


# ─────────────────────────────────────────────
# Filtering
# ─────────────────────────────────────────────

def keep_fields(payload: Dict, fields: Set[str]) -> Dict:
    """Return only the specified fields."""
    return {k: v for k, v in payload.items() if k in fields}


def drop_fields(payload: Dict, fields: Set[str]) -> Dict:
    """Remove the specified fields."""
    return {k: v for k, v in payload.items() if k not in fields}


def filter_severity(payload: Dict, min_level: str = "info",
                    severity_field: str = "severity") -> Optional[Dict]:
    """
    Return None (drop) if the record's severity is below min_level.
    Returns the payload unchanged if the severity field is absent.
    """
    if severity_field not in payload:
        return payload
    record_level = _SEVERITY_MAP.get(str(payload[severity_field]).lower(), 2)
    threshold    = _SEVERITY_MAP.get(min_level.lower(), 2)
    return payload if record_level >= threshold else None


def suppress_empty(payload: Dict) -> Optional[Dict]:
    """Return None if all values in the payload are None/empty/whitespace."""
    for v in payload.values():
        if v is not None and str(v).strip():
            return payload
    return None


def suppress_noise(payload: Dict, noise_patterns: Optional[List[str]] = None) -> Optional[Dict]:
    """
    Return None if the message field matches any noise pattern.
    Default patterns: heartbeat, keepalive, ping, noop.
    """
    defaults = ["heartbeat", "keepalive", "ping", "noop", "alive", "ok\n", "ok\r"]
    patterns = noise_patterns or defaults
    msg = str(payload.get("message", payload.get("msg", payload.get("_text", "")))).lower()
    if any(p in msg for p in patterns):
        return None
    return payload


# ─────────────────────────────────────────────
# Enrichment
# ─────────────────────────────────────────────

_HOSTNAME = socket.gethostname()
_PID      = os.getpid()


def enrich(
    payload:   Dict,
    bridge_id: Optional[str] = None,
    source:    Optional[str] = None,
    extra:     Optional[Dict] = None,
) -> Dict:
    """
    Add provenance and context metadata to a record.

    Injected fields (prefixed with _ic_ to avoid collisions):
      _ic_bridge_id     bridge that processed this record
      _ic_source        producer endpoint
      _ic_hostname      host that ran the daemon
      _ic_pid           daemon process ID
      _ic_ingested_at   ISO-8601 ingestion timestamp
    """
    result = dict(payload)
    result["_ic_ingested_at"] = unix_to_iso(time.time())
    result["_ic_hostname"]    = _HOSTNAME
    result["_ic_pid"]         = _PID
    if bridge_id:
        result["_ic_bridge_id"] = bridge_id
    if source:
        result["_ic_source"] = source
    if extra:
        result.update(extra)
    return result


# ─────────────────────────────────────────────
# SchemaConverter — composable pipeline
# ─────────────────────────────────────────────

class SchemaConverter:
    """
    A composable, ordered pipeline of schema transformations.

    Each step is a (name, callable) pair. The callable receives the current
    payload and returns the transformed payload (or None to drop the record).

    Usage:
        conv = SchemaConverter()
        conv.add_step("normalize",  lambda p: normalize_fields(p))
        conv.add_step("flatten",    lambda p: flatten_nested(p))
        conv.add_step("filter_sev", lambda p: filter_severity(p, "warning"))
        conv.add_step("enrich",     lambda p: enrich(p, bridge_id="br-123"))
        result = conv.apply({"lvl": "debug", "msg": "heartbeat"})
        # result is None (dropped: below severity threshold after normalize)
    """

    def __init__(self) -> None:
        self._steps: List[tuple] = []

    def add_step(self, name: str, fn) -> "SchemaConverter":
        self._steps.append((name, fn))
        return self

    def apply(self, payload: Any) -> Optional[Any]:
        """
        Apply each step in order.

        Failure semantics (per doc review):
        - A step that raises is a DATA ERROR, not a minor inconvenience.
        - We emit a WARNING (not DEBUG) so it is visible in normal logs.
        - We return None to DROP the record rather than forwarding corrupted data.
        - Silent pass-through on schema errors is worse than a loud drop.

        Callers can wrap with try/except if they need custom handling.
        """
        current = payload
        for name, fn in self._steps:
            if current is None:
                return None
            try:
                current = fn(current)
            except Exception as exc:
                logger.warning(
                    "SchemaConverter step '%s' FAILED — dropping record to prevent "
                    "silent data corruption: %s", name, exc
                )
                return None   # drop: do not forward a potentially corrupted payload
        return current

    def apply_batch(self, payloads: List[Any]) -> List[Any]:
        """Apply the pipeline to a list; None results (drops) are excluded."""
        results = []
        for p in payloads:
            out = self.apply(p)
            if out is not None:
                results.append(out)
        return results

    @classmethod
    def from_spec(cls, spec: Dict) -> "SchemaConverter":
        """
        Build a converter from a declarative spec dict.

        spec format:
          {
            "normalize_fields": true,
            "flatten": true,
            "keep_fields": ["field1", "field2"],
            "drop_fields": ["_internal"],
            "unit_conversions": {"timestamp": "unix_to_iso", "bytes": "bytes_to_human"},
            "filter_severity": "warning",
            "suppress_empty": true,
            "enrich": {"bridge_id": "br-123", "source": "http://src"}
          }
        """
        conv = cls()
        if spec.get("normalize_fields"):
            aliases = spec.get("field_aliases", {})
            conv.add_step("normalize_fields", lambda p, a=aliases: normalize_fields(p, a))
        if spec.get("flatten"):
            sep = spec.get("flatten_sep", ".")
            conv.add_step("flatten", lambda p, s=sep: flatten_nested(p, sep=s))
        if spec.get("unflatten"):
            sep = spec.get("flatten_sep", ".")
            conv.add_step("unflatten", lambda p, s=sep: unflatten_dotted(p, sep=s))
        if "keep_fields" in spec:
            fields = set(spec["keep_fields"])
            conv.add_step("keep_fields", lambda p, f=fields: keep_fields(p, f))
        if "drop_fields" in spec:
            fields = set(spec["drop_fields"])
            conv.add_step("drop_fields", lambda p, f=fields: drop_fields(p, f))
        if "unit_conversions" in spec:
            rules = spec["unit_conversions"]
            conv.add_step("unit_conversions", lambda p, r=rules: convert_units(p, r))
        if "filter_severity" in spec:
            level = spec["filter_severity"]
            field = spec.get("severity_field", "severity")
            conv.add_step("filter_severity",
                          lambda p, l=level, f=field: filter_severity(p, l, f))
        if spec.get("suppress_empty"):
            conv.add_step("suppress_empty", suppress_empty)
        if spec.get("suppress_noise"):
            patterns = spec.get("noise_patterns")
            conv.add_step("suppress_noise", lambda p, pat=patterns: suppress_noise(p, pat))
        if "enrich" in spec:
            enrich_spec = spec["enrich"]
            conv.add_step("enrich", lambda p, e=enrich_spec: enrich(p, **e))
        return conv
