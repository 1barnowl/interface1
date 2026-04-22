"""
Interface Core — Demo: Schema Pipeline
Shows every schema converter working on realistic Kali tool output.

Covers:
  1. Normalize messy field names (ts→timestamp, src_ip→source.address, lvl→severity)
  2. Filter by severity (drop DEBUG/INFO, keep WARNING+)
  3. Flatten nested JSON
  4. Unit conversion (unix→ISO-8601, bytes→human, C→F)
  5. Suppress noise (heartbeat/keepalive)
  6. Enrich with bridge provenance
  7. Full composable SchemaConverter pipeline
  8. csv_to_json, xml_to_json, json_to_csv conversions

Run:
    cd interface_core
    python3 examples/demo_schema_pipeline.py
"""

import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from interface_core.schema.converters import (
    SchemaConverter, normalize_fields, flatten_nested, unflatten_dotted,
    filter_severity, suppress_empty, suppress_noise,
    convert_units, enrich, keep_fields, drop_fields,
    json_to_csv, csv_to_json, xml_to_json,
    bytes_to_human, celsius_to_fahrenheit, unix_to_iso, normalize_severity,
)

LINE = "─" * 60


def section(title: str) -> None:
    print(f"\n{LINE}")
    print(f"  {title}")
    print(f"{LINE}")


def show(label: str, value) -> None:
    if isinstance(value, dict):
        print(f"  {label}:")
        for k, v in value.items():
            print(f"    {k}: {v!r}")
    elif isinstance(value, list):
        print(f"  {label}: [{len(value)} records]")
        for item in value[:3]:
            print(f"    {item!r}")
    elif value is None:
        print(f"  {label}: None  ← DROPPED")
    else:
        print(f"  {label}: {value!r}")
    print()


def main():
    print(f"\n{LINE}")
    print("  Interface Core — Schema Pipeline Demo")
    print(LINE)

    # ── 1. Field name normalization ──────────────────────────────────────────
    section("1. Field Name Normalization")
    raw_nmap = {
        "portid":    "80",
        "src_ip":    "192.168.1.100",
        "dst_ip":    "10.0.0.1",
        "ts":        1712650000,
        "lvl":       "warning",
        "msg":       "Port scan detected",
        "proc":      "nmap",
        "pid":       "4521",
    }
    show("Input (raw nmap-style record)", raw_nmap)
    normalized = normalize_fields(raw_nmap)
    show("After normalize_fields()", normalized)
    print("  ↳ portid→port, src_ip→source.address, ts→timestamp,")
    print("    lvl→severity, msg→message, proc→process.name, pid→process.pid")

    # ── 2. Severity filtering ────────────────────────────────────────────────
    section("2. Severity Filtering")
    records = [
        {"severity": "debug",   "message": "scan cycle started"},
        {"severity": "info",    "message": "probe sent to 192.168.1.1"},
        {"severity": "warning", "message": "suspicious connection attempt"},
        {"severity": "error",   "message": "authentication failure from 10.0.0.5"},
        {"severity": "critical","message": "rootkit signature detected"},
    ]
    print(f"  Input: {len(records)} records (debug → critical)")
    kept = [r for r in records if filter_severity(r, "warning") is not None]
    print(f"  After filter_severity(min='warning'): {len(kept)} records kept")
    for r in kept:
        print(f"    [{r['severity'].upper():<8}] {r['message']}")

    # ── 3. Noise suppression ─────────────────────────────────────────────────
    section("3. Noise Suppression")
    noisy = [
        {"message": "heartbeat"},
        {"message": "keepalive"},
        {"message": "TCP port 443 open on target 10.0.0.1"},
        {"message": "ping"},
        {"message": "Service fingerprint: Apache/2.4.41"},
    ]
    real = [r for r in noisy if suppress_noise(r) is not None]
    print(f"  Input: {len(noisy)} records  (including heartbeats/pings)")
    print(f"  After suppress_noise(): {len(real)} records kept")
    for r in real:
        print(f"    {r['message']!r}")

    # ── 4. Flatten nested JSON ───────────────────────────────────────────────
    section("4. Flatten Nested JSON")
    nested = {
        "host": {
            "address": "192.168.1.100",
            "os":      {"name": "Linux", "version": "5.15"},
        },
        "port": {"id": 443, "state": "open", "service": {"name": "https"}},
        "score": 0.92,
    }
    show("Input (nested nmap XML result)", nested)
    flat = flatten_nested(nested)
    show("After flatten_nested()", flat)
    # Roundtrip
    restored = unflatten_dotted(flat)
    print(f"  Roundtrip fidelity: {restored == nested}")

    # ── 5. Unit conversions ──────────────────────────────────────────────────
    section("5. Unit Conversions")
    sensor_record = {
        "timestamp":  1712650000,
        "temp_c":     23.5,
        "bytes_recv": 1_048_576,
        "severity":   "warn",
    }
    show("Input", sensor_record)
    converted = convert_units(sensor_record, {
        "timestamp":  "unix_to_iso",
        "temp_c":     "c_to_f",
        "bytes_recv": "bytes_to_human",
        "severity":   "normalize_severity",
    })
    show("After convert_units()", converted)

    # ── 6. Enrichment ────────────────────────────────────────────────────────
    section("6. Enrichment (provenance injection)")
    plain = {"source.address": "10.0.0.1", "port": 22, "severity": "warning"}
    enriched = enrich(plain, bridge_id="br-a1b2c3d4", source="http://siem:9000/events")
    show("After enrich(bridge_id=..., source=...)", enriched)
    print("  ↳ _ic_* fields added without touching original data")

    # ── 7. Full composable pipeline ──────────────────────────────────────────
    section("7. Full Composable Pipeline (Kali scan log → structured alert)")
    raw_logs = [
        # Normal findings
        {"ts": 1712650010, "src_ip": "10.0.0.5", "portid": "22",
         "lvl": "warning", "msg": "SSH brute-force attempt detected",
         "meta": {"attempts": 47, "duration_s": 12}},

        {"ts": 1712650020, "src_ip": "10.0.0.8", "portid": "443",
         "lvl": "error", "msg": "Invalid TLS cert on service",
         "meta": {"cert_cn": "MISMATCH", "expiry": "2020-01-01"}},

        # Noise to be dropped
        {"ts": 1712650015, "src_ip": "10.0.0.1", "portid": "0",
         "lvl": "debug", "msg": "keepalive",
         "meta": {}},

        # Empty record
        {"ts": None, "src_ip": None, "portid": None,
         "lvl": None, "msg": None, "meta": None},
    ]

    pipeline = (
        SchemaConverter()
        .add_step("normalize",       normalize_fields)
        .add_step("flatten",         flatten_nested)
        .add_step("unit_convert",    lambda p: convert_units(p, {
            "timestamp": "unix_to_iso",
            "severity":  "normalize_severity",
        }))
        .add_step("filter_sev",      lambda p: filter_severity(p, "warning"))
        .add_step("suppress_noise",  suppress_noise)
        .add_step("suppress_empty",  suppress_empty)
        .add_step("drop_internal",   lambda p: drop_fields(p, {"meta.duration_s"}))
        .add_step("enrich",          lambda p: enrich(p, bridge_id="br-kali-demo",
                                                       source="scan-engine"))
    )

    results = pipeline.apply_batch(raw_logs)

    print(f"  Input:  {len(raw_logs)} raw log records")
    print(f"  Output: {len(results)} enriched alerts\n")
    for i, r in enumerate(results, 1):
        print(f"  Alert {i}:")
        for k, v in r.items():
            if not k.startswith("_ic_"):
                print(f"    {k}: {v!r}")
        print(f"    (+ {sum(1 for k in r if k.startswith('_ic_'))} provenance fields)")
        print()

    # ── 8. Format conversions ────────────────────────────────────────────────
    section("8. Format Conversions: JSON ↔ CSV ↔ XML")

    # JSON → CSV
    scan_results = [
        {"host": "10.0.0.1", "port": 22, "state": "open",  "service": "ssh"},
        {"host": "10.0.0.1", "port": 80, "state": "open",  "service": "http"},
        {"host": "10.0.0.2", "port": 22, "state": "closed","service": "ssh"},
    ]
    csv_out = json_to_csv(scan_results, field_order=["host","port","state","service"])
    print("  JSON → CSV:")
    for line in csv_out.strip().splitlines():
        print(f"    {line}")

    # CSV back to JSON
    roundtrip = csv_to_json(csv_out)
    print(f"\n  CSV → JSON: {len(roundtrip)} records, first: {roundtrip[0]}")

    # XML → JSON
    nmap_xml = """<host>
        <address addr="10.0.0.1" addrtype="ipv4"/>
        <ports>
            <port protocol="tcp" portid="80">
                <state state="open"/>
                <service name="http" version="Apache 2.4"/>
            </port>
        </ports>
        <os><osmatch name="Linux" accuracy="95"/></os>
    </host>"""
    parsed = xml_to_json(nmap_xml)
    print(f"\n  XML → JSON: {json.dumps(parsed, indent=2)[:200]}...")

    # ── Summary ──────────────────────────────────────────────────────────────
    section("Summary")
    print("  SchemaConverter pipeline delivered:")
    print(f"    {len(raw_logs)} raw records → {len(results)} structured alerts")
    print(f"    {len(raw_logs) - len(results)} records dropped (noise / below threshold / empty)")
    print()
    print("  Field normalization:  portid→port, ts→timestamp, src_ip→source.address,")
    print("                        lvl→severity, msg→message, proc→process.name")
    print("  Unit conversion:      unix epoch → ISO-8601, warn → warning")
    print("  Flattening:           nested.key.value → flat key")
    print("  Enrichment:           _ic_bridge_id, _ic_source, _ic_hostname, _ic_pid")
    print()
    print("  All steps composable via SchemaConverter.from_spec({...}) for JSON config.")
    print()


if __name__ == "__main__":
    main()
