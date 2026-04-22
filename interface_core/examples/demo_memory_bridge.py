"""
Interface Core — Demo: In-Memory Bridge
=======================================
Runs entirely in-process with no external services.

Demonstrates:
  1. Registering a producer + consumer with mismatched field names
  2. Interface Core autonomously matching them, synthesizing a field-mapping adapter,
     and instantiating a live bridge
  3. Pushing payloads through inject() and reading transformed output from the consumer queue
  4. Watching live runtime stats

Run:
    cd interface_core
    python3 examples/demo_memory_bridge.py
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from interface_core.main import InterfaceCore
from interface_core.models import EventType


PRODUCER_ENDPOINT = "memory://sensor/temperature"
CONSUMER_ENDPOINT = "memory://dashboard/temperature"


async def main():
    print("\n" + "═" * 60)
    print("  Interface Core — In-Memory Bridge Demo")
    print("═" * 60 + "\n")

    core = InterfaceCore()

    # ── Wait for ML model ────────────────────────────────────────
    print("⏳  Loading ML matcher…")
    loop = asyncio.get_event_loop()
    ready = await loop.run_in_executor(None, core.ml_matcher.wait_ready, 30.0)
    model_name = "sentence-transformers" if ready and core.ml_matcher._embedder._model else "hashing-trick fallback"
    print(f"✓   ML matcher ready ({model_name})\n")

    # ── Register nodes ───────────────────────────────────────────
    # Producer has: temp_celsius, device_id, timestamp
    # Consumer wants: temperature, sensor_id, ts
    # Interface Core will align the fields automatically.
    print("📡  Registering producer (sensor feed)…")
    p_id = core.register_node(
        protocol         = "rest",        # use REST/memory transport
        endpoint         = PRODUCER_ENDPOINT,
        fields           = {
            "temp_celsius": "float",
            "device_id":    "str",
            "timestamp":    "int",
            "humidity_pct": "float",
        },
        tags             = ["telemetry", "iot", "temperature"],
        role             = "producer",
        emission_rate_hz = 2.0,
    )
    print(f"    id={p_id}")

    print("📥  Registering consumer (dashboard ingest)…")
    c_id = core.register_node(
        protocol  = "rest",
        endpoint  = CONSUMER_ENDPOINT,
        fields    = {
            "temperature": "float",
            "sensor_id":   "str",
            "ts":          "int",
            "humidity":    "float",
        },
        tags      = ["telemetry", "dashboard"],
        role      = "consumer",
    )
    print(f"    id={c_id}\n")

    # ── Capture BRIDGE_LIVE events ───────────────────────────────
    bridge_events = []
    event_q = core.event_bus.subscribe(EventType.BRIDGE_LIVE)

    # ── Run matching → policy → synthesis manually ────────────────
    print("🔍  Running matching…")
    await core.matching._match_all()
    print(f"    Bridge queue depth: {len(core.bridge_queue)}")

    synthesized = 0
    processed   = 0
    while not core.bridge_queue.empty() and processed < 20:
        cand = await core.bridge_queue.pop()
        if cand is None:
            break
        result = await core.policy_engine.evaluate(cand)
        if result.verdict.value == "approve":
            await core.synthesis_queue.push(cand)
        else:
            print(f"    ⚠ Policy: {result.verdict.value} — {result.reason}")
        processed += 1

    for _ in range(10):
        cand = await core.synthesis_queue.pop(timeout=0.2)
        if cand is None:
            break
        await core.synthesis._synthesize(cand)
        synthesized += 1

    # Drain bridge events
    while True:
        ev = await core.event_bus.next_event(event_q, timeout=0.1)
        if ev is None:
            break
        bridge_events.append(ev)

    if not bridge_events:
        print("\n⚠  No bridge was synthesized (score below threshold with fallback embedder).")
        print("   Install sentence-transformers for semantic matching:")
        print("   pip install sentence-transformers\n")
        _demo_adapter_directly(core, p_id, c_id)
        return

    print(f"\n✅  Bridge synthesized!  ({len(bridge_events)} BRIDGE_LIVE event(s))\n")
    for ev in bridge_events:
        p = ev.payload
        print(f"   Bridge {ev.bridge_id}")
        print(f"   {p.get('producer_protocol','?')} → {p.get('consumer_protocol','?')}")
        print(f"   Score:     {p.get('score', 0):.3f}")
        print(f"   Mappings:  {len(p.get('field_mappings', []))} field(s)")
        for m in p.get("field_mappings", []):
            arrow = "→" if m["src"] == m["dst"] else f"→ ({m['transform']})"
            print(f"              {m['src']} {arrow} {m['dst']}  [conf={m['confidence']:.2f}]")
        print()

    # ── Inject payloads and read output ─────────────────────────
    print("🚀  Injecting test payloads…\n")
    test_payloads = [
        {"temp_celsius": 23.5, "device_id": "sensor-001", "timestamp": 1712650000, "humidity_pct": 62.1},
        {"temp_celsius": 24.1, "device_id": "sensor-001", "timestamp": 1712650001, "humidity_pct": 61.8},
        {"temp_celsius": 25.0, "device_id": "sensor-002", "timestamp": 1712650002, "humidity_pct": 58.3},
    ]

    for i, payload in enumerate(test_payloads):
        ok = core.inject(p_id, payload)
        status = "✓" if ok else "✗ (not injected — check transport)"
        print(f"   Payload {i+1}: {status}")
        if not ok:
            print("   ℹ  Falling back to direct adapter transform…")

    # Give the runner a moment to process
    await asyncio.sleep(0.3)

    # Read from the consumer queue
    print("\n📊  Consumer received:\n")
    results = []
    for (pid, cid), entry in core.active_bridges.items():
        run_key = f"{pid}:{cid}"
        runner  = core._runners.get(run_key)
        if runner and hasattr(runner._consumer, "queue"):
            q = runner._consumer.queue
            while not q.empty():
                try:
                    results.append(q.get_nowait())
                except Exception:
                    break

    if results:
        for r in results:
            print(f"   {r}")
    else:
        # Runner may not have processed yet — show direct transform
        print("   (Runner still processing — showing direct adapter output)\n")
        _demo_adapter_directly(core, p_id, c_id)
        return

    # ── Stats ────────────────────────────────────────────────────
    print("\n📈  Bridge runtime stats:\n")
    for stats in core.bridge_stats():
        print(f"   Bridge  {stats['bridge_id']}")
        print(f"   Messages processed: {stats['messages']}")
        print(f"   Errors:             {stats['errors']}")
        print(f"   Circuit state:      {stats['circuit']}")
        print(f"   Latency p50:        {stats['latency_p50_ms']:.2f} ms")

    print("\n" + "═" * 60)
    print("  Demo complete.")
    print("═" * 60 + "\n")


def _demo_adapter_directly(core, p_id, c_id):
    """Fallback: show the adapter transform working even without a live bridge."""
    from interface_core.phases.synthesis import _synthesize_adapter, _validate_adapter
    p = core.registry.get(p_id)
    c = core.registry.get(c_id)
    if not p or not c:
        print("Nodes not found.")
        return

    p.embedding = core.ml_matcher.embed(p.contract)
    c.embedding = core.ml_matcher.embed(c.contract)
    adapter = _synthesize_adapter(p, c, core.ml_matcher)
    if not adapter:
        print("Adapter synthesis failed.")
        return

    print("✅  Adapter synthesized directly:\n")
    print(f"   Protocol:  {adapter.spec.producer_protocol.value} → {adapter.spec.consumer_protocol.value}")
    print(f"   Mappings:  {len(adapter.spec.field_mappings)} field(s)")
    for m in adapter.spec.field_mappings:
        print(f"              {m.src_field} → {m.dst_field}  [{m.transform}]")

    print("\n🔄  Transform test:\n")
    payload = {"temp_celsius": 23.5, "device_id": "sensor-001", "timestamp": 1712650000, "humidity_pct": 62.1}
    print(f"   Input:  {payload}")
    result = adapter.transform(payload)
    print(f"   Output: {result}\n")


if __name__ == "__main__":
    asyncio.run(main())
