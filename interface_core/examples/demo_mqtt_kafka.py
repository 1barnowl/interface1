"""
Interface Core — Demo: MQTT → Kafka Bridge
==========================================
Shows how Interface Core bridges a real IoT sensor stream to a Kafka topic.
Requires: aiokafka, asyncio-mqtt, and running Kafka + MQTT broker.

For local testing without infrastructure, run docker-compose up -d first.
Uses Mosquitto MQTT (port 1883) and Kafka (port 9092).

Run:
    cd interface_core
    docker-compose up -d
    python3 examples/demo_mqtt_kafka.py
"""

import asyncio
import json
import random
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from interface_core.main import InterfaceCore

MQTT_BROKER   = "localhost:1883"
KAFKA_BROKERS = "localhost:9092"


async def _publish_fake_sensor_data(n: int = 10):
    """Simulate a sensor publishing to MQTT."""
    try:
        import asyncio_mqtt as aiomqtt
        async with aiomqtt.Client(MQTT_BROKER.split(":")[0],
                                  int(MQTT_BROKER.split(":")[1])) as client:
            for i in range(n):
                payload = json.dumps({
                    "topic":      "sensors/temperature",
                    "device_id":  f"sensor-{(i % 3) + 1:03d}",
                    "temp_c":     round(20.0 + random.uniform(0, 10), 2),
                    "humidity":   round(40.0 + random.uniform(0, 30), 1),
                    "timestamp":  1712650000 + i,
                })
                await client.publish("sensors/temperature", payload)
                print(f"  MQTT ← published message {i+1}: {payload[:60]}…")
                await asyncio.sleep(0.5)
    except ImportError:
        print("  asyncio-mqtt not installed. Install with: pip install asyncio-mqtt")
    except Exception as exc:
        print(f"  MQTT publisher error: {exc}")


async def main():
    print("\n" + "═" * 60)
    print("  Interface Core — MQTT→Kafka Bridge Demo")
    print("═" * 60 + "\n")

    core = InterfaceCore()
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, core.ml_matcher.wait_ready, 30.0)

    # ── Register nodes ────────────────────────────────────────────
    print("📡  Registering MQTT producer (IoT sensor stream)…")
    p_id = core.register_node(
        protocol         = "mqtt",
        endpoint         = f"mqtt://{MQTT_BROKER}/sensors/temperature",
        fields           = {
            "topic":     "str",
            "device_id": "str",
            "temp_c":    "float",
            "humidity":  "float",
            "timestamp": "int",
        },
        tags             = ["iot", "telemetry", "temperature"],
        role             = "producer",
        emission_rate_hz = 2.0,
    )
    print(f"    id={p_id}")

    print("📥  Registering Kafka consumer (analytics pipeline)…")
    c_id = core.register_node(
        protocol  = "kafka",
        endpoint  = f"{KAFKA_BROKERS}/processed-sensors",
        fields    = {
            "key":         "str",
            "temperature": "float",
            "humidity":    "float",
            "device":      "str",
            "ts":          "int",
        },
        tags      = ["analytics", "telemetry"],
        role      = "consumer",
    )
    print(f"    id={c_id}\n")

    # ── Match and bridge ──────────────────────────────────────────
    print("🔍  Running matching engine…")
    await core.matching._match_all()
    print(f"    Queue depth: {len(core.bridge_queue)}")

    processed = 0
    while not core.bridge_queue.empty() and processed < 10:
        cand = await core.bridge_queue.pop()
        if cand is None:
            break
        result = await core.policy_engine.evaluate(cand)
        if result.verdict.value == "approve":
            await core.synthesis_queue.push(cand)
        processed += 1

    bridges_before = len(core.active_bridges)
    for _ in range(5):
        cand = await core.synthesis_queue.pop(timeout=0.2)
        if cand is None:
            break
        await core.synthesis._synthesize(cand)

    if len(core.active_bridges) == bridges_before:
        print("⚠  No bridge synthesized (score below threshold).")
        print("   Try installing sentence-transformers for better semantic matching.\n")
        status = core.status()
        print(f"   Nodes registered: {status['nodes']}")
        print(f"   Queue processed:  {processed}")
        return

    print(f"\n✅  {len(core.active_bridges)} bridge(s) live!\n")

    for stats in core.bridge_stats():
        print(f"   Bridge {stats['bridge_id']}: circuit={stats['circuit']}")

    # ── Publish fake sensor data ──────────────────────────────────
    print("\n🚀  Publishing fake sensor data to MQTT…\n")
    await _publish_fake_sensor_data(n=5)

    print("\n⏳  Waiting for bridge to process messages…")
    await asyncio.sleep(3.0)

    print("\n📈  Final stats:\n")
    for stats in core.bridge_stats():
        print(f"   Bridge  {stats['bridge_id']}")
        print(f"   Processed: {stats['messages']} msgs")
        print(f"   Errors:    {stats['errors']}")
        print(f"   Circuit:   {stats['circuit']}")
        print(f"   Latency:   p50={stats['latency_p50_ms']:.1f}ms  p99={stats['latency_p99_ms']:.1f}ms")

    # Graceful shutdown
    await core.stop()
    print("\n" + "═" * 60)
    print("  Demo complete. Check Kafka topic 'processed-sensors'.")
    print("═" * 60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
