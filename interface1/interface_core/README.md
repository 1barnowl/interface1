                                                                     Glue

<div align="center">


**Autonomous Middleware Daemon**  

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)]()
[![OpenTelemetry](https://img.shields.io/badge/OTel-observable-blueviolet)]()

</div>

## Overview

**Glue** is an autonomous middleware daemon that continuously discovers, maps, and bridges disconnected software endpoints in real time. It scans for data producers and consumers across protocols (REST, gRPC, MQTT, Kafka, sockets, telemetry), infers their data contracts, and uses semantic embeddings to score potential pairings. When a viable match is found, it synthesizes a protocol adapter and field-mapping shim, applies policy checks, and instantiates a resilient bridge with circuit breakers, buffering, and full observability—all without manual configuration.

Think of Glue as a **self-driving integration layer** that actively seeks out opportunities to connect your tools, services, and devices. It runs as a daemon, learns your ecosystem, and proactively creates reliable data flows wherever they are needed.

> ⚠️ **Project Status**: Glue is in active early development. The core architecture is defined, and we are building the foundational components. Contributions and feedback are welcome!

## Key Features

- **🔍 Autonomous Discovery**  
  Passive sniffing, API introspection, process scanning, and message broker enumeration across REST, gRPC, MQTT, Kafka, MAVLink, files, sockets, and more.

- **🧠 Semantic Matching**  
  Fine-tuned sentence transformers generate dense embeddings for inferred contracts, enabling probabilistic matching based on data meaning—not just protocol compatibility.

- **⚙️ Adapter Synthesis**  
  Automatically generates WASM-sandboxed adapters that handle protocol translation, field mapping, type coercion, and backpressure. Hot-reloadable without interrupting data flow.

- **🛡️ Policy Engine**  
  Open Policy Agent (OPA) integration enforces auth, rate limits, whitelist/blacklist, geo-fencing, and manual approval workflows before any bridge is instantiated.

- **🔄 Resilience & Self-Healing**  
  Every bridge includes exponential backoff retries, circuit breakers, bounded ring buffers, idempotency guards, and HMAC-signed provenance. Contracts are continuously monitored for drift; when detected, adapters are re-synthesized and hot-reloaded automatically.

- **📊 Observability**  
  Native OpenTelemetry support exports traces, metrics, and logs to Prometheus, Jaeger, and Loki. A real-time dashboard visualizes the capability graph, active bridges, drift events, and system health.

- **🧩 Extensible & Modular**  
  Plugin architecture for new protocols, custom adapters, and ML models. Built to scale horizontally in Kubernetes with per‑phase worker pools.

## Architecture

Glue is built as a multi‑threaded daemon where each phase runs on independent event loops:






- **Discovery Worker** scans the environment every 500ms, infers data contracts, and builds semantic embeddings.  
- **Matching Worker** ranks producer‑consumer pairs using a composite score (semantic similarity, protocol compatibility, latency estimates, policy risk).  
- **Policy Engine** (OPA) evaluates each candidate against user-defined Rego policies, rejecting unsafe matches and queueing high‑risk ones for manual approval.  
- **Synthesis Worker** compiles protocol translators, field mappers, and backpressure controllers into a WASM module, then deploys the bridge.  
- **Lifecycle Worker** monitors bridge health, handles retries/circuit breakers, and re‑synthesizes adapters when contract drift exceeds 15%.

All state is backed by PostgreSQL and Redis, with the capability graph held in memory for fast traversal.

## Core Dependencies

Glue leverages a carefully chosen set of open‑source technologies:

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Core Language** | Rust (or Go) | High‑performance, safe concurrency |
| **Message Bus** | ZeroMQ, Redpanda (Kafka‑compatible) | Internal event fabric |
| **Service Discovery** | Consul | Node registry and health checking |
| **State & Persistence** | PostgreSQL, Redis | Durable storage, caching, leader election |
| **Observability** | OpenTelemetry, Prometheus, Jaeger, Loki | Metrics, traces, logs |
| **Policy Engine** | Open Policy Agent (OPA) | Policy evaluation and enforcement |
| **WASM Runtime** | Wasmtime, Extism | Sandboxed adapter execution |
| **ML Inference** | ONNX Runtime, sentence-transformers | Semantic embedding and field alignment |
| **Container Orchestration** | Docker Compose (dev), Kubernetes/Helm (prod) | Deployment and scaling |

> Full dependency list is available in `Cargo.toml` (Rust) or `go.mod` (Go). We aim to keep the stack entirely self‑hostable and free of proprietary services.



























**Autonomous middleware glue daemon** — proactive discovery, library consolidation,
and dynamic bridging between disparate software nodes.

---

## What It Does

Interface Core runs as a background daemon that:

1. **Discovers** live nodes across your ecosystem — REST APIs, Kafka topics, MQTT brokers,
   sockets, files, gRPC services, MAVLink streams, and more
2. **Infers** each node's data contract (schema, encoding, emission rate, semantic domain)
   using a multi-strategy pipeline (OpenAPI parsing, traffic sampling, protocol reflection)
3. **Matches** producers to compatible consumers using semantic embeddings + protocol
   compatibility scoring
4. **Synthesizes** adapters — field mappers, protocol shims, backpressure controllers —
   compiled to WASM for hot-reload
5. **Instantiates** live bridges with exponential-backoff retries, circuit breakers, ring
   buffers, and idempotency guards
6. **Watches** for contract drift and hot-reloads adapters automatically, or tears down and
   re-evaluates if the change is too large

All of this happens **without human initiation**.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                       Interface Core Daemon                  │
│                                                              │
│  DiscoveryPhase  →  MatchingPhase  →  PolicyPhase           │
│       ↓                  ↓                ↓                  │
│  NodeRegistry      BridgeQueue      SynthesisQueue           │
│  CapabilityGraph   ML Matcher       PolicyEngine             │
│  DriftLog          BridgeHistory    AuditLog                 │
│                          ↓                                   │
│                   SynthesisPhase → active_bridges            │
│                          ↓                                   │
│                   LifecyclePhase ←→ DriftWatchPhase          │
│                                                              │
│  All phases communicate via ZeroMQ EventBus                  │
│  Metrics → Prometheus · Traces → Jaeger · Logs → Loki        │
└──────────────────────────────────────────────────────────────┘
```

### Phases

| Phase | Interval | Trigger |
|-------|----------|---------|
| Discovery | 500 ms | periodic |
| Matching | 1 s | `NEW_NODE`, `CONTRACT_DRIFT`, periodic |
| Policy | — | `bridge_queue.not_empty` |
| Synthesis | — | `synthesis_queue.not_empty` |
| Lifecycle | 1 s | periodic |
| DriftWatch | 5 s | periodic |

---

## Quick Start

### Local dev (Docker Compose)

```bash
# Spin up Postgres, Redis, Kafka, Consul, Jaeger, Prometheus, Grafana
docker compose up -d

# Run the daemon (in another terminal)
pip install -e ".[full]"
interface-core
```

### Embedded / programmatic

```python
import asyncio
from interface_core import InterfaceCore

async def main():
    core = InterfaceCore()

    # Register nodes manually (or let discovery find them via env config)
    producer_id = core.register_node(
        protocol="mqtt",
        endpoint="mqtt://broker:1883/sensors/temp",
        fields={"temperature": "float", "unit": "str", "device_id": "str"},
        tags=["telemetry", "iot"],
        role="producer",
        emission_rate_hz=10.0,
    )
    consumer_id = core.register_node(
        protocol="kafka",
        endpoint="kafka://broker:9092/processed-temps",
        fields={"temperature": "float", "device_id": "str"},
        tags=["telemetry"],
        role="consumer",
    )

    # Start all phases
    await core.start()

    # Interface Core will now autonomously:
    # 1. Score the producer/consumer pair
    # 2. Evaluate policy
    # 3. Synthesize an MQTT→Kafka adapter with field mapping
    # 4. Instantiate a live bridge with circuit breaker + backpressure
    # 5. Monitor and hot-reload if contracts drift

    # Check status
    print(core.status())

    # Operator approval (if a bridge was staged)
    # core.approve_bridge(producer_id, consumer_id)

    await core.wait()

asyncio.run(main())
```

### Configure via environment

```bash
# Discovery targets
IC_REST_TARGETS=http://localhost:8080,http://localhost:3000
IC_KAFKA_TOPICS=sensor-data,market-feed
IC_MQTT_BROKERS=mqtt://localhost:1883
IC_SOCKET_PORTS=8080,9000,5000

# Scoring
IC_MIN_SCORE=0.72           # minimum composite score to attempt bridging
IC_DRIFT_DELTA=0.15         # contract change fraction that triggers drift handling

# Policy
IC_BLACKLIST=node_id_1,node_id_2
IC_MAX_BRIDGES_PER_NODE=16

# Infrastructure
IC_REDIS_URL=redis://localhost:6379/0
IC_POSTGRES_DSN=postgresql://ic:ic@localhost:5432/interface_core
IC_KAFKA_BROKERS=localhost:9092
IC_OTEL_ENDPOINT=http://localhost:4317
IC_SECRET_KEY=your-secret-here
```

---

## Supported Protocols (Discovery)

| Protocol | Contract Inference Method |
|----------|--------------------------|
| REST / HTTP | OPTIONS → OpenAPI/Swagger → traffic sampling |
| Kafka | Schema Registry → Avro/JSON sniffing |
| MQTT | Payload sampling on connect |
| gRPC | Server reflection API |
| MAVLink | Protocol definition registry |
| WebSocket | Message sampling |
| File | JSON parse → line-delimited fallback |
| TCP Socket | Banner grab + byte pattern fingerprinting |
| AMQP | Queue introspection |

---

## Adapter Synthesis

Three-layer pipeline:

1. **Protocol shim** — pre-built template for common crossings (MQTT→Kafka, MAVLink→gRPC, REST→ZMQ, …)
2. **Field mapper** — ML-aligned field-to-field transforms: `direct`, `cast:<type>`, `extract:<path>`, `flatten`, `enrich:<literal>`
3. **Backpressure** — token bucket (smooth rate limiting), leaky bucket (bursty producers), or passthrough

Adapters are hot-reloadable: new adapter instances are atomically swapped into live bridges without pausing data flow.

---

## Resilience Stack (per bridge)

```
Retry           exponential backoff, 100ms→30s, jitter, max 5 attempts
Circuit Breaker trips at 40% error rate / 10s window; half-open probe after 30s
Ring Buffer     10,000 message buffer absorbs upstream backpressure during circuit trip
Idempotency     TTL bloom filter (30s) rejects duplicate message IDs
Provenance      HMAC-SHA256 signs every bridged payload for audit trail
```

---

## Observability

All four OpenTelemetry signals are emitted per bridge:

- **Traces** — end-to-end latency per message, span per adapter transform step → Jaeger
- **Metrics** — throughput (msg/s, byte/s), error rate, buffer fill, circuit state → Prometheus
- **Logs** — lifecycle events (create, teardown, reload, circuit trip) as structured JSON
- **Drift histogram** — delta per node over time, used for ML fine-tuning signal

Dashboards available in Grafana at `http://localhost:3000` after `docker compose up`.

---

## Stack

| Layer | Tool |
|-------|------|
| Event fabric | ZeroMQ + Kafka |
| Service discovery | Consul |
| Semantic matching | sentence-transformers (all-MiniLM-L6-v2) |
| Schema validation | Pydantic |
| Observability | OpenTelemetry → Prometheus + Jaeger |
| State | PostgreSQL + Redis |
| Container | Docker Compose (dev) / Kubernetes + Helm (prod) |

---

## Running Tests

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

Tests run fully offline — no external services required. The ML model falls back
to a hashing-trick embedder if `sentence-transformers` is not installed.

---

## Project Structure

```
interface_core/
├── __init__.py
├── main.py              # InterfaceCore daemon + entry point
├── config.py            # All tunables + protocol compat matrix
├── models.py            # Pydantic domain models
├── registry.py          # NodeRegistry + CapabilityGraph + DriftLog
├── event_bus.py         # ZeroMQ pub/sub (in-process fallback)
├── telemetry.py         # OpenTelemetry + Prometheus façade
├── ml/
│   └── matcher.py       # Embedder + FieldAligner + SemanticMatcher
├── adapters/
│   └── base.py          # Adapter base + templates + backpressure
└── phases/
    ├── discovery.py     # Discovery phase (500ms scan)
    ├── matching.py      # Matching phase + BridgeQueue
    ├── policy.py        # Policy engine + AuditLog
    ├── synthesis.py     # Synthesis phase + PolicyPhase
    └── lifecycle.py     # Lifecycle + DriftWatch + CircuitBreaker
tests/
    └── test_interface_core.py
docker-compose.yml
Dockerfile
requirements.txt
pyproject.toml
```
