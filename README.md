# Interface

## Core Idea

Interface functions as an autonomous middleware daemon that continuously discovers, maps, and bridges disparate software endpoints across heterogeneous protocols—REST, gRPC, MQTT, Kafka, WebSocket, MAVLink, files, and sockets—without manual configuration. It ingests raw telemetry from passive sniffing, API introspection, process inspection, and message broker enumeration to infer canonical data contracts (schema, encoding, emission rate, semantic domain) for each observed node. A semantic matching layer embeds these contracts into a dense vector space using fine‑tuned transformer models, scoring producer‑consumer pairs by both structural compatibility and meaning alignment. Candidates passing policy evaluation—enforced via Open Policy Agent with rules for authorization, rate limits, geofencing, and manual approval gates—trigger the synthesis of WASM‑sandboxed adapters that perform protocol translation, field mapping, type coercion, and backpressure control. Each bridge is instantiated with a full resilience stack: exponential backoff retries, circuit breakers, bounded ring buffers, idempotency guards, and cryptographically signed provenance. The system continuously monitors for contract drift, hot‑reloading adapters when feasible or tearing down and re‑queuing bridges when changes exceed thresholds. By operating as a proactive, self‑repairing integration fabric, This glue of interfaces eliminates manual wiring, reduces operational friction, and establishes a verifiable, policy‑compliant data plane across the entire software ecosystem.

## Constituent Subsystems

- Discovery & Contract Inference Engine
- Semantic Matcher & Policy Governor
- Adapter Synthesis & Bridge Orchestrator
- Resilience & Lifecycle Supervisor

## Comprehensive Capabilities

- Passive and active discovery across REST, gRPC, GraphQL, MQTT, Kafka, AMQP, WebSocket, MAVLink, CoAP, file systems, and Unix sockets
- Process inspection and file descriptor enumeration to identify unadvertised data producers and consumers
- Multi‑strategy contract inference combining OpenAPI/Swagger parsing, gRPC reflection, traffic sampling, and statistical schema induction
- Semantic embedding of inferred contracts using sentence‑transformer models for meaning‑based matching
- Composite scoring of bridge candidates incorporating semantic similarity, protocol compatibility, field alignment confidence, and policy risk
- Open Policy Agent integration for policy‑driven gating: allow/deny, rate limits, blacklist/whitelist, geofencing, and manual approval workflows
- Template‑driven protocol translation covering common crossings (MQTT→Kafka, REST→gRPC, MAVLink→WebSocket, file tail→stream)
- ML‑assisted field mapping with attention‑based alignment models generating typed transformation pipelines (rename, cast, extract, flatten, enrich)
- WASM‑based adapter compilation enabling sandboxed, hot‑reloadable execution and cross‑language portability
- Backpressure controllers including token bucket, leaky bucket, and sliding window rate limiters
- Resilience stack per bridge: exponential backoff retries with jitter, circuit breaker on error rate/latency, bounded ring buffer, and TTL‑based idempotency guard
- Cryptographic signing of bridged payloads with HMAC‑SHA256 for end‑to‑end provenance and auditability
- Continuous drift detection with configurable delta thresholds; automatic adapter re‑synthesis and hot‑reload on contract change
- Graceful teardown and re‑queuing of bridges when drift exceeds repair capability
- OpenTelemetry instrumentation exporting traces, metrics, and structured logs to Prometheus, Jaeger, and Loki
- Real‑time topology visualization of capability graph, active bridges, drift events, and system health
- Horizontal scalability via per‑phase worker pools with sharded node registry and distributed synthesis queue
- Leader election and durable state persistence in PostgreSQL and Redis for high‑availability deployments
- Plugin architecture for custom protocol adapters, field mappers, and ML models
- Operator console for manual bridge approval, field mapping correction, and policy override


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
