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

## Getting Started

### Prerequisites

- Docker & Docker Compose (for local development)
- Rust 1.75+ or Go 1.21+ (if building from source)
- PostgreSQL 14+ and Redis 7+ (provided via Docker Compose)
- Consul (optional, for service discovery)

### Quick Start with Docker Compose

```bash
git clone https://github.com/yourusername/glue.git
cd glue
docker-compose up -d


# Glue — Technical Deep Dive & Operational Manual

> **Companion Document**  
> This document expands on the core README with in‑depth architecture, configuration reference, API specifications, and operational guidance. For a high‑level overview, please see the [main README](README.md).

---

## Table of Contents

1. [Architecture in Depth](#architecture-in-depth)
2. [Component Interaction & Data Flow](#component-interaction--data-flow)
3. [Configuration Reference](#configuration-reference)
4. [API Reference](#api-reference)
5. [Adapter Development Kit](#adapter-development-kit)
6. [Security Model](#security-model)
7. [Performance Tuning & Scaling](#performance-tuning--scaling)
8. [Monitoring & Observability](#monitoring--observability)
9. [Troubleshooting Guide](#troubleshooting-guide)
10. [Example Use Cases](#example-use-cases)
11. [Contributor’s Guide](#contributors-guide)
12. [Roadmap Details](#roadmap-details)

---

## Architecture in Depth

### Internal Subsystems

Glue is composed of six loosely coupled subsystems, each running as an independent tokio (Rust) or goroutine (Go) task. Communication between subsystems occurs via bounded async channels and a shared state protected by fine‑grained locks.



### Shared State Details

| Structure          | Implementation                          | Concurrency Model                     |
|--------------------|-----------------------------------------|---------------------------------------|
| `NodeRegistry`     | DashMap (Rust) / `sync.Map` (Go)        | Lock‑free read, fine‑grained write    |
| `CapabilityGraph`  | `petgraph::Graph` + `RwLock`            | Read‑heavy, write‑infrequent          |
| `ActiveBridges`    | `Arc<RwLock<HashMap>>`                  | Read/write lock with short critical   |
| `BridgeQueue`      | BinaryHeap + `Mutex`                    | Single consumer, multiple producers   |
| `DriftLog`         | PostgreSQL timeseries table             | Async writes, periodic flush          |

### State Rehydration on Restart

1. **Redis Cache** holds the last known contract (TTL 7 days) for each discovered node.
2. On startup, Discovery subsystem replays active node list from Redis and re‑runs contract inference to refresh.
3. Active bridges are **not** blindly restored. Instead, the system re‑evaluates previous bridge candidates through the full matching and policy pipeline.
4. This ensures that no stale or policy‑violating bridge survives a restart.

---

## Component Interaction & Data Flow

### Discovery → Matching






---

## Configuration Reference

### Full Configuration Schema

```yaml
glue:
  version: 1

  instance:
    id: "glue-primary"                 # Unique instance identifier
    data_dir: "/var/lib/glue"          # Persistence directory
    log_level: "info"                  # debug, info, warn, error

  discovery:
    enabled: true
    interval_ms: 500
    scanners:
      network:
        enabled: true
        passive: true
        interfaces: ["lo", "eth0", "docker0"]
        promiscuous: false
        bpf_filter: "tcp or udp"       # Optional BPF filter
      api:
        enabled: true
        probe_paths:
          - "/openapi.json"
          - "/swagger.json"
          - "/.well-known/ai-plugin.json"
          - "/graphql"
        introspection_timeout_ms: 2000
      process:
        enabled: true
        scan_interval_sec: 10
        include_namespaces: ["host"]    # For containerized deployments
      filesystem:
        enabled: true
        watch_paths:
          - "/var/log"
          - "/tmp/scans"
        patterns: ["*.csv", "*.json", "*.xml", "*.log"]
      message_bus:
        brokers:
          - "localhost:9092"            # Kafka
          - "nats://localhost:4222"     # NATS
        topic_discovery: true
        schema_registry: "http://localhost:8081"
      consul:
        address: "http://localhost:8500"
        service_prefix: "glue-"

  inference:
    contract_sampling_size: 100        # Number of messages to sample
    min_confidence_threshold: 0.6
    schema_inference:
      json_max_depth: 10
      infer_nullable: true
      infer_enums: true
    semantic_embedding:
      model_path: "/models/all-MiniLM-L6-v2.onnx"
      batch_size: 32
      max_batch_latency_ms: 10
      use_gpu: false
    protocol_definitions:
      mavlink: "/defs/mavlink.xml"
      protobuf: ["/defs/*.proto"]

  matching:
    enabled: true
    score_threshold: 0.72
    weights:
      semantic: 0.45
      protocol: 0.25
      schema_alignment: 0.15
      policy_risk: 0.10
      historical_success: 0.05
    protocol_compatibility_matrix:
      rest: { rest: 1.0, grpc: 0.7, mqtt: 0.5, kafka: 0.6 }
      grpc: { grpc: 1.0, rest: 0.7, kafka: 0.8 }
      # ... (full matrix defined in code)
    candidate_queue:
      max_size: 10000
      dedup_window_sec: 60

  policy:
    opa:
      url: "http://localhost:8181/v1/data/glue/authz"
      policy_path: "/policies/glue.rego"
      allow_dry_run: false
    approval:
      enabled: true
      ttl_seconds: 3600               # How long to wait for manual approval
      notification:
        webhook_url: "https://hooks.slack.com/..."
        email_smtp: "smtp.example.com:587"
    builtin_rules:
      forbid_circular: true
      forbid_external_egress: true    # Block outbound internet unless whitelisted
      rate_limit_default: 1000        # msg/sec per bridge

  synthesis:
    wasm:
      runtime: "wasmtime"             # or "extism"
      cache_dir: "/var/cache/glue/wasm"
      max_module_size_mb: 10
      compilation_timeout_sec: 30
    adapter_templates:
      - name: "rest_to_kafka"
        path: "/adapters/rest_to_kafka.wasm"
      - name: "mqtt_to_postgres"
        path: "/adapters/mqtt_to_postgres.wasm"
      # ... more built‑ins
    field_mapping:
      model: "attention_alignment"
      confidence_threshold: 0.8       # Require manual review below this
      unit_conversion_db: "/data/units.json"
    backpressure:
      default_strategy: "leaky_bucket"
      buffer_size: 10000
      rate_limit:
        default_per_second: 5000

  resilience:
    retry:
      max_attempts: 5
      initial_backoff_ms: 100
      max_backoff_ms: 30000
      jitter: true
    circuit_breaker:
      error_threshold_percent: 40
      window_sec: 10
      half_open_probes: 3
      recovery_timeout_sec: 30
    idempotency:
      ttl_sec: 60
      bloom_filter_size: 1000000
    dead_letter:
      enabled: true
      storage: "postgres"             # or "kafka"

  drift:
    detection_interval_sec: 5
    delta_threshold: 0.15
    auto_heal: true
    max_reload_attempts: 3

  observability:
    opentelemetry:
      endpoint: "http://localhost:4317"
      protocol: "grpc"
      traces:
        enabled: true
        sampling_ratio: 1.0
      metrics:
        enabled: true
        export_interval_ms: 10000
      logs:
        enabled: true
    prometheus:
      listen_address: ":9090"
    dashboard:
      enabled: true
      listen_address: ":8080"
      auth: "basic"                   # none, basic, oidc

  storage:
    postgres:
      url: "postgres://glue:glue@localhost:5432/glue"
      pool_size: 10
      drift_log_retention_days: 7
    redis:
      url: "redis://localhost:6379"
      pool_size: 5
      contract_cache_ttl_sec: 604800  # 7 days

  security:
    mtls:
      enabled: false
      ca_cert: "/certs/ca.crt"
      cert: "/certs/glue.crt"
      key: "/certs/glue.key"
    provenance_signing:
      enabled: true
      hmac_key: "env:GLUE_HMAC_KEY"
    encryption:
      in_transit: "tls"
      at_rest: false                 # Future: encrypt drift log
