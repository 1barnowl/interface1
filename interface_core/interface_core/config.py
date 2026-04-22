"""
Interface Core — Configuration
All tunables via environment variables; no external deps required.
"""
from __future__ import annotations
import os
from typing import Dict, List


def _s(key: str, default: str) -> str:  return os.environ.get(key, default)
def _i(key: str, default: int) -> int:
    try: return int(os.environ.get(key, default))
    except (ValueError, TypeError): return default
def _f(key: str, default: float) -> float:
    try: return float(os.environ.get(key, default))
    except (ValueError, TypeError): return default
def _ls(key: str, default: List[str]) -> List[str]:
    v = os.environ.get(key, "")
    return [x.strip() for x in v.split(",") if x.strip()] if v else list(default)
def _li(key: str, default: List[int]) -> List[int]:
    v = os.environ.get(key, "")
    try: return [int(x.strip()) for x in v.split(",") if x.strip()] if v else list(default)
    except ValueError: return list(default)


class _Settings:
    node_name                    = _s("IC_NODE_NAME",   "interface-core-0")
    secret_key                   = _s("IC_SECRET_KEY",  "change-me-in-prod")
    discovery_interval_ms        = _i("IC_DISCOVERY_INTERVAL_MS",   500)
    matching_interval_ms         = _i("IC_MATCHING_INTERVAL_MS",   1000)
    lifecycle_interval_ms        = _i("IC_LIFECYCLE_INTERVAL_MS",  1000)
    drift_watch_interval_ms      = _i("IC_DRIFT_WATCH_INTERVAL_MS",5000)
    operator_approval_timeout_s  = _i("IC_APPROVAL_TIMEOUT_S",       60)
    min_composite_score          = _f("IC_MIN_SCORE",    0.72)
    contract_drift_delta         = _f("IC_DRIFT_DELTA",  0.15)
    score_weights: Dict[str, float] = {
        "semantic_similarity": 0.40,
        "protocol_compat":     0.20,
        "latency_score":       0.15,
        "risk_score":          0.15,
        "history_score":       0.10,
    }
    blacklisted_nodes            = _ls("IC_BLACKLIST",  [])
    whitelisted_nodes            = _ls("IC_WHITELIST",  [])
    max_bridges_per_node         = _i("IC_MAX_BRIDGES_PER_NODE", 16)
    require_approval_above_risk  = _f("IC_STAGE_RISK_THRESHOLD", 0.7)
    retry_max                    = _i("IC_RETRY_MAX",             5)
    retry_base_ms                = _i("IC_RETRY_BASE_MS",       100)
    retry_ceil_ms                = _i("IC_RETRY_CEIL_MS",      30000)
    circuit_error_threshold      = _f("IC_CIRCUIT_THRESHOLD",   0.40)
    circuit_window_s             = _i("IC_CIRCUIT_WINDOW_S",      10)
    circuit_halfopen_s           = _i("IC_CIRCUIT_HALFOPEN_S",    30)
    buffer_size                  = _i("IC_BUFFER_SIZE",        10000)
    idempotency_ttl_s            = _i("IC_IDEMPOTENCY_TTL_S",     30)
    synthesis_max_retries        = _i("IC_SYNTHESIS_RETRIES",      3)
    latency_degradation_factor   = _f("IC_LATENCY_FACTOR",       2.0)
    lifecycle_error_threshold    = _f("IC_LIFECYCLE_ERROR_THRESHOLD", 0.4)
    reconnect_max                = _i("IC_RECONNECT_MAX",           3)
    embedding_model              = _s("IC_EMBED_MODEL",  "all-MiniLM-L6-v2")
    embed_batch_timeout_ms       = _i("IC_EMBED_BATCH_MS",         10)
    embed_dim                    = _i("IC_EMBED_DIM",             384)
    redis_url                    = _s("IC_REDIS_URL",    "redis://localhost:6379/0")
    postgres_dsn                 = _s("IC_POSTGRES_DSN","postgresql://ic:ic@localhost:5432/interface_core")
    kafka_brokers                = _s("IC_KAFKA_BROKERS","localhost:9092")
    consul_host                  = _s("IC_CONSUL_HOST",  "localhost")
    consul_port                  = _i("IC_CONSUL_PORT",  8500)
    zmq_pub_addr                 = _s("IC_ZMQ_PUB",      "tcp://127.0.0.1:5555")
    zmq_sub_addr                 = _s("IC_ZMQ_SUB",      "tcp://127.0.0.1:5555")
    otel_endpoint                = _s("IC_OTEL_ENDPOINT","http://localhost:4317")
    prometheus_port              = _i("IC_PROMETHEUS_PORT", 9090)
    scan_rest_targets            = _ls("IC_REST_TARGETS",  [])
    scan_kafka_topics            = _ls("IC_KAFKA_TOPICS",  [])
    scan_mqtt_brokers            = _ls("IC_MQTT_BROKERS",  [])
    scan_socket_ports            = _li("IC_SOCKET_PORTS", [8080, 8443, 3000, 5000, 7000, 9000, 4000])
    scan_grpc_targets            = _ls("IC_GRPC_TARGETS",  [])

    # ── HTTP API ─────────────────────────────
    api_host                     = _s("IC_API_HOST", "0.0.0.0")
    api_port                     = _i("IC_API_PORT", 8000)
    api_key                      = _s("IC_API_KEY",  "")    # empty = open mode (no auth)

    # ── Lifecycle ─────────────────────────────────────────────────
    node_stale_s                 = _i("IC_NODE_STALE_S", 30)


# ── Protocol compatibility matrix ────────────────────────────────────────────
PROTOCOL_COMPAT_MATRIX: Dict[str, Dict[str, float]] = {
    "rest":      {"rest": 1.0, "grpc": 0.6, "websocket": 0.7, "mqtt": 0.5, "kafka": 0.6, "zeromq": 0.5, "file": 0.8, "socket": 0.5, "process": 0.4, "database": 0.7, "mavlink": 0.3, "amqp": 0.6, "unknown": 0.2},
    "grpc":      {"rest": 0.6, "grpc": 1.0, "websocket": 0.5, "mqtt": 0.4, "kafka": 0.5, "zeromq": 0.6, "file": 0.5, "socket": 0.6, "process": 0.4, "database": 0.5, "mavlink": 0.3, "amqp": 0.4, "unknown": 0.2},
    "websocket": {"rest": 0.7, "grpc": 0.5, "websocket": 1.0, "mqtt": 0.7, "kafka": 0.6, "zeromq": 0.6, "file": 0.4, "socket": 0.7, "process": 0.3, "database": 0.4, "mavlink": 0.4, "amqp": 0.5, "unknown": 0.2},
    "mqtt":      {"rest": 0.5, "grpc": 0.4, "websocket": 0.7, "mqtt": 1.0, "kafka": 0.8, "zeromq": 0.7, "file": 0.4, "socket": 0.5, "process": 0.3, "database": 0.4, "mavlink": 0.6, "amqp": 0.8, "unknown": 0.2},
    "kafka":     {"rest": 0.6, "grpc": 0.5, "websocket": 0.6, "mqtt": 0.8, "kafka": 1.0, "zeromq": 0.7, "file": 0.6, "socket": 0.5, "process": 0.4, "database": 0.7, "mavlink": 0.3, "amqp": 0.8, "unknown": 0.2},
    "zeromq":    {"rest": 0.5, "grpc": 0.6, "websocket": 0.6, "mqtt": 0.7, "kafka": 0.7, "zeromq": 1.0, "file": 0.5, "socket": 0.7, "process": 0.6, "database": 0.4, "mavlink": 0.4, "amqp": 0.6, "unknown": 0.2},
    "mavlink":   {"rest": 0.3, "grpc": 0.3, "websocket": 0.4, "mqtt": 0.6, "kafka": 0.3, "zeromq": 0.4, "file": 0.3, "socket": 0.5, "process": 0.3, "database": 0.3, "mavlink": 1.0, "amqp": 0.3, "unknown": 0.1},
    "amqp":      {"rest": 0.6, "grpc": 0.4, "websocket": 0.5, "mqtt": 0.8, "kafka": 0.8, "zeromq": 0.6, "file": 0.5, "socket": 0.4, "process": 0.3, "database": 0.5, "mavlink": 0.3, "amqp": 1.0, "unknown": 0.2},
    "file":      {"rest": 0.8, "grpc": 0.5, "websocket": 0.4, "mqtt": 0.4, "kafka": 0.6, "zeromq": 0.5, "file": 1.0, "socket": 0.5, "process": 0.7, "database": 0.7, "mavlink": 0.2, "amqp": 0.5, "unknown": 0.3},
    "socket":    {"rest": 0.5, "grpc": 0.6, "websocket": 0.7, "mqtt": 0.5, "kafka": 0.5, "zeromq": 0.7, "file": 0.5, "socket": 1.0, "process": 0.6, "database": 0.4, "mavlink": 0.5, "amqp": 0.4, "unknown": 0.3},
    "process":   {"rest": 0.4, "grpc": 0.4, "websocket": 0.3, "mqtt": 0.3, "kafka": 0.4, "zeromq": 0.6, "file": 0.7, "socket": 0.6, "process": 1.0, "database": 0.5, "mavlink": 0.2, "amqp": 0.3, "unknown": 0.3},
    "database":  {"rest": 0.7, "grpc": 0.5, "websocket": 0.4, "mqtt": 0.4, "kafka": 0.7, "zeromq": 0.4, "file": 0.7, "socket": 0.4, "process": 0.5, "database": 1.0, "mavlink": 0.2, "amqp": 0.5, "unknown": 0.2},
    "unknown":   {"rest": 0.2, "grpc": 0.2, "websocket": 0.2, "mqtt": 0.2, "kafka": 0.2, "zeromq": 0.2, "file": 0.3, "socket": 0.3, "process": 0.3, "database": 0.2, "mavlink": 0.1, "amqp": 0.2, "unknown": 0.5},
}

settings = _Settings()
