"""
Interface Core — Telemetry
OpenTelemetry setup: traces (Jaeger), metrics (Prometheus), logs (structured JSON).
Falls back to no-op if opentelemetry-sdk is not installed.
"""

from __future__ import annotations
import json
import logging
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Try to import OpenTelemetry
# ─────────────────────────────────────────────

try:
    from opentelemetry import trace, metrics
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    try:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
        _OTLP_AVAILABLE = True
    except ImportError:
        _OTLP_AVAILABLE = False
    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False


# ─────────────────────────────────────────────
# Prometheus (optional)
# ─────────────────────────────────────────────

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server  # type: ignore
    _PROM_AVAILABLE = True
except ImportError:
    _PROM_AVAILABLE = False


# ─────────────────────────────────────────────
# Telemetry façade
# ─────────────────────────────────────────────

class Telemetry:
    """
    Unified telemetry surface used by all phases.
    Gracefully degrades if SDKs are absent.
    """

    def __init__(self, otel_endpoint: str, prometheus_port: int) -> None:
        import os as _os
        self._enabled = _os.environ.get('IC_TELEMETRY_ENABLED', 'true').lower() != 'false'
        if not self._enabled:
            logger.info("Telemetry disabled (IC_TELEMETRY_ENABLED=false)")
            return
        self._bridges: Dict[str, Any] = {}
        self._bridge_metrics: Dict[str, Dict] = {}

        self._tracer = None
        self._meter  = None

        # ── OpenTelemetry ──────────────────────────────────────────────────
        if _OTEL_AVAILABLE:
            tp = TracerProvider()
            if _OTLP_AVAILABLE:
                try:
                    exporter = OTLPSpanExporter(endpoint=otel_endpoint, insecure=True)
                    # schedule_delay=60s, max_export_batch_size=512 — reduces retry noise
                    tp.add_span_processor(BatchSpanProcessor(
                        exporter,
                        schedule_delay_millis=60000,
                        max_export_batch_size=512,
                    ))
                    logger.info("OTLP trace exporter → %s", otel_endpoint)
                except Exception as exc:
                    logger.warning("OTLP exporter init failed: %s", exc)
            trace.set_tracer_provider(tp)
            self._tracer = trace.get_tracer("interface_core")

            if _OTLP_AVAILABLE:
                try:
                    metric_reader = PeriodicExportingMetricReader(
                        OTLPMetricExporter(endpoint=otel_endpoint, insecure=True)
                    )
                    mp = MeterProvider(metric_readers=[metric_reader])
                    metrics.set_meter_provider(mp)
                    self._meter = metrics.get_meter("interface_core")
                except Exception as exc:
                    logger.warning("OTLP metric exporter init failed: %s", exc)
        else:
            logger.info("opentelemetry-sdk not installed; tracing disabled")

        # ── Prometheus ────────────────────────────────────────────────────
        # Silence the opentelemetry exporter logger — it logs WRN+ERR every 60s
        # when the OTLP collector endpoint is unavailable, which floods the terminal.
        # Users can re-enable by setting IC_OTEL_LOG_LEVEL=WARNING in their env.
        import logging as _lmod
        _otel_log_level = _lmod.WARNING if not __import__('os').environ.get('IC_OTEL_LOG_LEVEL') else getattr(_lmod, __import__('os').environ['IC_OTEL_LOG_LEVEL'], _lmod.WARNING)
        for _noisy in ('opentelemetry.exporter', 'opentelemetry.sdk.metrics', 'opentelemetry.sdk.trace'):
            _lmod.getLogger(_noisy).setLevel(max(_otel_log_level, _lmod.ERROR))

        if _PROM_AVAILABLE:
            try:
                start_http_server(prometheus_port)
                logger.info("Prometheus metrics on :%d", prometheus_port)
                self._prom_msg_total = Counter(
                    "ic_bridge_messages_total", "Total messages through a bridge",
                    ["bridge_id", "producer_id", "consumer_id"]
                )
                self._prom_err_rate = Gauge(
                    "ic_bridge_error_rate", "Bridge error rate",
                    ["bridge_id"]
                )
                self._prom_latency = Histogram(
                    "ic_bridge_latency_ms", "Bridge end-to-end latency",
                    ["bridge_id"]
                )
                self._prom_active        = Gauge("ic_active_bridges", "Number of live bridges")
                self._prom_nodes          = Gauge("ic_registered_nodes", "Registered nodes")
                self._prom_buffer_fill    = Gauge(
                    "ic_bridge_buffer_fill",
                    "Ring buffer fill fraction (0–1)",
                    ["bridge_id"]
                )
                self._prom_circuit_trips  = Counter(
                    "ic_bridge_circuit_trips_total",
                    "Number of circuit breaker trips per bridge",
                    ["bridge_id"]
                )
                self._prom_drift_events   = Counter(
                    "ic_contract_drift_events_total",
                    "Number of contract drift events per node",
                    ["node_id"]
                )
            except Exception as exc:
                logger.warning("Prometheus init failed: %s", exc)
                _PROM_AVAILABLE_ = False
        else:
            logger.info("prometheus_client not installed; metrics endpoint disabled")

    def register_bridge(self, bridge: Any) -> None:
        if not getattr(self, "_enabled", True):
            return
        self._bridges[bridge.id] = bridge
        self._bridge_metrics[bridge.id] = {
            "msg_count": 0, "err_count": 0, "latencies": [],
        }
        if _PROM_AVAILABLE:
            try:
                self._prom_active.inc()
            except Exception:
                pass

    def unregister_bridge(self, bridge_id: str) -> None:
        if not getattr(self, "_enabled", True):
            return
        self._bridges.pop(bridge_id, None)
        self._bridge_metrics.pop(bridge_id, None)
        if _PROM_AVAILABLE:
            try:
                self._prom_active.dec()
            except Exception:
                pass

    def record_message(
        self,
        bridge_id: str,
        producer_id: str,
        consumer_id: str,
        latency_ms: float,
        success: bool,
    ) -> None:
        if not getattr(self, "_enabled", True):
            return
        m = self._bridge_metrics.get(bridge_id)
        if m:
            if success:
                m["msg_count"] += 1
            else:
                m["err_count"] += 1
            m["latencies"].append(latency_ms)
            if len(m["latencies"]) > 1000:
                m["latencies"] = m["latencies"][-1000:]

        if _PROM_AVAILABLE:
            try:
                self._prom_msg_total.labels(
                    bridge_id=bridge_id,
                    producer_id=producer_id,
                    consumer_id=consumer_id,
                ).inc()
                total = m["msg_count"] + m["err_count"] if m else 1
                self._prom_err_rate.labels(bridge_id=bridge_id).set(
                    m["err_count"] / total if m and total else 0
                )
                self._prom_latency.labels(bridge_id=bridge_id).observe(latency_ms)
            except Exception:
                pass

    def span(self, name: str):
        """Context manager for a trace span (no-op if tracing disabled)."""
        if self._tracer:
            return self._tracer.start_as_current_span(name)
        return _NoopSpan()

    def update_bridge_metrics(
        self,
        bridge_id:    str,
        buffer_fill:  float,
        circuit_open: bool,
    ) -> None:
        if not getattr(self, "_enabled", True):
            return
        """
        Called by LifecyclePhase._tick() to update buffer fill and circuit state.
        These are runtime signals not available at message time.
        """
        if not _PROM_AVAILABLE:
            return
        try:
            self._prom_buffer_fill.labels(bridge_id=bridge_id).set(buffer_fill)
            # Circuit breaker open state is logged but not incremented here
            # (trips are recorded by record_circuit_trip)
        except Exception:
            pass

    def record_circuit_trip(self, bridge_id: str) -> None:
        if not getattr(self, "_enabled", True):
            return
        """Called by CircuitBreaker when it trips open."""
        if not _PROM_AVAILABLE:
            return
        try:
            self._prom_circuit_trips.labels(bridge_id=bridge_id).inc()
        except Exception:
            pass

    def record_drift_event(self, node_id: str) -> None:
        if not getattr(self, "_enabled", True):
            return
        """Called by DiscoveryPhase when contract drift is detected."""
        if not _PROM_AVAILABLE:
            return
        try:
            self._prom_drift_events.labels(node_id=node_id).inc()
        except Exception:
            pass

    def snapshot(self) -> Dict:
        if not getattr(self, "_enabled", True):
            return
        """Return a JSON-serialisable telemetry snapshot."""
        bridges = {}
        for bid, m in self._bridge_metrics.items():
            lats = sorted(m["latencies"])
            total = m["msg_count"] + m["err_count"]
            bridges[bid] = {
                "messages": m["msg_count"],
                "errors":   m["err_count"],
                "error_rate": m["err_count"] / total if total else 0,
                "latency_p50": lats[len(lats) // 2] if lats else 0,
                "latency_p99": lats[int(len(lats) * 0.99)] if lats else 0,
            }
        return {"timestamp": time.time(), "bridges": bridges}

    def log_event(self, event_type: str, payload: Dict) -> None:
        logger.info(json.dumps({
            "event": event_type,
            "ts":    time.time(),
            **payload,
        }))


class _NoopSpan:
    def __enter__(self): return self
    def __exit__(self, *_): pass
