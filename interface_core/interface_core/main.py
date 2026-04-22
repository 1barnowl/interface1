"""
Interface Core — Main Daemon
Boots all phases in parallel as asyncio tasks.

Improvements wired here:
  #1  BridgeRunner I/O loops (via SynthesisPhase + shared runners dict)
  #2  BridgeStore persistence (Redis + memory fallback)
  #3  HTTP API server (aiohttp, graceful no-op if absent)
  #4  Full AdapterSpec in BRIDGE_LIVE events (in SynthesisPhase)
  #5  Score decay in BridgeQueue (in MatchingPhase)
"""

from __future__ import annotations
import asyncio
import logging
import signal
import sys
from typing import Dict, List, Optional, Tuple

from .config import settings
from .event_bus import make_event_bus
from .ml.matcher import SemanticMatcher
from .models import DataContract, FieldSchema, Node, NodeRole, Protocol
from .phases.discovery import DiscoveryPhase
from .phases.lifecycle import DriftWatchPhase, LifecyclePhase
from .phases.matching import BridgeQueue, MatchingPhase
from .phases.policy import PolicyEngine
from .phases.synthesis import PolicyPhase, SynthesisPhase, SynthesisQueue
from .registry import CapabilityGraph, DriftLog, NodeRegistry
from .store import BridgeStore
from .persistence import PostgresPersistence
from .consul_discovery import ConsulDiscovery
from .phases.process_scanner import ProcessScanner
from .telemetry import Telemetry

from .logging_setup import setup_logging, print_banner, print_phase_header, print_status_table
setup_logging(level="INFO")
logger = logging.getLogger("interface_core")


class InterfaceCore:
    """
    Top-level daemon.

    Usage:
        core = InterfaceCore()
        await core.start()     # boots all phases, restores bridges, starts API
        await core.wait()      # blocks until shutdown signal
        await core.stop()      # graceful teardown

    One-liner:
        asyncio.run(InterfaceCore().run_forever())
    """

    def __init__(self) -> None:
        # ── Shared state ───────────────────────────────────────────────────
        self.registry        = NodeRegistry()
        self.graph           = CapabilityGraph()
        self.drift_log       = DriftLog()
        # 3-tuple: (Bridge, Adapter, BridgeRuntime|None)
        self.active_bridges: Dict[Tuple, Tuple] = {}
        # Shared runners dict — keyed by "producer_id:consumer_id"
        self._runners: Dict[str, Any] = {}

        # ── Infra ──────────────────────────────────────────────────────────
        self.event_bus  = make_event_bus(settings.zmq_pub_addr, settings.zmq_sub_addr)
        self.ml_matcher = SemanticMatcher(settings.embedding_model)
        self.telemetry  = Telemetry(settings.otel_endpoint, settings.prometheus_port)
        # Improvement #2: persistent bridge store
        self.store      = BridgeStore(settings.redis_url)
        # Postgres: durable drift log + bridge checkpoints
        self.pg              = PostgresPersistence(settings.postgres_dsn)

        # Consul service registry scanner
        self.consul          = ConsulDiscovery(
            host=settings.consul_host, port=settings.consul_port,
            registry=self.registry, graph=self.graph,
            drift_log=self.drift_log, event_bus=self.event_bus,
            ml_matcher=self.ml_matcher,
        )

        # Local process table scanner
        # Exclude the API port so the scanner doesn't discover itself
        _api_port = getattr(settings, 'api_port', 8000)
        self.process_scanner = ProcessScanner(
            registry=self.registry, graph=self.graph,
            drift_log=self.drift_log, event_bus=self.event_bus,
            ml_matcher=self.ml_matcher,
            exclude_ports={_api_port},
        )

        # ── Queues ─────────────────────────────────────────────────────────
        self.bridge_queue    = BridgeQueue()
        self.synthesis_queue = SynthesisQueue()

        # ── Policy engine ──────────────────────────────────────────────────
        self.policy_engine = PolicyEngine(self.registry, self.active_bridges)

        # ── Phases ─────────────────────────────────────────────────────────
        self.discovery = DiscoveryPhase(
            registry   = self.registry,
            graph      = self.graph,
            drift_log  = self.drift_log,
            event_bus  = self.event_bus,
            ml_matcher = self.ml_matcher,
        )
        self.matching = MatchingPhase(
            registry        = self.registry,
            graph           = self.graph,
            bridge_queue    = self.bridge_queue,
            active_bridges  = self.active_bridges,
            event_bus       = self.event_bus,
            ml_matcher      = self.ml_matcher,
            policy_engine   = self.policy_engine,
        )
        self.policy_phase = PolicyPhase(
            bridge_queue    = self.bridge_queue,
            synthesis_queue = self.synthesis_queue,
            policy_engine   = self.policy_engine,
            event_bus       = self.event_bus,
        )
        self.synthesis = SynthesisPhase(
            synthesis_queue = self.synthesis_queue,
            registry        = self.registry,
            active_bridges  = self.active_bridges,
            event_bus       = self.event_bus,
            ml_matcher      = self.ml_matcher,
            telemetry       = self.telemetry,
            store           = self.store,       # #2
            runners         = self._runners,    # #1
        )
        self.lifecycle = LifecyclePhase(
            registry        = self.registry,
            graph           = self.graph,
            active_bridges  = self.active_bridges,
            bridge_queue    = self.bridge_queue,
            event_bus       = self.event_bus,
            matching_phase  = self.matching,
            store           = self.store,       # #2
            runners         = self._runners,    # #1
        )
        self.drift_watch = DriftWatchPhase(
            registry         = self.registry,
            drift_log        = self.drift_log,
            active_bridges   = self.active_bridges,
            event_bus        = self.event_bus,
            ml_matcher       = self.ml_matcher,
            synthesis_queue  = self.synthesis_queue,
            bridge_queue     = self.bridge_queue,
        )

        # ── Wire pg into phases (set after construction to avoid circular deps) ──
        # These are set in start() once pg is connected.
        self._tasks: list             = []
        self._stop_event              = asyncio.Event()
        self._api_runner: Any         = None
        # Tracks which infrastructure services are unavailable
        # Populated during start(); exposed via /health
        self._degraded_services: list = []

    # ── Boot ───────────────────────────────────────────────────────────────

    async def start(self) -> None:
        print_banner()
        logger.info("Booting Interface Core  node=%s", settings.node_name)

        # ── Config validation ──────────────────────────────────────────────────
        _config_errors = []
        if settings.min_composite_score <= 0 or settings.min_composite_score > 1:
            _config_errors.append(f"IC_MIN_SCORE={settings.min_composite_score} must be 0<x≤1")
        if settings.max_bridges_per_node < 1:
            _config_errors.append(f"IC_MAX_BRIDGES_PER_NODE={settings.max_bridges_per_node} must be ≥1")
        if settings.buffer_size < 10:
            _config_errors.append(f"IC_BUFFER_SIZE={settings.buffer_size} must be ≥10")
        if settings.node_stale_s < 5:
            _config_errors.append(f"IC_NODE_STALE_S={settings.node_stale_s} must be ≥5")
        if settings.discovery_interval_ms < 100:
            _config_errors.append(f"IC_DISCOVERY_INTERVAL_MS={settings.discovery_interval_ms} must be ≥100")
        if _config_errors:
            for err in _config_errors:
                logger.error("CONFIG ERROR: %s", err)
            raise SystemExit(f"Interface Core: {len(_config_errors)} config error(s) — fix above then restart")

        # ── ML Matcher ────────────────────────────────────────────────────────
        print_phase_header("ML MATCHER", "loading embedder model")
        logger.info("Loading semantic matcher  model=%s", settings.embedding_model)
        # ML loading is non-blocking: start model load in background,
        # phases start immediately with hashing-fallback until model is ready.
        # This prevents a 3-5s startup tax when the model is cached,
        # and keeps the daemon functional even if the model never loads.
        import concurrent.futures
        _ml_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1,
                                                               thread_name_prefix='ic-ml')
        _ml_future   = asyncio.get_event_loop().run_in_executor(
            _ml_executor, self.ml_matcher.wait_ready, 30.0
        )

        async def _log_ml_ready():
            try:
                ready = await asyncio.wait_for(_ml_future, timeout=35.0)
                if ready and self.ml_matcher._embedder._model is not None:
                    logger.info(
                        "ML sentence-transformer ready  model=%s  dim=%d",
                        settings.embedding_model,
                        self.ml_matcher._embedder.dim,
                    )
                else:
                    logger.warning(
                        "ML using hashing-trick fallback  "
                        "(sentence-transformers not installed or model load failed)  "
                        "Semantic matching quality reduced.  "
                        "pip install sentence-transformers"
                    )
            except asyncio.TimeoutError:
                logger.warning("ML model load timed out after 35s — using hashing fallback")
        asyncio.create_task(_log_ml_ready(), name="ml_ready_log")

        # ── Postgres ─────────────────────────────────────────────────────────
        print_phase_header("PERSISTENCE", "postgres + redis")
        logger.info("Connecting to Postgres  %s", settings.postgres_dsn.split("@")[-1])
        pg_ok = await self.pg.connect()
        _degraded: list = []
        if pg_ok:
            await self.pg.setup_schema()
            for phase in [self.discovery, self.synthesis, self.lifecycle, self.consul,
                          self.process_scanner]:
                phase._pg = self.pg
            self.policy_engine.audit._pg = self.pg
            self.policy_engine._pg       = self.pg   # for staged-approval audit writes
            logger.info("Postgres ready  tables=ic_bridges,ic_drift_log,ic_audit_log")
        else:
            _degraded.append("postgres")
            logger.warning(
                "DEGRADED: Postgres unavailable — bridge state is NOT durable. "
                "Bridges and drift history will be lost on restart. "
                "Fix: start Postgres at %s",
                settings.postgres_dsn.split('@')[-1],
            )

        # Check Redis
        if self.store.backend_name != 'redis':
            _degraded.append("redis")
            logger.warning(
                "DEGRADED: Redis unavailable — bridge store is in-memory only. "
                "Bridges will not survive restart. Fix: start Redis at %s",
                settings.redis_url,
            )

        self._degraded_services = _degraded

        if _degraded:
            logger.warning(
                "*** RUNNING IN DEGRADED MODE: missing %s — "
                "system appears healthy but persistence is partially disabled ***",
                ', '.join(_degraded).upper(),
            )

        # Wire telemetry into phases (always, regardless of Postgres state)
        self.lifecycle._telemetry = self.telemetry
        self.discovery._telemetry = self.telemetry

        # ── Phase tasks ────────────────────────────────────────────────────────
        print_phase_header("PHASES", "spawning 10 background tasks")
        self._tasks = [
            asyncio.create_task(self.discovery.run(),          name="discovery"),
            asyncio.create_task(self.matching.run(),           name="matching"),
            asyncio.create_task(self.policy_phase.run(),       name="policy"),
            asyncio.create_task(self.synthesis.run(),          name="synthesis"),
            asyncio.create_task(self.lifecycle.run(),          name="lifecycle"),
            asyncio.create_task(self.drift_watch.run(),        name="drift_watch"),
            asyncio.create_task(self._deferred_restore(),      name="restore"),
            asyncio.create_task(self.consul.run(),             name="consul"),
            asyncio.create_task(self.process_scanner.run(),    name="process_scan"),
            asyncio.create_task(self._pg_maintenance(),        name="pg_maintenance"),
        ]
        logger.info(
            "Tasks spawned  discovery=%dms  matching=%dms  lifecycle=%dms  drift=%dms",
            settings.discovery_interval_ms, settings.matching_interval_ms,
            settings.lifecycle_interval_ms, settings.drift_watch_interval_ms,
        )

        # ── HTTP API ──────────────────────────────────────────────────────────
        print_phase_header("HTTP API", f"http://{settings.api_host}:{settings.api_port}")
        _auth_mode = "key-protected (IC_API_KEY set)" if settings.api_key else "open (no IC_API_KEY)"
        logger.info("HTTP API  http://%s:%d  auth=%s",
                    settings.api_host, settings.api_port, _auth_mode)
        await self._start_api()

        # ── Config summary ────────────────────────────────────────────────────
        print_phase_header("CONFIG SUMMARY", "")
        logger.info(
            "Scoring  min_score=%.2f  decay=0.95/min  drift_delta=%.2f",
            settings.min_composite_score, settings.contract_drift_delta,
        )
        logger.info(
            "Resilience  circuit=%.0f%%  buffer=%d  idem_ttl=%ds",
            settings.circuit_error_threshold * 100,
            settings.buffer_size, settings.idempotency_ttl_s,
        )
        logger.info(
            "Policy  max_bridges=%d  stage_above_risk=%.2f",
            settings.max_bridges_per_node, settings.require_approval_above_risk,
        )
        # Security summary
        from .schema.security import SecurityPolicy
        _sec = SecurityPolicy.from_env()
        logger.info(
            "Security  hard_refuse=%s  allowlist=%s  trust_zones=%d zones",
            "enabled" if _sec.hard_refuse._enabled else "disabled",
            "strict" if _sec.allowlist._active else "open",
            len(_sec.trust_zones._zones),
        )
        if _sec.allowlist._active:
            logger.info("  Allowlist sources: %s",
                        ', '.join(_sec.allowlist._patterns[:5]))
        if _sec.trust_zones._zones:
            logger.info("  Trust zones: %s",
                        ', '.join(_sec.trust_zones._zones.keys()))

        logger.info(
            "Discovery  consul=%s:%d  process_scan=enabled  "
            "rest_targets=%d  kafka_topics=%d  grpc_targets=%d",
            settings.consul_host, settings.consul_port,
            len(settings.scan_rest_targets),
            len(settings.scan_kafka_topics),
            len(getattr(settings, "scan_grpc_targets", [])),
        )
        if settings.scan_rest_targets:
            logger.info("  REST: %s", "  ".join(settings.scan_rest_targets[:5]))
        if settings.scan_kafka_topics:
            logger.info("  Kafka: %s", "  ".join(settings.scan_kafka_topics[:5]))

        logger.info(
            "%s Interface Core ready  bridges=%d %s",
            "━" * 10, len(self.active_bridges), "━" * 10,
        )


    async def _restore_bridges(self) -> None:
        """
        Improvement #2: Re-instantiate bridges that were persisted before last shutdown.
        Each restored bridge skips matching/policy and goes straight to synthesis queue.
        """
        restored = await self.store.restore_all()
        if not restored:
            return
        logger.info("Restoring %d bridges from store…", len(restored))
        for bridge, candidate in restored:
            # Only restore if both nodes are still registered
            p = self.registry.get(bridge.producer_id)
            c = self.registry.get(bridge.consumer_id)
            if p and c:
                # Push directly to synthesis queue — already approved
                await self.synthesis_queue.push(candidate)
                logger.info("Queued restored bridge %s for re-synthesis", bridge.id)
            else:
                # Nodes gone; delete stale entry
                await self.store.delete(bridge.producer_id, bridge.consumer_id)
                logger.info(
                    "Dropped stale bridge %s (nodes no longer registered)", bridge.id
                )

    async def _deferred_restore(self) -> None:
        """
        Improvement #2: Wait one discovery cycle for nodes to register,
        then restore previously-approved bridges from the store.
        Any restored bridge whose nodes have not re-appeared is discarded.
        """
        grace = (settings.discovery_interval_ms * 3) / 1000.0
        logger.info("Bridge restore: waiting %.1fs for discovery to populate nodes…", grace)
        await asyncio.sleep(grace)
        await self._restore_bridges()

    async def _pg_maintenance(self) -> None:
        """Purges drift log entries older than 7 days.
        Runs once immediately on boot, then every 6 hours.
        """
        import asyncio as _a
        while True:
            try:
                deleted = await self.pg.purge_old_drift()
                if deleted:
                    logger.info("pg_maintenance: purged %d old drift entries", deleted)
            except Exception as exc:
                logger.debug("pg_maintenance error: %s", exc)
            await _a.sleep(6 * 3600)

    async def _start_api(self) -> None:
        """Improvement #3: start HTTP API if aiohttp is available."""
        try:
            from .api import start_api
            host = settings.api_host if hasattr(settings, "api_host") else "0.0.0.0"
            port = settings.api_port if hasattr(settings, "api_port") else 8000
            self._api_runner = await start_api(self, host=host, port=port)
        except Exception as exc:
            logger.warning("HTTP API failed to start: %s — daemon will run without API", exc)

    # ── Shutdown ────────────────────────────────────────────────────────────

    async def stop(self) -> None:
        logger.info("Shutting down Interface Core…")

        # Stop all phase tasks
        await self.discovery.stop()
        await self.matching.stop()
        await self.policy_phase.stop()
        await self.synthesis.stop()
        await self.lifecycle.stop()
        await self.drift_watch.stop()
        await self.consul.stop()
        await self.process_scanner.stop()

        # Stop all BridgeRunners — signal stop first, then drain ring buffers
        for runner in list(self._runners.values()):
            try:
                runner._running = False   # signal loop to exit after current msg
            except Exception:
                pass
        # Brief drain window: let in-flight messages finish
        if self._runners:
            logger.info("Draining %d bridge runner(s)…", len(self._runners))
            await asyncio.sleep(0.3)
        for runner in list(self._runners.values()):
            try:
                # Flush ring buffer to consumer before closing
                rt = runner._runtime
                buffered = rt.drain_buffer()
                if buffered:
                    logger.info(
                        "Flushing %d buffered messages for bridge %s",
                        len(buffered), runner._id,
                    )
                    for payload in buffered:
                        try:
                            await runner._consumer.write(payload)
                        except Exception:
                            pass
                await runner.stop()
            except Exception:
                pass
        self._runners.clear()

        # Cancel asyncio tasks
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

        # Postgres
        await self.pg.close()

        # Stop API server
        if self._api_runner:
            try:
                await self._api_runner.cleanup()
            except Exception:
                pass

        self._stop_event.set()
        logger.info("Interface Core stopped.")

    async def wait(self) -> None:
        await self._stop_event.wait()

    async def run_forever(self) -> None:
        loop = asyncio.get_running_loop()

        def _handle_signal():
            asyncio.create_task(self.stop())

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _handle_signal)
            except NotImplementedError:
                pass

        await self.start()
        await self.wait()

    # ── Public API ─────────────────────────────────────────────────────────

    def register_node(
        self,
        protocol:         str,
        endpoint:         str,
        fields:           Optional[Dict]  = None,
        tags:             Optional[List]  = None,
        role:             Optional[str]   = None,
        emission_rate_hz: Optional[float] = None,
    ) -> str:
        schema_fields = []
        if fields:
            for name, ftype in fields.items():
                schema_fields.append(FieldSchema(name=name, type=ftype))

        contract = DataContract(
            schema_fields    = schema_fields or [FieldSchema(name="data", type="object")],
            semantic_tags    = tags or ["generic"],
            encoding         = "json",
            emission_rate_hz = emission_rate_hz,
            confidence       = 1.0,
        )
        proto = Protocol(protocol) if protocol in Protocol.__members__.values() else Protocol.UNKNOWN
        r     = NodeRole(role) if role else None
        return self.discovery.register_node_directly(proto, endpoint, contract, r)

    def deregister_node(self, node_id: str) -> bool:
        """
        Atomically remove a node from registry, graph, and drift_log.
        Returns True if the node existed and was removed.

        Using a single method ensures all three structures stay consistent —
        callers cannot accidentally remove from registry but leave a dangling
        graph node or drift history that references a non-existent node.
        """
        node = self.registry.get(node_id)
        if not node:
            return False
        # Remove from all three structures
        self.registry.remove(node_id)
        self.graph.remove_node(node_id)
        self.drift_log.clear_node(node_id)
        logger.info("Node deregistered: %s [%s] %s",
                    node_id, node.protocol.value, node.endpoint)
        return True

    def status(self) -> Dict:
        bridges_detail = {}
        for (pid, cid), entry in self.active_bridges.items():
            bridge  = entry[0]
            run_key = f"{pid}:{cid}"
            runner  = self._runners.get(run_key)
            bridges_detail[bridge.id] = {
                "producer_id": pid,
                "consumer_id": cid,
                "state":       bridge.state.value,
                "runtime":     runner.stats if runner else None,
            }
        return {
            "nodes":             len(self.registry),
            "active_bridges":    len(self.active_bridges),
            "bridge_queue":      len(self.bridge_queue),
            "pending_approvals": self.policy_engine.pending_approvals(),
            "staged_approvals":  [
                {"producer_id": v["producer_id"], "consumer_id": v["consumer_id"],
                 "score": v["score"], "staged_at": v["staged_at"]}
                for v in self.policy_engine._staged_approvals.values()
            ],
            "store_backend":     self.store.backend_name,
            "postgres":          self.pg.connected,
            "degraded_services": self._degraded_services,
            "durable":           len(self._degraded_services) == 0,
            "consul_nodes":      sum(1 for n in self.registry.all_nodes()
                                     if n.metadata.get("consul_service")),
            "process_scan_ports": self.process_scanner.seen_port_count,
            "bridges":           bridges_detail,
            "telemetry":         self.telemetry.snapshot(),
        }

    def approve_bridge(self, producer_id: str, consumer_id: str) -> bool:
        return self.policy_engine.operator_approve(producer_id, consumer_id)

    def reject_bridge(self, producer_id: str, consumer_id: str) -> bool:
        return self.policy_engine.operator_reject(producer_id, consumer_id)

    def audit_log(self, n: int = 50) -> list:
        return self.policy_engine.audit.recent(n)

    def inject(self, producer_id: str, payload: Any) -> bool:
        """
        Push a payload into a producer's transport queue.

        Works for any transport that exposes a .queue attribute (MemoryProducerTransport).
        For transports without a queue (REST, Kafka, MQTT), logs a warning and returns False.
        Primary use: testing, simulation, and programmatic data injection.
        """
        injected = False
        for (pid, cid), entry in list(self.active_bridges.items()):
            if pid != producer_id:
                continue
            run_key = f"{pid}:{cid}"
            runner  = self._runners.get(run_key)
            if runner is None:
                continue
            transport = runner._producer
            if hasattr(transport, "queue"):
                try:
                    transport.queue.put_nowait(payload)
                    injected = True
                except Exception as exc:
                    logger.warning("inject() queue full for bridge %s→%s: %s", pid, cid, exc)
            elif hasattr(transport, "_fallback") and hasattr(transport._fallback, "queue"):
                # Kafka/MQTT fallback transport
                try:
                    transport._fallback.queue.put_nowait(payload)
                    injected = True
                except Exception:
                    pass
            else:
                logger.warning(
                    "inject(): transport %s for %s→%s has no queue; "                    "use a memory-backed node or send via the real protocol",
                    type(transport).__name__, pid, cid
                )
        return injected

    def bridge_stats(self) -> List[Dict]:
        """Return live runtime stats for all active bridge runners."""
        return [r.stats for r in self._runners.values()]


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(
        description="Interface Core — autonomous middleware glue daemon"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable DEBUG logging for all Interface Core loggers"
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG","INFO","WARNING","ERROR"],
        help="Log level (default: INFO)"
    )
    parser.add_argument(
        "--status-interval", type=int, default=0, metavar="SECONDS",
        help="Print status table every N seconds (0 = disabled)"
    )
    args = parser.parse_args()

    setup_logging(level=args.log_level, verbose=args.verbose)

    async def _run():
        core = InterfaceCore()
        if args.status_interval > 0:
            async def _status_printer():
                while True:
                    await asyncio.sleep(args.status_interval)
                    print_status_table(core.status())
            asyncio.create_task(_status_printer(), name="status_printer")
        await core.run_forever()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
