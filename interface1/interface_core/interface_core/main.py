"""
Interface Core — Main Daemon
Boots all phases in parallel as asyncio tasks.
"""

from __future__ import annotations
import asyncio
import logging
import signal
import sys
from typing import Dict, Optional, Tuple

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
from .telemetry import Telemetry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("interface_core")


# ─────────────────────────────────────────────
# InterfaceCore daemon
# ─────────────────────────────────────────────

class InterfaceCore:
    """
    Top-level daemon.  Wires up all subsystems and runs them concurrently.

    Usage:
        core = InterfaceCore()
        await core.start()          # non-blocking; returns when all phases are up
        await core.wait()           # blocks until shutdown
        await core.stop()           # graceful shutdown

    Or as a one-liner:
        asyncio.run(InterfaceCore().run_forever())
    """

    def __init__(self) -> None:
        # ── Shared state ───────────────────────────────────────────────────
        self.registry        = NodeRegistry()
        self.graph           = CapabilityGraph()
        self.drift_log       = DriftLog()
        self.active_bridges: Dict[Tuple, Tuple] = {}   # key→(Bridge, Adapter)

        # ── Infra ──────────────────────────────────────────────────────────
        self.event_bus    = make_event_bus(settings.zmq_pub_addr, settings.zmq_sub_addr)
        self.ml_matcher   = SemanticMatcher(settings.embedding_model)
        self.telemetry    = Telemetry(settings.otel_endpoint, settings.prometheus_port)

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
        )
        self.lifecycle = LifecyclePhase(
            registry        = self.registry,
            graph           = self.graph,
            active_bridges  = self.active_bridges,
            bridge_queue    = self.bridge_queue,
            event_bus       = self.event_bus,
            matching_phase  = self.matching,
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

        self._tasks: list = []
        self._stop_event  = asyncio.Event()

    # ── Lifecycle ──────────────────────────────────────────────────────────

    async def start(self) -> None:
        logger.info("Interface Core booting up (node=%s)", settings.node_name)

        # Wait for ML model to load (up to 30s)
        ready = await asyncio.get_event_loop().run_in_executor(
            None, self.ml_matcher.wait_ready, 30.0
        )
        if ready:
            logger.info("ML matcher ready")
        else:
            logger.warning("ML matcher not ready — using fallback embedder")

        # Spawn all phases as background tasks
        self._tasks = [
            asyncio.create_task(self.discovery.run(),    name="discovery"),
            asyncio.create_task(self.matching.run(),     name="matching"),
            asyncio.create_task(self.policy_phase.run(), name="policy"),
            asyncio.create_task(self.synthesis.run(),    name="synthesis"),
            asyncio.create_task(self.lifecycle.run(),    name="lifecycle"),
            asyncio.create_task(self.drift_watch.run(),  name="drift_watch"),
        ]
        logger.info("All phases started ✓")

    async def stop(self) -> None:
        logger.info("Shutting down Interface Core...")
        await self.discovery.stop()
        await self.matching.stop()
        await self.policy_phase.stop()
        await self.synthesis.stop()
        await self.lifecycle.stop()
        await self.drift_watch.stop()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._stop_event.set()
        logger.info("Interface Core stopped.")

    async def wait(self) -> None:
        await self._stop_event.wait()

    async def run_forever(self) -> None:
        """Convenience: start, install signal handlers, block until SIGINT/SIGTERM."""
        loop = asyncio.get_running_loop()

        def _handle_signal():
            asyncio.create_task(self.stop())

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _handle_signal)
            except NotImplementedError:
                pass   # Windows

        await self.start()
        await self.wait()

    # ── Public API ─────────────────────────────────────────────────────────

    def register_node(
        self,
        protocol: str,
        endpoint: str,
        fields: Optional[Dict] = None,
        tags: Optional[list]   = None,
        role: Optional[str]    = None,
        emission_rate_hz: Optional[float] = None,
    ) -> str:
        """
        Register a node programmatically (e.g. from tests or embedded usage).
        Returns the node_id.
        """
        schema_fields = []
        if fields:
            for name, ftype in fields.items():
                schema_fields.append(FieldSchema(name=name, type=ftype))

        contract = DataContract(
            schema_fields  = schema_fields or [FieldSchema(name="data", type="object")],
            semantic_tags  = tags or ["generic"],
            encoding       = "json",
            emission_rate_hz = emission_rate_hz,
            confidence     = 1.0,
        )
        p = Protocol(protocol) if protocol in Protocol.__members__.values() else Protocol.UNKNOWN
        r = NodeRole(role) if role else None

        return self.discovery.register_node_directly(p, endpoint, contract, r)

    def status(self) -> Dict:
        """Return a snapshot of current system state."""
        return {
            "nodes":          len(self.registry),
            "active_bridges": len(self.active_bridges),
            "bridge_queue":   len(self.bridge_queue),
            "pending_approvals": self.policy_engine.pending_approvals(),
            "telemetry":      self.telemetry.snapshot(),
        }

    def approve_bridge(self, producer_id: str, consumer_id: str) -> bool:
        return self.policy_engine.operator_approve(producer_id, consumer_id)

    def reject_bridge(self, producer_id: str, consumer_id: str) -> bool:
        return self.policy_engine.operator_reject(producer_id, consumer_id)

    def audit_log(self, n: int = 50) -> list:
        return self.policy_engine.audit.recent(n)


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

def main() -> None:
    asyncio.run(InterfaceCore().run_forever())


if __name__ == "__main__":
    main()
