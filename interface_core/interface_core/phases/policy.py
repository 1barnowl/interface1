"""
Interface Core — Policy Engine
Evaluates bridge candidates against auth, rate limits, blacklist, geo-fence,
schema safety, and circular dependency rules.
"""

from __future__ import annotations
import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from ..config import settings
from ..models import BridgeCandidate, Event, EventType, Node, PolicyVerdict
from ..registry import NodeRegistry
from ..schema.security import SecurityPolicy

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Policy Rule definitions
# ─────────────────────────────────────────────

class PolicyResult:
    def __init__(self, verdict: PolicyVerdict, reason: str = "", risk: float = 0.0):
        self.verdict = verdict
        self.reason  = reason
        self.risk    = risk   # 0-1

    def __repr__(self) -> str:
        return f"PolicyResult({self.verdict.value}, reason={self.reason!r}, risk={self.risk:.2f})"


class AuditLog:
    """In-memory audit log (APPEND-ONLY). Flushed to PostgreSQL by persistence layer."""

    def __init__(self, max_size: int = 65536) -> None:
        self._entries: List[Dict] = []
        self._max    = max_size
        self._pg     = None    # PostgresPersistence | None

    def write(self, verdict: str, candidate: BridgeCandidate, reason: str) -> None:
        entry = {
            "timestamp":   time.time(),
            "verdict":     verdict,
            "producer_id": candidate.producer_id,
            "consumer_id": candidate.consumer_id,
            "score":       candidate.score.composite,
            "reason":      reason,
        }
        self._entries.append(entry)
        if len(self._entries) > self._max:
            self._entries = self._entries[-self._max // 2:]   # trim to half
        logger.info("AUDIT %s %s→%s reason=%s",
                    verdict, candidate.producer_id, candidate.consumer_id, reason)
        if self._pg:
            import asyncio as _a
            try:
                _a.ensure_future(self._pg.write_audit(
                    verdict, candidate.producer_id, candidate.consumer_id,
                    candidate.score.composite, reason,
                ))
            except RuntimeError:
                pass  # no running event loop in sync tests

    def recent(self, n: int = 100) -> List[Dict]:
        return list(reversed(self._entries[-n:]))


# ─────────────────────────────────────────────
# Policy Engine
# ─────────────────────────────────────────────

class PolicyEngine:
    """
    Evaluates a BridgeCandidate and returns APPROVE / STAGE / REJECT.

    Rules checked in order:
      1. Blacklist   → REJECT immediately
      2. Whitelist   → APPROVE immediately (if both nodes are whitelisted)
      3. Max bridges → REJECT if either node already at cap
      4. Circular    → REJECT (A→B and B→A both active = circular)
      5. Schema safe → REJECT if producer.confidence < 0.3
      6. Rate limit  → REJECT if rate differential too extreme (>1000×)
      7. Risk score  → if > stage_threshold → STAGE (needs operator)
                       else → APPROVE
    """

    def __init__(
        self,
        registry:       NodeRegistry,
        active_bridges: Dict,
    ) -> None:
        self._registry       = registry
        self._active_bridges = active_bridges
        self.audit           = AuditLog()
        # Approval queue: in-memory futures (fast path for live approvals)
        self._pending_approvals: Dict[Tuple[str, str], asyncio.Future] = {}
        # Durable staged state: (pid, cid) → {producer, consumer, score, staged_at}
        # Survives across evaluate() calls; written to Postgres if available.
        self._staged_approvals: Dict[Tuple[str, str], dict] = {}
        self._pg = None   # PostgresPersistence | None — set by main
        # Rate limiting: node_id → list of attempt timestamps (sliding window)
        self._rate_attempts: Dict[str, List[float]] = {}
        # Security gate: hard refuse, allowlist, trust zones
        self.security = SecurityPolicy.from_env()

    # ── Quick risk estimate (used by matching, no side-effects) ─────────────

    def quick_risk(self, producer: Node, consumer: Node) -> float:
        """Returns a 0-1 risk score. Used by MatchingPhase before full eval."""
        risk = 0.0
        if producer.id in settings.blacklisted_nodes or consumer.id in settings.blacklisted_nodes:
            risk += 1.0
        # Unknown protocol raises risk
        if producer.protocol.value == "unknown" or consumer.protocol.value == "unknown":
            risk += 0.4
        # Low confidence contract
        if producer.contract.confidence < 0.4 or consumer.contract.confidence < 0.4:
            risk += 0.3
        return min(1.0, risk)

    # ── Full policy evaluation ──────────────────────────────────────────────

    async def evaluate(self, candidate: BridgeCandidate) -> PolicyResult:
        p_node = self._registry.get(candidate.producer_id)
        c_node = self._registry.get(candidate.consumer_id)

        if not p_node or not c_node:
            self.audit.write("REJECT", candidate, "node-not-found")
            return PolicyResult(PolicyVerdict.REJECT, "node-not-found")

        # ── Security gate (hard refuse / allowlist / trust zones) ──────────
        sec_verdict, sec_reason = self.security.evaluate(
            p_node.id, p_node.endpoint,
            c_node.id, c_node.endpoint,
        )
        if sec_verdict == "hard_reject":
            self.audit.write("REJECT", candidate, sec_reason)
            return PolicyResult(PolicyVerdict.REJECT, sec_reason, risk=1.0)
        if sec_verdict == "stage":
            self.audit.write("STAGE", candidate, sec_reason)
            return PolicyResult(PolicyVerdict.STAGE, sec_reason, risk=0.8)


        # 1. Blacklist
        if p_node.id in settings.blacklisted_nodes:
            self.audit.write("REJECT", candidate, f"blacklisted-producer:{p_node.id}")
            logger.warning("POLICY FAIL  %s→%s  blacklisted-producer",
                           candidate.producer_id[:8], candidate.consumer_id[:8])
            return PolicyResult(PolicyVerdict.REJECT, f"blacklisted-producer:{p_node.id}", risk=1.0)
        if c_node.id in settings.blacklisted_nodes:
            self.audit.write("REJECT", candidate, f"blacklisted-consumer:{c_node.id}")
            return PolicyResult(PolicyVerdict.REJECT, f"blacklisted-consumer:{c_node.id}", risk=1.0)

        # 2. Whitelist fast-path
        if settings.whitelisted_nodes:
            if p_node.id in settings.whitelisted_nodes and c_node.id in settings.whitelisted_nodes:
                self.audit.write("APPROVE", candidate, "both-whitelisted")
                return PolicyResult(PolicyVerdict.APPROVE, "both-whitelisted", risk=0.0)

        # 3. Bridge cap
        p_bridge_count = sum(
            1 for (pid, cid) in self._active_bridges
            if pid == p_node.id or cid == p_node.id
        )
        c_bridge_count = sum(
            1 for (pid, cid) in self._active_bridges
            if pid == c_node.id or cid == c_node.id
        )
        if p_bridge_count >= settings.max_bridges_per_node:
            self.audit.write("REJECT", candidate, f"producer-bridge-cap:{p_bridge_count}")
            return PolicyResult(PolicyVerdict.REJECT, "producer-bridge-cap", risk=0.5)
        if c_bridge_count >= settings.max_bridges_per_node:
            self.audit.write("REJECT", candidate, f"consumer-bridge-cap:{c_bridge_count}")
            return PolicyResult(PolicyVerdict.REJECT, "consumer-bridge-cap", risk=0.5)

        # 4. Rate limit: reject if a node has been part of too many bridge
        #    attempts in the last 60 seconds (prevents runaway bridging loops)
        _now = time.time()
        _window = 60.0
        _max_attempts = 10  # max bridge attempts per node per 60s
        for _nid in (p_node.id, c_node.id):
            attempts = self._rate_attempts.setdefault(_nid, [])
            # Purge old timestamps outside window
            self._rate_attempts[_nid] = [t for t in attempts if _now - t < _window]
            if len(self._rate_attempts[_nid]) >= _max_attempts:
                self.audit.write("REJECT", candidate, f"rate-limit:{_nid}")
                return PolicyResult(PolicyVerdict.REJECT,
                    f"rate-limit-exceeded:{_nid[:8]}", risk=0.3)
        # Record this attempt for both nodes
        for _nid in (p_node.id, c_node.id):
            self._rate_attempts.setdefault(_nid, []).append(_now)

        # 5. Circular dependency: B→A exists while we're creating A→B
        _now = time.time()
        reverse_key = (candidate.consumer_id, candidate.producer_id)
        if reverse_key in self._active_bridges:
            self.audit.write("REJECT", candidate, "circular-dependency")
            return PolicyResult(PolicyVerdict.REJECT, "circular-dependency", risk=0.8)

        # 6. Schema safety
        if p_node.contract.confidence < 0.3:
            self.audit.write("REJECT", candidate, "low-confidence-producer-contract")
            return PolicyResult(PolicyVerdict.REJECT, "low-confidence-contract", risk=0.6)

        # 7. Rate mismatch guard (>1000× difference)
        p_rate = p_node.contract.emission_rate_hz or 1.0
        c_rate = c_node.contract.emission_rate_hz or p_rate
        if p_rate > 0 and c_rate > 0:
            ratio = max(p_rate, c_rate) / max(min(p_rate, c_rate), 1e-6)
            if ratio > 1000:
                self.audit.write("STAGE", candidate, f"extreme-rate-mismatch:{ratio:.0f}x")
                return PolicyResult(PolicyVerdict.STAGE, "extreme-rate-mismatch", risk=0.7)

        # 8. Risk threshold
        composite_risk = self.quick_risk(p_node, c_node)
        if composite_risk >= settings.require_approval_above_risk:
            self.audit.write("STAGE", candidate, f"risk-threshold:{composite_risk:.2f}")
            return PolicyResult(PolicyVerdict.STAGE, f"risk:{composite_risk:.2f}", risk=composite_risk)

        self.audit.write("APPROVE", candidate, "all-checks-passed")
        logger.info(
            "POLICY PASS  %s→%s  score=%.3f risk=%.2f  [security:allow blacklist:pass cap:pass circular:pass confidence:pass rate:pass]",
            candidate.producer_id[:8], candidate.consumer_id[:8],
            candidate.score.composite, composite_risk,
        )
        return PolicyResult(PolicyVerdict.APPROVE, "all-checks-passed", risk=composite_risk)

    async def request_operator_approval(
        self, candidate: BridgeCandidate, timeout: float = 60.0
    ) -> bool:
        """
        Parks the candidate waiting for operator approval.
        Records the staged state durably in _staged_approvals and Postgres.
        Returns True if approved within timeout.
        """
        import time as _t
        key   = (candidate.producer_id, candidate.consumer_id)
        loop  = asyncio.get_event_loop()
        fut   = loop.create_future()
        self._pending_approvals[key] = fut

        # Durable record — survives if the future is lost (e.g. phase restart)
        self._staged_approvals[key] = {
            "producer_id": candidate.producer_id,
            "consumer_id": candidate.consumer_id,
            "score":       candidate.score.composite,
            "staged_at":   _t.time(),
        }
        if self._pg:
            try:
                asyncio.ensure_future(self._pg.write_audit(
                    "STAGED",
                    candidate.producer_id, candidate.consumer_id,
                    candidate.score.composite,
                    f"timeout={timeout}s",
                ))
            except RuntimeError:
                pass

        logger.warning(
            "AWAITING OPERATOR APPROVAL: bridge %s→%s (timeout=%ds)",
            candidate.producer_id, candidate.consumer_id, int(timeout)
        )
        try:
            return await asyncio.wait_for(asyncio.shield(fut), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Operator approval timed out for %s→%s", *key)
            return False
        finally:
            self._pending_approvals.pop(key, None)
            self._staged_approvals.pop(key, None)   # clear on resolution

    def operator_approve(self, producer_id: str, consumer_id: str) -> bool:
        """Called by operator/API to approve a staged bridge."""
        key = (producer_id, consumer_id)
        fut = self._pending_approvals.get(key)
        if fut and not fut.done():
            fut.set_result(True)
            return True
        return False

    def operator_reject(self, producer_id: str, consumer_id: str) -> bool:
        """Called by operator/API to reject a staged bridge."""
        key = (producer_id, consumer_id)
        fut = self._pending_approvals.get(key)
        if fut and not fut.done():
            fut.set_result(False)
            return True
        return False

    def pending_approvals(self) -> List[Dict]:
        return [{"producer_id": k[0], "consumer_id": k[1]} for k in self._pending_approvals]
