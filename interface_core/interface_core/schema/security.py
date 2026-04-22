"""
Interface Core — Trust Zones and Security Policy

Implements the three hard security requirements from the design docs:

1. Trust zones: nodes are assigned to zones; bridges that cross zone boundaries
   require explicit human approval regardless of score.

2. Hard refuse patterns: endpoint prefixes/patterns that are NEVER bridged
   automatically — admin APIs, SSH, credential stores, auth endpoints, etc.

3. Allowlist enforcement: when IC_ALLOWLIST_SOURCES is set, only explicitly
   listed source node IDs or endpoint patterns can be used as producers.

These checks run BEFORE the normal policy gates in PolicyEngine.evaluate().
A HARD_REFUSE returns PolicyVerdict.REJECT with no appeal.
A CROSS_ZONE returns PolicyVerdict.STAGE always (human must approve).
An ALLOWLIST_VIOLATION returns PolicyVerdict.REJECT with no appeal.

Configuration (all via environment variables):
  IC_TRUST_ZONES            JSON: {"zone_a": ["node_id_1","http://api.local/*"],
                                   "zone_b": ["kafka://broker/*"]}
  IC_CROSS_ZONE_APPROVE     comma-separated "zone_a:zone_b" pairs that auto-approve
  IC_ALLOWLIST_SOURCES      comma-separated node IDs or endpoint prefixes allowed as producers
  IC_HARD_REFUSE_PATTERNS   comma-separated endpoint prefixes/patterns to always reject
  IC_HARD_REFUSE_ENABLED    true/false (default: true)
"""

from __future__ import annotations
import fnmatch
import json
import logging
import os
import re
from typing import Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Built-in hard refuse patterns
# ─────────────────────────────────────────────
# These are endpoint patterns that should NEVER be auto-bridged.
# Admin interfaces, auth services, credential stores, system control endpoints.

_BUILTIN_HARD_REFUSE: List[str] = [
    # Auth / identity
    "*oauth*", "*oidc*", "*saml*", "*auth/token*", "*login*", "*password*",
    "*credentials*", "*secrets*", "*/vault/*", "*/keyvault/*",

    # Admin interfaces
    "*/admin/*", "*/management/*", "*/actuator/*", "*:9090/admin*",
    "*/_cluster/*", "*/_cat/*",   # Elasticsearch cluster admin

    # SSH, RDP, WMI
    "*:22", "*:3389", "*:135", "*:5985", "*:5986",   # WinRM

    # Database admin ports
    "*:5432/postgres", "*:3306/mysql", "*:27017/admin",
    "*:6379/admin*",

    # Kubernetes API / Docker
    "*:6443/*", "*:2375/*", "*:2376/*",   # k8s apiserver, docker daemon

    # System / OS
    "/proc/*", "/sys/*", "/dev/*",

    # Known sensitive path prefixes
    "*/internal/*", "*/private/*", "*/system/*",
]


# ─────────────────────────────────────────────
# TrustZone
# ─────────────────────────────────────────────

class TrustZone:
    """
    Assigns nodes to named trust zones and enforces cross-zone approval.

    A node belongs to a zone if its ID or endpoint matches any of the
    zone's patterns (glob or exact). Nodes that match no zone are assigned
    to the "default" zone.

    Cross-zone bridges are STAGED (require human approval) unless both
    zones appear in the auto-approve list.
    """

    def __init__(
        self,
        zone_patterns:     Optional[Dict[str, List[str]]] = None,
        auto_approve_pairs: Optional[List[Tuple[str, str]]] = None,
    ) -> None:
        # zone_name → list of glob patterns
        self._zones: Dict[str, List[str]] = zone_patterns or {}
        # set of (zone_a, zone_b) pairs that auto-approve (order-insensitive)
        self._auto: Set[Tuple[str, str]] = set()
        for pair in (auto_approve_pairs or []):
            a, b = pair
            self._auto.add((a, b))
            self._auto.add((b, a))

    @classmethod
    def from_env(cls) -> "TrustZone":
        """Build a TrustZone from IC_TRUST_ZONES and IC_CROSS_ZONE_APPROVE env vars."""
        zone_patterns = {}
        raw_zones = os.environ.get("IC_TRUST_ZONES", "")
        if raw_zones:
            try:
                zone_patterns = json.loads(raw_zones)
            except json.JSONDecodeError:
                logger.warning("IC_TRUST_ZONES is not valid JSON — trust zones disabled")

        auto_pairs = []
        raw_auto = os.environ.get("IC_CROSS_ZONE_APPROVE", "")
        if raw_auto:
            for pair_str in raw_auto.split(","):
                pair_str = pair_str.strip()
                if ":" in pair_str:
                    a, b = pair_str.split(":", 1)
                    auto_pairs.append((a.strip(), b.strip()))

        return cls(zone_patterns, auto_pairs)

    def zone_of(self, node_id: str, endpoint: str) -> str:
        """Return the zone name for a node. 'default' if no match."""
        for zone_name, patterns in self._zones.items():
            for pat in patterns:
                if fnmatch.fnmatch(node_id, pat) or fnmatch.fnmatch(endpoint, pat):
                    return zone_name
        return "default"

    def requires_approval(self, producer_zone: str, consumer_zone: str) -> bool:
        """True if this zone pair requires human approval."""
        if producer_zone == consumer_zone:
            return False
        # Different zones always need approval unless explicitly auto-approved
        return (producer_zone, consumer_zone) not in self._auto

    def check(
        self, producer_id: str, producer_endpoint: str,
        consumer_id: str, consumer_endpoint: str,
    ) -> Tuple[bool, str, str, str]:
        """
        Returns (needs_approval, reason, producer_zone, consumer_zone).
        needs_approval=False means auto-approve (same zone or in auto list).
        """
        p_zone = self.zone_of(producer_id, producer_endpoint)
        c_zone = self.zone_of(consumer_id, consumer_endpoint)
        needs  = self.requires_approval(p_zone, c_zone)
        reason = (f"cross-zone-bridge:{p_zone}→{c_zone}" if needs
                  else f"same-zone-or-approved:{p_zone}↔{c_zone}")
        return needs, reason, p_zone, c_zone


# ─────────────────────────────────────────────
# HardRefuseChecker
# ─────────────────────────────────────────────

class HardRefuseChecker:
    """
    Checks endpoints against a list of patterns that must NEVER be bridged.
    Returns a refusal reason if the endpoint matches, or None if safe.
    """

    def __init__(
        self,
        extra_patterns: Optional[List[str]] = None,
        enabled:        bool = True,
    ) -> None:
        self._enabled  = enabled
        self._patterns = list(_BUILTIN_HARD_REFUSE)
        if extra_patterns:
            self._patterns.extend(extra_patterns)
        logger.debug(
            "HardRefuseChecker: %d patterns  enabled=%s",
            len(self._patterns), enabled,
        )

    @classmethod
    def from_env(cls) -> "HardRefuseChecker":
        enabled = os.environ.get("IC_HARD_REFUSE_ENABLED", "true").lower() != "false"
        extra   = []
        raw     = os.environ.get("IC_HARD_REFUSE_PATTERNS", "")
        if raw:
            extra = [p.strip() for p in raw.split(",") if p.strip()]
        return cls(extra_patterns=extra, enabled=enabled)

    def check(self, endpoint: str, node_id: str = "") -> Optional[str]:
        """
        Return a refusal reason string if the endpoint matches a hard-refuse pattern.
        Return None if the endpoint is safe.
        """
        if not self._enabled:
            return None
        ep = endpoint.lower()
        for pat in self._patterns:
            if fnmatch.fnmatch(ep, pat.lower()):
                return f"hard-refuse-pattern:{pat}"
        return None

    def check_pair(
        self, producer_endpoint: str, consumer_endpoint: str,
        producer_id: str = "", consumer_id: str = "",
    ) -> Optional[str]:
        """Check both endpoints. Return reason for first match, or None."""
        r = self.check(producer_endpoint, producer_id)
        if r:
            return f"producer:{r}"
        r = self.check(consumer_endpoint, consumer_id)
        if r:
            return f"consumer:{r}"
        return None


# ─────────────────────────────────────────────
# AllowlistChecker
# ─────────────────────────────────────────────

class AllowlistChecker:
    """
    When an allowlist is configured, ONLY listed sources can act as producers.
    An empty allowlist means "allow everything" (open mode).

    The allowlist contains node IDs or endpoint glob patterns.
    A node must match at least one entry to be allowed as a producer.
    """

    def __init__(self, patterns: Optional[List[str]] = None) -> None:
        self._patterns = [p.strip() for p in (patterns or []) if p.strip()]
        self._active   = bool(self._patterns)
        if self._active:
            logger.info(
                "AllowlistChecker: %d source patterns (strict mode)",
                len(self._patterns),
            )
        else:
            logger.debug("AllowlistChecker: empty — all sources allowed")

    @classmethod
    def from_env(cls) -> "AllowlistChecker":
        raw = os.environ.get("IC_ALLOWLIST_SOURCES", "")
        patterns = [p.strip() for p in raw.split(",") if p.strip()] if raw else []
        return cls(patterns)

    def is_allowed(self, node_id: str, endpoint: str) -> bool:
        """Return True if the node is allowed as a producer (or allowlist is empty)."""
        if not self._active:
            return True
        for pat in self._patterns:
            if fnmatch.fnmatch(node_id, pat) or fnmatch.fnmatch(endpoint, pat):
                return True
        return False

    def check(self, node_id: str, endpoint: str) -> Optional[str]:
        """Return rejection reason if not allowed, None if allowed."""
        if self.is_allowed(node_id, endpoint):
            return None
        return f"not-in-source-allowlist:{node_id[:16]}"


# ─────────────────────────────────────────────
# SecurityPolicy (composite)
# ─────────────────────────────────────────────

class SecurityPolicy:
    """
    Composite security gate.
    Runs all three checks: hard refuse → allowlist → trust zones.

    Used by PolicyEngine before its own checks.
    Returns (verdict, reason):
      ("hard_reject", reason) — immutable, no appeal
      ("stage",       reason) — cross-zone: human must approve
      ("allow",       reason) — passed all security checks
    """

    def __init__(
        self,
        hard_refuse: Optional[HardRefuseChecker] = None,
        allowlist:   Optional[AllowlistChecker]   = None,
        trust_zones: Optional[TrustZone]           = None,
    ) -> None:
        self.hard_refuse = hard_refuse or HardRefuseChecker(enabled=False)
        self.allowlist   = allowlist   or AllowlistChecker()
        self.trust_zones = trust_zones or TrustZone()

    @classmethod
    def from_env(cls) -> "SecurityPolicy":
        return cls(
            hard_refuse = HardRefuseChecker.from_env(),
            allowlist   = AllowlistChecker.from_env(),
            trust_zones = TrustZone.from_env(),
        )

    def evaluate(
        self,
        producer_id:       str,
        producer_endpoint: str,
        consumer_id:       str,
        consumer_endpoint: str,
    ) -> Tuple[str, str]:
        # 1. Hard refuse — immutable
        refusal = self.hard_refuse.check_pair(
            producer_endpoint, consumer_endpoint,
            producer_id, consumer_id,
        )
        if refusal:
            logger.warning(
                "SECURITY HARD_REFUSE  %s→%s  reason=%s",
                producer_id[:12], consumer_id[:12], refusal,
            )
            return "hard_reject", refusal

        # 2. Allowlist — immutable
        al_reason = self.allowlist.check(producer_id, producer_endpoint)
        if al_reason:
            logger.warning(
                "SECURITY ALLOWLIST_REJECT  %s  reason=%s",
                producer_id[:12], al_reason,
            )
            return "hard_reject", al_reason

        # 3. Trust zones — may require staging
        needs_approval, tz_reason, p_zone, c_zone = self.trust_zones.check(
            producer_id, producer_endpoint, consumer_id, consumer_endpoint
        )
        if needs_approval:
            logger.info(
                "SECURITY CROSS_ZONE  %s[%s]→%s[%s]  requires human approval",
                producer_id[:12], p_zone, consumer_id[:12], c_zone,
            )
            return "stage", tz_reason

        return "allow", tz_reason
