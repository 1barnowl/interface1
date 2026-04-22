"""
Interface Core — Logging Setup
Rich, structured terminal output with phase-by-phase boot progress.
"""

from __future__ import annotations
import logging
import sys
import time
from typing import Optional

# ── Colour codes (disabled if not a TTY) ─────────────────────────────────────
_IS_TTY = sys.stdout.isatty()

class _C:
    RESET  = "\033[0m"   if _IS_TTY else ""
    BOLD   = "\033[1m"   if _IS_TTY else ""
    DIM    = "\033[2m"   if _IS_TTY else ""
    GREEN  = "\033[32m"  if _IS_TTY else ""
    YELLOW = "\033[33m"  if _IS_TTY else ""
    CYAN   = "\033[36m"  if _IS_TTY else ""
    RED    = "\033[31m"  if _IS_TTY else ""
    BLUE   = "\033[34m"  if _IS_TTY else ""
    MAG    = "\033[35m"  if _IS_TTY else ""
    WHITE  = "\033[97m"  if _IS_TTY else ""


# ── Log-level → colour mapping ────────────────────────────────────────────────
_LEVEL_COLOURS = {
    "DEBUG":    _C.DIM    + "DBG",
    "INFO":     _C.GREEN  + "INF",
    "WARNING":  _C.YELLOW + "WRN",
    "ERROR":    _C.RED    + "ERR",
    "CRITICAL": _C.RED    + _C.BOLD + "CRT",
}

# ── Logger-name → display label + colour ─────────────────────────────────────
_NAME_MAP = {
    "interface_core":                      (_C.BOLD + _C.WHITE,  "CORE      "),
    "interface_core.phases.discovery":     (_C.CYAN,             "DISCOVERY "),
    "interface_core.phases.matching":      (_C.BLUE,             "MATCHING  "),
    "interface_core.phases.policy":        (_C.MAG,              "POLICY    "),
    "interface_core.phases.synthesis":     (_C.GREEN,            "SYNTHESIS "),
    "interface_core.phases.lifecycle":     (_C.YELLOW,           "LIFECYCLE "),
    "interface_core.phases.process_scan":  (_C.CYAN,             "PROC_SCAN "),
    "interface_core.phases.runner":        (_C.DIM + _C.CYAN,    "RUNNER    "),
    "interface_core.consul_discovery":     (_C.BLUE,             "CONSUL    "),
    "interface_core.phases.process_scanner": (_C.CYAN,           "PROC_SCAN "),
    "interface_core.schema.security":         (_C.RED + _C.BOLD, "SECURITY  "),
    "interface_core.schema.converters":       (_C.BLUE,          "SCHEMA    "),
    "interface_core.schema.topology":         (_C.BLUE,          "TOPOLOGY  "),
    "interface_core.store":                (_C.DIM,              "STORE     "),
    "interface_core.persistence":          (_C.DIM,              "POSTGRES  "),
    "interface_core.event_bus":            (_C.DIM,              "EVENT_BUS "),
    "interface_core.telemetry":            (_C.DIM,              "TELEMETRY "),
    "interface_core.ml.matcher":           (_C.MAG,              "ML        "),
}

_BOOT_START = time.time()


class ICFormatter(logging.Formatter):
    """
    Custom formatter producing structured, colour-coded terminal lines:

      [+00.123s] INF  DISCOVERY  DiscoveryPhase started (interval=500ms)
      [+00.456s] WRN  ML         sentence-transformers not installed; using fallback
    """

    def format(self, record: logging.LogRecord) -> str:
        elapsed   = time.time() - _BOOT_START
        ts        = f"[+{elapsed:07.3f}s]"

        lvl_str   = _LEVEL_COLOURS.get(record.levelname, record.levelname[:3])

        # Find the best matching logger name label
        name      = record.name
        colour    = _C.DIM
        label     = f"{name.split('.')[-1][:10]:<10}"
        for prefix in sorted(_NAME_MAP.keys(), key=len, reverse=True):
            if name.startswith(prefix):
                colour, label = _NAME_MAP[prefix]
                break

        msg       = record.getMessage()

        # Highlight key events with extra visual weight
        if any(kw in msg for kw in ("SECURITY HARD_REFUSE", "SECURITY ALLOWLIST")):
            msg = _C.BOLD + _C.RED + "⚠ " + msg + _C.RESET
        elif any(kw in msg for kw in ("SECURITY CROSS_ZONE",)):
            msg = _C.BOLD + _C.YELLOW + "⚠ " + msg + _C.RESET
        elif any(kw in msg for kw in ("POLICY PASS",)):
            msg = _C.GREEN + msg + _C.RESET
        elif any(kw in msg for kw in ("POLICY FAIL", "POLICY REJECT")):
            msg = _C.YELLOW + msg + _C.RESET
        elif any(kw in msg for kw in ("PROBE", "SYNTHESIS START", "SYNTHESIS RETRY")):
            msg = _C.DIM + msg + _C.RESET
        elif any(kw in msg for kw in ("FanOut", "FanIn", "fan_out", "fan_in")):
            msg = _C.CYAN + msg + _C.RESET
        if any(kw in msg for kw in ("BRIDGE_LIVE", "BRIDGE_TORN", "ADAPTER_RELOAD")):
            msg = _C.BOLD + _C.GREEN + msg + _C.RESET
        elif any(kw in msg for kw in ("SYNTHESIS_FAILED", "ERROR", "error")):
            msg = _C.RED + msg + _C.RESET
        elif any(kw in msg for kw in ("NEW_NODE", "CONSUL_NODE", "PROCESS_SCAN")):
            msg = _C.CYAN + msg + _C.RESET
        elif any(kw in msg for kw in ("CONTRACT_DRIFT", "ADAPTER_RELOAD")):
            msg = _C.YELLOW + msg + _C.RESET
        elif any(kw in msg for kw in ("All phases started", "Interface Core booting",
                                       "Interface Core stopped")):
            msg = _C.BOLD + _C.WHITE + msg + _C.RESET
        elif any(kw in msg for kw in ("Circuit", "CIRCUIT", "QUEUE_DECAY")):
            msg = _C.YELLOW + msg + _C.RESET

        line = (
            f"{_C.DIM}{ts}{_C.RESET} "
            f"{lvl_str}{_C.RESET}  "
            f"{colour}{label}{_C.RESET}  "
            f"{msg}"
        )

        # Append exception info if present
        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)

        return line


def setup_logging(level: str = "INFO", verbose: bool = False) -> None:
    """
    Configure root logging for Interface Core.

    Args:
        level:   Log level string ("DEBUG", "INFO", "WARNING", etc.)
        verbose: If True, show DEBUG messages from all IC loggers.
    """
    global _BOOT_START
    _BOOT_START = time.time()

    root = logging.getLogger()
    root.setLevel(logging.DEBUG if verbose else getattr(logging, level.upper(), logging.INFO))

    # Remove any existing handlers
    for h in root.handlers[:]:
        root.removeHandler(h)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(ICFormatter())
    handler.setLevel(logging.DEBUG)
    root.addHandler(handler)

    # Silence noisy third-party loggers unless verbose
    if not verbose:
        for noisy in ("asyncio", "urllib3", "aiohttp", "aiokafka", "grpc"):
            logging.getLogger(noisy).setLevel(logging.WARNING)


def print_banner() -> None:
    """Print the startup banner."""
    banner = f"""
{_C.BOLD}{_C.CYAN}
  ╔══════════════════════════════════════════════════════╗
  ║          I N T E R F A C E   C O R E                ║
  ║    autonomous middleware glue daemon  v0.1.0         ║
  ╚══════════════════════════════════════════════════════╝
{_C.RESET}"""
    print(banner, flush=True)


def print_phase_header(name: str, detail: str = "") -> None:
    """Print a section header as a phase starts."""
    pad   = "─" * max(0, 50 - len(name) - len(detail))
    print(
        f"\n{_C.DIM}  ── {_C.RESET}{_C.BOLD}{name}{_C.RESET}"
        f"{_C.DIM}  {detail}  {pad}{_C.RESET}",
        flush=True,
    )


def print_status_table(status: dict) -> None:
    """Print a compact status table to stdout."""
    print(f"\n{_C.BOLD}{_C.WHITE}  ── STATUS ──────────────────────────────────{_C.RESET}")
    rows = [
        ("Nodes registered",    str(status.get("nodes", 0))),
        ("Active bridges",      str(status.get("active_bridges", 0))),
        ("Bridge queue depth",  str(status.get("bridge_queue", 0))),
        ("Consul nodes",        str(status.get("consul_nodes", 0))),
        ("Process scan ports",  str(status.get("process_scan_ports", 0))),
        ("Store backend",       status.get("store_backend", "?")),
        ("Postgres",            "connected" if status.get("postgres") else "unavailable"),
        ("Pending approvals",   str(len(status.get("pending_approvals", [])))),
    ]
    for k, v in rows:
        colour = _C.GREEN if v not in ("0", "unavailable", "?") else _C.DIM
        print(f"  {_C.DIM}{k:<24}{_C.RESET}  {colour}{v}{_C.RESET}")

    bridges = status.get("bridges", {})
    if bridges:
        print(f"\n{_C.BOLD}  Bridges:{_C.RESET}")
        for bid, b in bridges.items():
            rt = b.get("runtime") or {}
            print(
                f"  {_C.CYAN}{bid}{_C.RESET}  "
                f"{b['producer_id'][:8]}→{b['consumer_id'][:8]}  "
                f"state={b.get('state','?')}  "
                f"msgs={rt.get('messages',0)}  "
                f"errs={rt.get('errors',0)}  "
                f"circuit={rt.get('circuit','?')}"
            )
    print()
