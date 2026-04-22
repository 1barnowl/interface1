"""
Interface Core — PostgreSQL Persistence Layer
Durably stores:
  - drift_log   : contract snapshots per node (rolling 7-day window)
  - bridges     : active bridge checkpoints (synced every 10s)

Schema is created automatically on first connect via CREATE TABLE IF NOT EXISTS.
Falls back silently if asyncpg is not installed or Postgres is unreachable.

Usage (called from main.py on start):
    pg = PostgresPersistence(dsn)
    await pg.connect()
    await pg.setup_schema()
    # Then pass to the relevant phases:
    #   discovery uses pg.record_contract_snapshot()
    #   lifecycle uses pg.checkpoint_bridge() / pg.delete_bridge()
    #   startup   uses pg.restore_bridges()
"""

from __future__ import annotations
import json
import logging
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_ASYNCPG_AVAILABLE = False
try:
    import asyncpg  # type: ignore
    _ASYNCPG_AVAILABLE = True
except ImportError:
    pass


# ─────────────────────────────────────────────
# Schema DDL
# ─────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS ic_drift_log (
    id          BIGSERIAL PRIMARY KEY,
    node_id     TEXT        NOT NULL,
    fingerprint TEXT        NOT NULL,
    delta       FLOAT       NOT NULL DEFAULT 0.0,
    contract    JSONB       NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_drift_log_node_id   ON ic_drift_log(node_id);
CREATE INDEX IF NOT EXISTS ix_drift_log_recorded  ON ic_drift_log(recorded_at);

CREATE TABLE IF NOT EXISTS ic_bridges (
    producer_id TEXT        NOT NULL,
    consumer_id TEXT        NOT NULL,
    bridge_id   TEXT        NOT NULL,
    adapter     JSONB       NOT NULL,
    score       FLOAT       NOT NULL DEFAULT 0.5,
    provenance  TEXT,
    created_at  FLOAT       NOT NULL,
    updated_at  FLOAT       NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW()),
    PRIMARY KEY (producer_id, consumer_id)
);

CREATE TABLE IF NOT EXISTS ic_audit_log (
    id          BIGSERIAL    PRIMARY KEY,
    verdict     TEXT         NOT NULL,
    producer_id TEXT         NOT NULL,
    consumer_id TEXT         NOT NULL,
    score       FLOAT,
    reason      TEXT,
    recorded_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_audit_log_time ON ic_audit_log(recorded_at);
"""

# Purge drift log entries older than 7 days
_PURGE_DRIFT = """
DELETE FROM ic_drift_log
WHERE recorded_at < NOW() - INTERVAL '7 days'
"""


# ─────────────────────────────────────────────
# Persistence facade
# ─────────────────────────────────────────────

class PostgresPersistence:
    """
    Thin asyncpg wrapper for Interface Core durable state.
    All methods degrade gracefully to no-ops when Postgres is unavailable.
    """

    def __init__(self, dsn: str) -> None:
        self._dsn  = dsn
        self._pool: Optional[Any] = None
        self._ok   = False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def connect(self) -> bool:
        """Attempt connection. Returns True if successful."""
        if not _ASYNCPG_AVAILABLE:
            logger.info("asyncpg not installed — Postgres persistence disabled. "
                        "Install with: pip install asyncpg")
            return False
        try:
            self._pool = await asyncpg.create_pool(
                self._dsn,
                min_size=1, max_size=4,
                command_timeout=10,
            )
            self._ok = True
            logger.info("Postgres connected: %s", self._dsn.split("@")[-1])
            return True
        except Exception as exc:
            logger.warning("Postgres unavailable (%s) — falling back to Redis/memory", exc)
            return False

    async def setup_schema(self) -> None:
        """Create tables if they don't exist."""
        if not self._ok:
            return
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(_DDL)
            logger.info("Postgres schema ready")
        except Exception as exc:
            logger.warning("Postgres schema setup failed: %s", exc)

    async def close(self) -> None:
        if self._pool:
            try:
                await self._pool.close()
            except Exception:
                pass
            self._pool = None
            self._ok   = False

    # ── Drift log ──────────────────────────────────────────────────────────────

    async def record_contract_snapshot(self, snapshot: Any) -> None:
        """Persist a ContractSnapshot to ic_drift_log."""
        if not self._ok:
            return
        try:
            contract_json = json.dumps({
                "fields":        [{"name": f.name, "type": f.type}
                                   for f in snapshot.contract.schema_fields],
                "tags":          snapshot.contract.semantic_tags,
                "encoding":      snapshot.contract.encoding,
                "emission_rate": snapshot.contract.emission_rate_hz,
                "confidence":    snapshot.contract.confidence,
            })
            async with self._pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO ic_drift_log (node_id, fingerprint, delta, contract) "
                    "VALUES ($1, $2, $3, $4::jsonb)",
                    snapshot.node_id,
                    snapshot.fingerprint,
                    snapshot.delta,
                    contract_json,
                )
        except Exception as exc:
            logger.debug("Drift log write failed: %s", exc)

    async def recent_drift(self, node_id: str, limit: int = 100) -> List[Dict]:
        """Fetch recent contract snapshots for a node."""
        if not self._ok:
            return []
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT node_id, fingerprint, delta, contract, recorded_at "
                    "FROM ic_drift_log WHERE node_id = $1 "
                    "ORDER BY recorded_at DESC LIMIT $2",
                    node_id, limit,
                )
            return [dict(r) for r in rows]
        except Exception as exc:
            logger.debug("Drift log read failed: %s", exc)
            return []

    async def purge_old_drift(self) -> int:
        """Remove drift entries older than 7 days. Returns rows deleted."""
        if not self._ok:
            return 0
        try:
            async with self._pool.acquire() as conn:
                result = await conn.execute(_PURGE_DRIFT)
            deleted = int(result.split()[-1]) if result else 0
            if deleted:
                logger.info("Purged %d old drift log entries", deleted)
            return deleted
        except Exception as exc:
            logger.debug("Drift log purge failed: %s", exc)
            return 0

    # ── Bridge checkpoints ────────────────────────────────────────────────────

    async def checkpoint_bridge(self, bridge: Any, candidate: Any) -> None:
        """Upsert bridge state into ic_bridges."""
        if not self._ok:
            return
        try:
            adapter_json = json.dumps({
                "producer_protocol": bridge.adapter.producer_protocol.value,
                "consumer_protocol": bridge.adapter.consumer_protocol.value,
                "backpressure_mode": bridge.adapter.backpressure_mode,
                "version":           bridge.adapter.version,
                "field_mappings": [
                    {"src_field": m.src_field, "dst_field": m.dst_field,
                     "transform": m.transform, "confidence": m.confidence}
                    for m in bridge.adapter.field_mappings
                ],
            })
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO ic_bridges
                        (producer_id, consumer_id, bridge_id, adapter, score,
                         provenance, created_at, updated_at)
                    VALUES ($1,$2,$3,$4::jsonb,$5,$6,$7,$8)
                    ON CONFLICT (producer_id, consumer_id)
                    DO UPDATE SET
                        bridge_id  = EXCLUDED.bridge_id,
                        adapter    = EXCLUDED.adapter,
                        score      = EXCLUDED.score,
                        updated_at = EXCLUDED.updated_at
                    """,
                    bridge.producer_id,
                    bridge.consumer_id,
                    bridge.id,
                    adapter_json,
                    float(candidate.score.composite),
                    bridge.provenance_key,
                    float(bridge.created_at),
                    float(time.time()),
                )
        except Exception as exc:
            logger.debug("Bridge checkpoint failed: %s", exc)

    async def delete_bridge(self, producer_id: str, consumer_id: str) -> None:
        """Remove a bridge from ic_bridges."""
        if not self._ok:
            return
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM ic_bridges WHERE producer_id=$1 AND consumer_id=$2",
                    producer_id, consumer_id,
                )
        except Exception as exc:
            logger.debug("Bridge delete failed: %s", exc)

    async def restore_bridges(self) -> List[Dict]:
        """
        Fetch all persisted bridges from ic_bridges.
        Returns list of dicts ready for _dict_to_bridge_and_candidate().
        """
        if not self._ok:
            return []
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT producer_id, consumer_id, bridge_id, adapter, "
                    "score, provenance, created_at FROM ic_bridges "
                    "ORDER BY created_at ASC"
                )
            result = []
            for row in rows:
                adapter_data = json.loads(row["adapter"])
                result.append({
                    "bridge_id":    row["bridge_id"],
                    "producer_id":  row["producer_id"],
                    "consumer_id":  row["consumer_id"],
                    "provenance":   row["provenance"],
                    "created_at":   row["created_at"],
                    "adapter":      adapter_data,
                    "score":        row["score"],
                })
            logger.info("Postgres restored %d bridges", len(result))
            return result
        except Exception as exc:
            logger.warning("Bridge restore from Postgres failed: %s", exc)
            return []

    # ── Audit log ──────────────────────────────────────────────────────────────

    async def write_audit(
        self, verdict: str, producer_id: str, consumer_id: str,
        score: float, reason: str
    ) -> None:
        """Persist an audit entry to ic_audit_log."""
        if not self._ok:
            return
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO ic_audit_log "
                    "(verdict, producer_id, consumer_id, score, reason) "
                    "VALUES ($1,$2,$3,$4,$5)",
                    verdict, producer_id, consumer_id, float(score), reason,
                )
        except Exception as exc:
            logger.debug("Audit log write failed: %s", exc)

    async def recent_audit(self, limit: int = 100) -> List[Dict]:
        """Fetch most recent audit entries."""
        if not self._ok:
            return []
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT verdict, producer_id, consumer_id, score, reason, recorded_at "
                    "FROM ic_audit_log ORDER BY recorded_at DESC LIMIT $1",
                    limit,
                )
            return [dict(r) for r in rows]
        except Exception as exc:
            logger.debug("Audit log read failed: %s", exc)
            return []

    @property
    def connected(self) -> bool:
        return self._ok
