"""
Interface Core — Bridge Store (Improvement #2)
Persists active bridge state to Redis so restarts don't lose approved bridges.

On BRIDGE_LIVE  → store.save(bridge, candidate)
On BRIDGE_TORN  → store.delete(producer_id, consumer_id)
On boot         → store.restore_all() → list of (bridge, candidate) to re-instantiate

Falls back to a plain in-memory dict when Redis is unavailable,
so the daemon still runs with zero infrastructure.
"""

from __future__ import annotations
import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

# Module-level import with lazy fallback (avoids circular imports at package load)
_models = None

def _get_models():
    global _models
    if _models is None:
        from interface_core import models
        _models = models
    return _models

logger = logging.getLogger(__name__)

_BRIDGE_PREFIX  = "ic:bridge:"
_BRIDGE_SET_KEY = "ic:bridges"


# ─────────────────────────────────────────────
# Serialisation helpers
# ─────────────────────────────────────────────

def _bridge_to_dict(bridge: Any, candidate: Any) -> Dict:
    """Flatten bridge + candidate into a JSON-serialisable dict."""
    return {
        "bridge_id":    bridge.id,
        "producer_id":  bridge.producer_id,
        "consumer_id":  bridge.consumer_id,
        "provenance":   bridge.provenance_key,
        "created_at":   bridge.created_at,
        "adapter": {
            "producer_protocol": bridge.adapter.producer_protocol.value,
            "consumer_protocol": bridge.adapter.consumer_protocol.value,
            "backpressure_mode": bridge.adapter.backpressure_mode,
            "version":           bridge.adapter.version,
            "field_mappings": [
                {
                    "src_field":  m.src_field,
                    "dst_field":  m.dst_field,
                    "transform":  m.transform,
                    "confidence": m.confidence,
                }
                for m in bridge.adapter.field_mappings
            ],
        },
        "score": candidate.score.composite,
        "saved_at": time.time(),
    }


def _dict_to_bridge_and_candidate(data: Dict) -> Tuple[Any, Any]:
    """Reconstruct minimal Bridge + BridgeCandidate from stored dict."""
    m = _get_models()
    AdapterSpec, Bridge, BridgeState, BridgeCandidate = (
        m.AdapterSpec, m.Bridge, m.BridgeState, m.BridgeCandidate
    )
    FieldMapping, MatchScore, Protocol = m.FieldMapping, m.MatchScore, m.Protocol

    field_mappings = [
        FieldMapping(
            src_field=m["src_field"],
            dst_field=m["dst_field"],
            transform=m.get("transform", "direct"),
            confidence=m.get("confidence", 1.0),
        )
        for m in data["adapter"].get("field_mappings", [])
    ]

    adapter_spec = AdapterSpec(
        producer_protocol=Protocol(data["adapter"]["producer_protocol"]),
        consumer_protocol=Protocol(data["adapter"]["consumer_protocol"]),
        field_mappings=field_mappings,
        backpressure_mode=data["adapter"].get("backpressure_mode", "token_bucket"),
        version=data["adapter"].get("version", "0.1.0"),
    )

    bridge = Bridge(
        id=data["bridge_id"],
        producer_id=data["producer_id"],
        consumer_id=data["consumer_id"],
        adapter=adapter_spec,
        state=BridgeState.LIVE,
        provenance_key=data.get("provenance"),
        created_at=data.get("created_at", time.time()),
    )

    candidate = BridgeCandidate(
        producer_id=data["producer_id"],
        consumer_id=data["consumer_id"],
        score=MatchScore(composite=data.get("score", 0.5)),
    )

    return bridge, candidate


# ─────────────────────────────────────────────
# In-memory fallback store
# ─────────────────────────────────────────────

class _MemoryBridgeStore:
    """Thread-safe in-memory store — used when Redis is unavailable."""

    def __init__(self) -> None:
        self._data: Dict[str, Dict] = {}

    async def save(self, bridge: Any, candidate: Any) -> None:
        key = f"{bridge.producer_id}:{bridge.consumer_id}"
        self._data[key] = _bridge_to_dict(bridge, candidate)
        logger.debug("BridgeStore(mem) saved %s", key)

    async def delete(self, producer_id: str, consumer_id: str) -> None:
        key = f"{producer_id}:{consumer_id}"
        self._data.pop(key, None)
        logger.debug("BridgeStore(mem) deleted %s", key)

    async def restore_all(self) -> List[Tuple[Any, Any]]:
        results = []
        for data in self._data.values():
            try:
                results.append(_dict_to_bridge_and_candidate(data))
            except Exception as exc:
                logger.warning("BridgeStore(mem) restore failed: %s", exc)
        logger.info("BridgeStore(mem) restored %d bridges", len(results))
        return results

    async def count(self) -> int:
        return len(self._data)


# ─────────────────────────────────────────────
# Redis store
# ─────────────────────────────────────────────

class _RedisBridgeStore:
    """
    Persists bridge state to Redis.
    Key schema:
      ic:bridge:<producer_id>:<consumer_id>  →  JSON blob
      ic:bridges                             →  Redis SET of all keys
    """

    def __init__(self, redis_url: str) -> None:
        self._url    = redis_url
        self._client = None

    async def _get_client(self):
        if self._client is None:
            try:
                import redis.asyncio as aioredis
                self._client = aioredis.from_url(self._url, decode_responses=True)
                await self._client.ping()
                logger.info("BridgeStore connected to Redis at %s", self._url)
            except Exception as exc:
                # Distinguish "module not found" from "connection refused"
                if "No module named" in str(exc):
                    logger.warning(
                        "Redis unavailable (redis package not installed). "
                        "Install with: pip install redis  — using in-memory store"
                    )
                else:
                    logger.warning("Redis unavailable (%s) — using in-memory store", exc)
                self._client = None
        return self._client

    async def save(self, bridge: Any, candidate: Any) -> None:
        client = await self._get_client()
        if client is None:
            return
        key   = f"{_BRIDGE_PREFIX}{bridge.producer_id}:{bridge.consumer_id}"
        value = json.dumps(_bridge_to_dict(bridge, candidate))
        try:
            pipe = client.pipeline()
            pipe.set(key, value)
            pipe.sadd(_BRIDGE_SET_KEY, key)
            await pipe.execute()
            logger.debug("BridgeStore(redis) saved %s", key)
        except Exception as exc:
            logger.warning("BridgeStore save failed: %s", exc)

    async def delete(self, producer_id: str, consumer_id: str) -> None:
        client = await self._get_client()
        if client is None:
            return
        key = f"{_BRIDGE_PREFIX}{producer_id}:{consumer_id}"
        try:
            pipe = client.pipeline()
            pipe.delete(key)
            pipe.srem(_BRIDGE_SET_KEY, key)
            await pipe.execute()
            logger.debug("BridgeStore(redis) deleted %s", key)
        except Exception as exc:
            logger.warning("BridgeStore delete failed: %s", exc)

    async def restore_all(self) -> List[Tuple[Any, Any]]:
        client = await self._get_client()
        if client is None:
            return []
        results = []
        try:
            keys = await client.smembers(_BRIDGE_SET_KEY)
            for key in keys:
                raw = await client.get(key)
                if raw:
                    try:
                        data = json.loads(raw)
                        results.append(_dict_to_bridge_and_candidate(data))
                    except Exception as exc:
                        logger.warning("BridgeStore restore failed for %s: %s", key, exc)
        except Exception as exc:
            logger.warning("BridgeStore restore_all failed: %s", exc)
        logger.info("BridgeStore(redis) restored %d bridges", len(results))
        return results

    async def count(self) -> int:
        client = await self._get_client()
        if client is None:
            return 0
        try:
            return await client.scard(_BRIDGE_SET_KEY)
        except Exception:
            return 0


# ─────────────────────────────────────────────
# Factory
# ─────────────────────────────────────────────

class BridgeStore:
    """
    Public facade.  Tries Redis first; falls back to in-memory transparently.
    Caller never needs to know which backend is active.
    """

    def __init__(self, redis_url: str) -> None:
        self._redis  = _RedisBridgeStore(redis_url)
        self._memory = _MemoryBridgeStore()
        self._using_redis = False

    async def _backend(self):
        """Try Redis first, return memory store if Redis fails."""
        client = await self._redis._get_client()
        if client is not None:
            self._using_redis = True
            return self._redis
        return self._memory

    async def save(self, bridge: Any, candidate: Any) -> None:
        backend = await self._backend()
        await backend.save(bridge, candidate)
        # Always keep memory in sync as a safety net
        if self._using_redis:
            await self._memory.save(bridge, candidate)

    async def delete(self, producer_id: str, consumer_id: str) -> None:
        backend = await self._backend()
        await backend.delete(producer_id, consumer_id)
        if self._using_redis:
            await self._memory.delete(producer_id, consumer_id)

    async def restore_all(self) -> List[Tuple[Any, Any]]:
        backend = await self._backend()
        return await backend.restore_all()

    async def count(self) -> int:
        backend = await self._backend()
        return await backend.count()

    @property
    def backend_name(self) -> str:
        return "redis" if self._using_redis else "memory"
