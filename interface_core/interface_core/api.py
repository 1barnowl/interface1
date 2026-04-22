"""
Interface Core — HTTP API (Improvement #3)
Minimal REST API so any process can register nodes and query the daemon over HTTP.

Routes:
  POST   /nodes                         register a node
  GET    /nodes                         list all registered nodes
  GET    /status                        system snapshot
  GET    /bridges                       list active bridges + stats
  GET    /bridges/{pid}/{cid}           single bridge detail
  POST   /bridges/{pid}/{cid}/approve   operator-approve a staged bridge
  POST   /bridges/{pid}/{cid}/reject    operator-reject a staged bridge
  GET    /audit                         recent audit log entries
  GET    /queue                         bridge candidate queue depth + top items

Uses aiohttp if installed; prints a clear install hint and skips gracefully otherwise.
"""

from __future__ import annotations
import json
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

_AIOHTTP_AVAILABLE = False
try:
    import aiohttp
    from aiohttp import web
    _AIOHTTP_AVAILABLE = True
except ImportError:
    pass


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _json(data: Any, status: int = 200):
    return web.Response(
        text=json.dumps(data, default=str),
        status=status,
        content_type="application/json",
    )


def _node_to_dict(node: Any) -> dict:
    return {
        "id":       node.id,
        "protocol": node.protocol.value,
        "role":     node.role.value,
        "endpoint": node.endpoint,
        "last_seen": node.last_seen,
        "contract": {
            "fields":        [{"name": f.name, "type": f.type} for f in node.contract.schema_fields],
            "tags":          node.contract.semantic_tags,
            "encoding":      node.contract.encoding,
            "emission_rate": node.contract.emission_rate_hz,
            "confidence":    node.contract.confidence,
        },
    }


def _bridge_to_dict(bridge: Any, runner_stats: Optional[dict] = None) -> dict:
    d = {
        "id":          bridge.id,
        "producer_id": bridge.producer_id,
        "consumer_id": bridge.consumer_id,
        "state":       bridge.state.value,
        "created_at":  bridge.created_at,
        "provenance":  bridge.provenance_key,
        "adapter": {
            "producer_protocol": bridge.adapter.producer_protocol.value,
            "consumer_protocol": bridge.adapter.consumer_protocol.value,
            "backpressure_mode": bridge.adapter.backpressure_mode,
            "version":           bridge.adapter.version,
            "field_mappings": [
                {
                    "src":       m.src_field,
                    "dst":       m.dst_field,
                    "transform": m.transform,
                    "confidence": m.confidence,
                }
                for m in bridge.adapter.field_mappings
            ],
        },
    }
    if runner_stats:
        d["runtime"] = runner_stats
    return d


# ─────────────────────────────────────────────
# API Application
# ─────────────────────────────────────────────

def build_app(core: Any) -> Any:
    """
    Build and return the aiohttp Application wired to an InterfaceCore instance.
    Returns None if aiohttp is not installed.
    """
    if not _AIOHTTP_AVAILABLE:
        logger.warning(
            "aiohttp not installed — HTTP API disabled. "
            "Install with: pip install aiohttp"
        )
        return None

    import os
    _API_KEY = os.environ.get("IC_API_KEY", "").strip()

    # ── API key authentication middleware ─────────────────────────────────────
    # When IC_API_KEY is set every request must include:
    #   Authorization: Bearer <key>   OR   X-API-Key: <key>
    # /health and /metrics are always public (Kubernetes liveness probes).
    # Empty IC_API_KEY = open mode (no auth required).
    async def _auth(request: web.Request, handler):
        if not _API_KEY or request.path in ("/health", "/metrics"):
            return await handler(request)
        provided = ""
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            provided = auth[7:].strip()
        else:
            provided = request.headers.get("X-API-Key", "").strip()
        if provided != _API_KEY:
            logger.warning(
                "API AUTH FAIL  path=%s  remote=%s", request.path, request.remote
            )
            return _json({"error": "unauthorized"}, 401)
        return await handler(request)

    # ── Request rate limiter ──────────────────────────────────────────────────
    # IC_API_RATE_LIMIT requests per minute per remote IP (default 120)
    # Write endpoints (POST/DELETE) count double.
    # 0 = disabled.
    import collections, time as _time
    _RATE_LIMIT = int(os.environ.get('IC_API_RATE_LIMIT', '120'))
    _rate_counters: dict = collections.defaultdict(list)   # ip → [timestamps]

    async def _rate_limit(request, handler):
        if _RATE_LIMIT <= 0 or request.path in ('/health', '/metrics'):
            return await handler(request)
        ip    = request.remote or 'unknown'
        now   = _time.time()
        # Write endpoints count double
        cost  = 2 if request.method in ('POST', 'DELETE', 'PUT', 'PATCH') else 1
        hits  = _rate_counters[ip]
        # Purge entries older than 60 seconds
        _rate_counters[ip] = [t for t in hits if now - t < 60.0]
        effective = sum(cost for _ in _rate_counters[ip])   # simplified: use len
        if len(_rate_counters[ip]) >= _RATE_LIMIT:
            logger.warning('API RATE_LIMIT  ip=%s  path=%s', ip, request.path)
            return _json({'error': 'rate limit exceeded', 'limit': _RATE_LIMIT,
                          'reset_seconds': 60}, 429)
        for _ in range(cost):
            _rate_counters[ip].append(now)
        return await handler(request)

    app = web.Application(middlewares=[_auth, _rate_limit])

    # ── POST /nodes ────────────────────────────────────────────────────────
    async def post_nodes(request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            return _json({"error": "invalid JSON body"}, 400)

        protocol = body.get("protocol")
        endpoint = body.get("endpoint")
        if not protocol or not endpoint:
            return _json({"error": "protocol and endpoint are required"}, 400)

        try:
            node_id = core.register_node(
                protocol        = protocol,
                endpoint        = endpoint,
                fields          = body.get("fields"),
                tags            = body.get("tags"),
                role            = body.get("role"),
                emission_rate_hz= body.get("emission_rate_hz"),
            )
            return _json({"node_id": node_id, "endpoint": endpoint}, 201)
        except Exception as exc:
            logger.exception("POST /nodes error")
            return _json({"error": str(exc)}, 500)

    # ── GET /nodes ─────────────────────────────────────────────────────────
    async def get_nodes(request: web.Request) -> web.Response:
        nodes = core.registry.all_nodes()
        return _json([_node_to_dict(n) for n in nodes])

    # ── GET /status ────────────────────────────────────────────────────────
    async def get_status(request: web.Request) -> web.Response:
        s = core.status()
        s["store_backend"] = core.store.backend_name if hasattr(core, "store") else "n/a"
        return _json(s)

    # ── GET /bridges ───────────────────────────────────────────────────────
    async def get_bridges(request: web.Request) -> web.Response:
        result = []
        for (pid, cid), (bridge, _adapter) in core.active_bridges.items():
            runner      = core._runners.get(f"{pid}:{cid}")
            stats       = runner.stats if runner else None
            result.append(_bridge_to_dict(bridge, stats))
        return _json(result)

    # ── GET /bridges/{pid}/{cid} ───────────────────────────────────────────
    async def get_bridge(request: web.Request) -> web.Response:
        pid = request.match_info["pid"]
        cid = request.match_info["cid"]
        item = core.active_bridges.get((pid, cid))
        if not item:
            return _json({"error": "bridge not found"}, 404)
        bridge, _ = item
        runner     = core._runners.get(f"{pid}:{cid}")
        return _json(_bridge_to_dict(bridge, runner.stats if runner else None))

    # ── POST /bridges/{pid}/{cid}/approve ─────────────────────────────────
    async def approve_bridge(request: web.Request) -> web.Response:
        pid = request.match_info["pid"]
        cid = request.match_info["cid"]
        ok  = core.approve_bridge(pid, cid)
        if ok:
            return _json({"approved": True, "producer_id": pid, "consumer_id": cid})
        return _json({"error": "no pending approval for this pair"}, 404)

    # ── POST /bridges/{pid}/{cid}/reject ──────────────────────────────────
    async def reject_bridge(request: web.Request) -> web.Response:
        pid = request.match_info["pid"]
        cid = request.match_info["cid"]
        ok  = core.reject_bridge(pid, cid)
        if ok:
            return _json({"rejected": True, "producer_id": pid, "consumer_id": cid})
        return _json({"error": "no pending rejection for this pair"}, 404)

    # ── GET /audit ─────────────────────────────────────────────────────────
    async def get_audit(request: web.Request) -> web.Response:
        n = int(request.rel_url.query.get("n", "50"))
        return _json(core.audit_log(n))

    # ── GET /queue ─────────────────────────────────────────────────────────
    async def get_queue(request: web.Request) -> web.Response:
        return _json({
            "bridge_queue_depth":    len(core.bridge_queue),
            "synthesis_queue_empty": core.synthesis_queue.empty(),
            "pending_approvals":     core.policy_engine.pending_approvals(),
        })

    # ── DELETE /nodes/{node_id} ──────────────────────────────────────────────
    async def delete_node(request: web.Request) -> web.Response:
        node_id = request.match_info['node_id']
        if not core.registry.get(node_id):
            return _json({'error': 'node not found'}, 404)
        affected = [(p,c) for (p,c) in list(core.active_bridges.keys()) if p==node_id or c==node_id]
        # deregister_node is atomic: removes from registry + graph + drift_log together
        core.deregister_node(node_id)
        logger.info('API: removed node %s  bridges=%d', node_id, len(affected))
        return _json({'deleted': node_id, 'affected_bridges': [{'producer_id':p,'consumer_id':c} for p,c in affected]})

    # ── GET /nodes/{node_id}/drift ───────────────────────────────────────────
    async def get_node_drift(request: web.Request) -> web.Response:
        node_id = request.match_info['node_id']
        history = core.drift_log.history(node_id)
        return _json([{'node_id':s.node_id,'fingerprint':s.fingerprint,'delta':s.delta,
                        'timestamp':s.timestamp,'fields':[f.name for f in s.contract.schema_fields],
                        'tags':s.contract.semantic_tags} for s in history[-50:]])

    # ── POST /bridges/trigger ────────────────────────────────────────────────
    async def trigger_bridge(request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            return _json({'error': 'invalid JSON'}, 400)
        pid = body.get('producer_id'); cid = body.get('consumer_id')
        if not pid or not cid:
            return _json({'error': 'producer_id and consumer_id required'}, 400)
        if not core.registry.get(pid): return _json({'error': f'producer {pid} not found'}, 404)
        if not core.registry.get(cid): return _json({'error': f'consumer {cid} not found'}, 404)
        if (pid, cid) in core.active_bridges: return _json({'error': 'bridge already active'}, 409)
        from interface_core.models import BridgeCandidate, MatchScore
        await core.synthesis_queue.push(BridgeCandidate(producer_id=pid, consumer_id=cid,
                                                         score=MatchScore(composite=1.0)))
        return _json({'queued': True, 'producer_id': pid, 'consumer_id': cid}, 202)

    # ── GET /health ──────────────────────────────────────────────────────────
    async def health(request: web.Request) -> web.Response:
        """Kubernetes liveness + readiness probe.
        200 ok      — fully healthy, all services connected
        200 degraded — running but missing durable services (bridges may not survive restart)
        """
        s      = core.status()
        degrad = getattr(core, '_degraded_services', [])
        return _json({
            'status':            'degraded' if degrad else 'ok',
            'nodes':             s.get('nodes', 0),
            'bridges':           s.get('active_bridges', 0),
            'store':             s.get('store_backend', '?'),
            'postgres':          s.get('postgres', False),
            'degraded_services': degrad,
            'durable':           len(degrad) == 0,
        })

    # ── GET /events  (WebSocket — real-time event stream) ─────────────────
    async def ws_events(request: web.Request) -> web.WebSocketResponse:
        """
        WebSocket endpoint that streams every Interface Core Event as JSON.
        Connect with: ws://host:8000/events
        Optional query param: ?types=new_node,bridge_live  to filter event types
        """
        ws = web.WebSocketResponse(heartbeat=30)
        await ws.prepare(request)

        # Parse optional type filter
        wanted = set()
        if 'types' in request.rel_url.query:
            wanted = {t.strip() for t in request.rel_url.query['types'].split(',')}

        # Subscribe to all events
        q = core.event_bus.subscribe(None)
        logger.info("WS /events client connected  filter=%s", wanted or 'all')

        try:
            while not ws.closed:
                event = await core.event_bus.next_event(q, timeout=1.0)
                if event is None:
                    continue
                if wanted and event.type.value not in wanted:
                    continue
                try:
                    await ws.send_json({
                        'type':      event.type.value,
                        'node_id':   event.node_id,
                        'bridge_id': event.bridge_id,
                        'payload':   event.payload,
                        'timestamp': event.timestamp,
                    })
                except Exception:
                    break
        finally:
            logger.info("WS /events client disconnected")
        return ws

    # ── Register routes ────────────────────────────────────────────────────
    app.router.add_post("/nodes",                            post_nodes)
    app.router.add_get ("/nodes",                            get_nodes)
    app.router.add_get ("/status",                           get_status)
    app.router.add_get ("/bridges",                          get_bridges)
    app.router.add_get ("/bridges/{pid}/{cid}",              get_bridge)
    app.router.add_post("/bridges/{pid}/{cid}/approve",      approve_bridge)
    app.router.add_post("/bridges/{pid}/{cid}/reject",       reject_bridge)
    app.router.add_get ("/audit",                            get_audit)
    app.router.add_get   ("/queue",                           get_queue)
    app.router.add_delete("/nodes/{node_id}",                delete_node)
    app.router.add_get   ("/nodes/{node_id}/drift",          get_node_drift)
    app.router.add_post  ("/bridges/trigger",                trigger_bridge)
    app.router.add_get   ("/health",                         health)
    app.router.add_get   ("/events",                         ws_events)

    return app


# ─────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────

async def start_api(core: Any, host: str = "0.0.0.0", port: int = 8000) -> Optional[Any]:
    """
    Start the HTTP API server as a background task.
    Returns the runner handle (or None if aiohttp not available).
    """
    app = build_app(core)
    if app is None:
        return None

    runner = web.AppRunner(app)
    await runner.setup()
    site   = web.TCPSite(runner, host, port)
    await site.start()
    logger.info("HTTP API listening on http://%s:%d", host, port)
    return runner
