Adapter registry — typed registry of adapters (storage, network proxy, message bus, API client, runtime shim).

Capability-token system — declarative, time-bounded capability tokens issued to internal primitives.

Policy & context validator — validate requests vs. policies, consent, and runtime context before issuing tokens.

Adapter mapper & dispatcher — map tokenized requests to the correct adapter and dispatch calls.

Sandbox & quota enforcement — per-call resource caps, syscall/network filters, timeouts, and QoS limits.

Handshake & compatibility negotiation — protocol/version negotiation, schema translation, backoff and retry rules.

Normalization & provenance tagging — canonicalize inputs/outputs and attach seed/genome/request provenance metadata.

Audit & revocation log — append-only tamper-evident logs of requests, tokens, results, and token revocations.

Graceful degradation & fallbacks — no-op adapters, read-only modes, queued actions, and operator escalation paths.

API/SDK & RBAC controls — compact API/SDK for cores, token introspection, operator approvals, and role-based access control.
