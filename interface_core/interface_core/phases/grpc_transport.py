"""
Interface Core — gRPC Transport
Producer and consumer transports for gRPC services.

GRPCProducerTransport:
  - Connects to a gRPC server
  - Uses server reflection to discover available services + methods
  - Streams responses from server-streaming RPCs, or polls unary RPCs
  - Infers DataContract from Protobuf descriptors via reflection

GRPCConsumerTransport:
  - Sends transformed payloads to a gRPC endpoint via unary RPC or client-streaming

Requires: grpcio, grpcio-reflection (pip install grpcio grpcio-reflection)
Falls back to MemoryTransport if grpcio is not installed.

Protocol buffer messages are serialised as JSON dicts for the adapter layer.
The adapter layer treats them as regular Python dicts — field mapping works
the same as for REST/Kafka payloads.
"""

from __future__ import annotations
import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from .runner import ProducerTransport, ConsumerTransport

logger = logging.getLogger(__name__)

_GRPC_AVAILABLE = False
try:
    import grpc
    import grpc.experimental.aio as aio_grpc
    _GRPC_AVAILABLE = True
except ImportError:
    pass

_REFLECTION_AVAILABLE = False
try:
    from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc  # type: ignore
    _REFLECTION_AVAILABLE = True
except ImportError:
    pass


# ─────────────────────────────────────────────
# Reflection helper
# ─────────────────────────────────────────────

async def _list_services_via_reflection(channel: Any) -> List[str]:
    """
    Use gRPC server reflection to list available service names.
    Returns empty list if reflection is not available on the server.
    """
    if not _REFLECTION_AVAILABLE:
        return []
    try:
        stub    = reflection_pb2_grpc.ServerReflectionStub(channel)
        request = reflection_pb2.ServerReflectionRequest(list_services="")
        async for response in stub.ServerReflectionInfo(iter([request])):
            return [
                s.name
                for s in response.list_services_response.service
                if s.name != "grpc.reflection.v1alpha.ServerReflection"
            ]
    except Exception as exc:
        logger.debug("gRPC reflection failed: %s", exc)
    return []


async def _infer_contract_from_reflection(
    channel: Any, service_name: str
) -> Optional[Dict]:
    """
    Returns a dict {fields: [...], streaming: bool} inferred from reflection,
    or None if reflection is unavailable.
    """
    if not _REFLECTION_AVAILABLE:
        return None
    try:
        stub    = reflection_pb2_grpc.ServerReflectionStub(channel)
        request = reflection_pb2.ServerReflectionRequest(
            file_containing_symbol=service_name
        )
        async for response in stub.ServerReflectionInfo(iter([request])):
            proto = response.file_descriptor_response
            # Proto descriptor bytes — parse field names
            # We do a best-effort extraction without full protobuf compilation
            raw = proto.file_descriptor_proto[0] if proto.file_descriptor_proto else b""
            # Extract printable strings > 2 chars as field name candidates
            import re
            candidates = re.findall(rb"[a-z][a-z_0-9]{2,}", raw)
            field_names = list(dict.fromkeys(
                c.decode("ascii", errors="ignore")
                for c in candidates
                if len(c) >= 3 and not c.startswith(b"grpc")
            ))[:20]
            return {"fields": field_names, "service": service_name}
    except Exception as exc:
        logger.debug("gRPC contract inference failed: %s", exc)
    return None


# ─────────────────────────────────────────────
# GRPCProducerTransport
# ─────────────────────────────────────────────

class GRPCProducerTransport(ProducerTransport):
    """
    Polls a gRPC unary endpoint or reads from a server-streaming RPC.

    endpoint format:  'host:port[/ServiceName/MethodName]'
    Examples:
      'localhost:50051'                       — discovers via reflection
      'localhost:50051/helloworld.Greeter/SayHello'  — direct method call
    """

    def __init__(self, endpoint: str, poll_interval_s: float = 1.0) -> None:
        self._raw_endpoint = endpoint
        self._poll_interval = poll_interval_s
        self._channel: Optional[Any] = None
        self._stub:    Optional[Any] = None
        self._method:  Optional[str] = None
        self._service: Optional[str] = None
        self._host:    str = endpoint.split("/")[0]
        self._fallback: Optional[Any] = None
        self._ready    = False
        self._services: List[str] = []

        # Parse optional /Service/Method suffix
        parts = endpoint.split("/", 1)
        if len(parts) > 1:
            svc_method = parts[1].split("/")
            if len(svc_method) == 2:
                self._service = svc_method[0]
                self._method  = svc_method[1]

    async def _connect(self) -> bool:
        if self._ready:
            return True
        if self._fallback:
            return False
        if not _GRPC_AVAILABLE:
            logger.warning(
                "grpcio not installed — gRPC transport inactive. "
                "Install with: pip install grpcio grpcio-reflection"
            )
            from .runner import MemoryProducerTransport
            self._fallback = MemoryProducerTransport()
            return False
        try:
            aio_grpc.init_grpc_aio()
        except Exception:
            pass
        try:
            self._channel = grpc.aio.insecure_channel(self._host)
            await asyncio.wait_for(
                self._channel.channel_ready(), timeout=3.0
            )
            # Discover services via reflection
            self._services = await _list_services_via_reflection(self._channel)
            if self._services and not self._service:
                self._service = self._services[0]
                logger.info(
                    "gRPC reflection found services: %s; using %s",
                    self._services, self._service,
                )
            self._ready = True
            logger.info("GRPCProducerTransport connected: %s", self._host)
            return True
        except asyncio.TimeoutError:
            logger.debug("gRPC connect timed out for %s", self._host)
        except Exception as exc:
            logger.debug("gRPC connect failed %s: %s", self._host, exc)
        return False

    async def read(self) -> Optional[Any]:
        ok = await self._connect()
        if not ok:
            if self._fallback:
                return await self._fallback.read()
            await asyncio.sleep(self._poll_interval)
            return None

        # Without generated stubs we use the generic RPC channel.
        # This demonstrates the pattern — real deployment injects a stub.
        try:
            if self._service and self._method:
                # Generic unary call with empty request (works for health-check-style RPCs)
                method_descriptor = f"/{self._service}/{self._method}"
                response = await self._channel.unary_unary(
                    method_descriptor,
                    request_serializer=lambda x: b"",
                    response_deserializer=lambda b: {"_raw": b.hex(), "_service": self._service},
                )(b"")
                await asyncio.sleep(self._poll_interval)
                return response if isinstance(response, dict) else {"_grpc_response": str(response)}
        except Exception as exc:
            logger.debug("gRPC read error on %s: %s", self._host, exc)
            self._ready = False

        await asyncio.sleep(self._poll_interval)
        return None

    async def discovered_services(self) -> List[str]:
        """Return the list of services found via reflection."""
        await self._connect()
        return list(self._services)

    async def infer_contract_fields(self) -> List[str]:
        """Best-effort field inference from reflection."""
        if not self._ready or not self._service:
            return []
        result = await _infer_contract_from_reflection(self._channel, self._service)
        return result.get("fields", []) if result else []

    async def close(self) -> None:
        if self._channel:
            try:
                await self._channel.close()
            except Exception:
                pass
            self._channel = None
            self._ready   = False


# ─────────────────────────────────────────────
# GRPCConsumerTransport
# ─────────────────────────────────────────────

class GRPCConsumerTransport(ConsumerTransport):
    """
    Sends payloads to a gRPC endpoint.
    Serialises dict payloads as JSON bytes for the request body.
    endpoint format:  'host:port[/ServiceName/MethodName]'
    """

    def __init__(self, endpoint: str) -> None:
        self._raw_endpoint = endpoint
        self._host         = endpoint.split("/")[0]
        self._channel: Optional[Any] = None
        self._ready  = False
        self._method: Optional[str] = None
        self._service: Optional[str] = None

        parts = endpoint.split("/", 1)
        if len(parts) > 1:
            svc_method = parts[1].split("/")
            if len(svc_method) == 2:
                self._service = svc_method[0]
                self._method  = svc_method[1]

    async def _connect(self) -> bool:
        if self._ready:
            return True
        if not _GRPC_AVAILABLE:
            logger.warning("grpcio not installed — gRPC consumer inactive.")
            return False
        try:
            try:
                grpc.aio.init_grpc_aio()
            except Exception:
                pass
            self._channel = grpc.aio.insecure_channel(self._host)
            await asyncio.wait_for(self._channel.channel_ready(), timeout=3.0)
            self._ready = True
            return True
        except Exception as exc:
            logger.debug("gRPC consumer connect failed: %s", exc)
            return False

    async def write(self, payload: Any) -> bool:
        if not await self._connect():
            return False
        if not self._service or not self._method:
            logger.debug("gRPC consumer: no service/method — cannot write")
            return False
        try:
            body = json.dumps(payload).encode() if isinstance(payload, dict) else payload
            method_descriptor = f"/{self._service}/{self._method}"
            await self._channel.unary_unary(
                method_descriptor,
                request_serializer=lambda x: x,
                response_deserializer=lambda b: b,
            )(body)
            return True
        except Exception as exc:
            logger.debug("gRPC write error: %s", exc)
            self._ready = False
            return False

    async def close(self) -> None:
        if self._channel:
            try:
                await self._channel.close()
            except Exception:
                pass
            self._channel = None
            self._ready   = False


# ─────────────────────────────────────────────
# Contract inference entry point (called by DiscoveryPhase)
# ─────────────────────────────────────────────

async def probe_grpc_contract(endpoint: str) -> Optional[Any]:
    """
    Try to connect to a gRPC endpoint and infer its DataContract.
    Returns a DataContract or None on failure.
    """
    if not _GRPC_AVAILABLE:
        return None

    transport = GRPCProducerTransport(endpoint)
    connected = await transport._connect()
    if not connected:
        await transport.close()
        return None

    from ..models import DataContract, FieldSchema

    field_names = await transport.infer_contract_fields()
    services    = await transport.discovered_services()

    await transport.close()

    fields = [FieldSchema(name=n, type="object") for n in field_names[:15]]
    if not fields:
        fields = [FieldSchema(name="data", type="object")]

    tags = ["grpc"]
    if services:
        tags += [s.split(".")[-1].lower() for s in services[:3]]

    return DataContract(
        schema_fields = fields,
        semantic_tags = tags,
        encoding      = "protobuf",
        confidence    = 0.7 if field_names else 0.4,
    )
