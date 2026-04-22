"""
Interface Core — Transport Implementations
All ProducerTransport / ConsumerTransport subclasses beyond the basics in runner.py.

Kafka:
  KafkaProducerTransport  — consumes from a Kafka topic (aiokafka)
  KafkaConsumerTransport  — produces to a Kafka topic (aiokafka)

MQTT:
  MQTTProducerTransport   — subscribes to an MQTT topic (asyncio-mqtt)
  MQTTConsumerTransport   — publishes to an MQTT topic (asyncio-mqtt)

Socket:
  SocketProducerTransport — reads newline-delimited JSON from a TCP socket
  SocketConsumerTransport — writes newline-delimited JSON to a TCP socket

All transports fall back gracefully when their library is absent.
"""

from __future__ import annotations
import asyncio
import json
import logging
from typing import Any, Optional

from .runner import ProducerTransport, ConsumerTransport

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Kafka
# ─────────────────────────────────────────────

class KafkaProducerTransport(ProducerTransport):
    """
    Reads messages from a Kafka topic using aiokafka.
    endpoint format: 'broker:port/topic'
    Falls back to MemoryProducerTransport if aiokafka is absent.
    """

    def __init__(self, endpoint: str) -> None:
        self._endpoint  = endpoint
        self._consumer  = None
        self._ready     = False
        self._fallback  = None

        # Parse 'broker:port/topic'
        if "/" in endpoint:
            broker_part, self._topic = endpoint.rsplit("/", 1)
            self._brokers = broker_part
        else:
            self._brokers = endpoint
            self._topic   = "interface-core-default"

    async def _ensure_consumer(self) -> bool:
        if self._ready:
            return True
        if self._fallback:
            return False
        try:
            from aiokafka import AIOKafkaConsumer
            self._consumer = AIOKafkaConsumer(
                self._topic,
                bootstrap_servers=self._brokers,
                auto_offset_reset="latest",
                consumer_timeout_ms=100,
                group_id="interface-core",
            )
            await self._consumer.start()
            self._ready = True
            logger.info("KafkaProducerTransport connected: %s / %s", self._brokers, self._topic)
            return True
        except ImportError:
            logger.warning("aiokafka not installed — Kafka transport inactive. pip install aiokafka")
            from .runner import MemoryProducerTransport
            self._fallback = MemoryProducerTransport()
            return False
        except Exception as exc:
            logger.warning("Kafka connect failed (%s) — will retry", exc)
            return False

    async def read(self) -> Optional[Any]:
        ok = await self._ensure_consumer()
        if not ok:
            return await self._fallback.read() if self._fallback else None
        try:
            msg = await asyncio.wait_for(
                self._consumer.__anext__(), timeout=0.2
            )
            raw = msg.value
            try:
                return json.loads(raw)
            except Exception:
                return {"data": raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw}
        except (asyncio.TimeoutError, StopAsyncIteration):
            return None
        except Exception as exc:
            logger.debug("KafkaProducerTransport read error: %s", exc)
            self._ready = False   # trigger reconnect next cycle
            return None

    async def close(self) -> None:
        if self._consumer:
            try:
                await self._consumer.stop()
            except Exception:
                pass
            self._consumer = None
            self._ready    = False


class KafkaConsumerTransport(ConsumerTransport):
    """
    Produces messages to a Kafka topic using aiokafka.
    endpoint format: 'broker:port/topic'
    """

    def __init__(self, endpoint: str) -> None:
        self._endpoint = endpoint
        self._producer = None
        self._ready    = False

        if "/" in endpoint:
            broker_part, self._topic = endpoint.rsplit("/", 1)
            self._brokers = broker_part
        else:
            self._brokers = endpoint
            self._topic   = "interface-core-default"

    async def _ensure_producer(self) -> bool:
        if self._ready:
            return True
        try:
            from aiokafka import AIOKafkaProducer
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._brokers,
                value_serializer=lambda v: json.dumps(v).encode() if not isinstance(v, bytes) else v,
            )
            await self._producer.start()
            self._ready = True
            logger.info("KafkaConsumerTransport connected: %s / %s", self._brokers, self._topic)
            return True
        except ImportError:
            logger.warning("aiokafka not installed — Kafka output inactive. pip install aiokafka")
            return False
        except Exception as exc:
            logger.warning("Kafka producer connect failed (%s) — will retry", exc)
            return False

    async def write(self, payload: Any) -> bool:
        ok = await self._ensure_producer()
        if not ok:
            return False
        try:
            value = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
            await self._producer.send(self._topic, value=value)
            return True
        except Exception as exc:
            logger.debug("KafkaConsumerTransport write error: %s", exc)
            self._ready = False
            return False

    async def close(self) -> None:
        if self._producer:
            try:
                await self._producer.stop()
            except Exception:
                pass
            self._producer = None
            self._ready    = False


# ─────────────────────────────────────────────
# MQTT
# ─────────────────────────────────────────────

def _parse_mqtt_endpoint(endpoint: str):
    """Parse 'mqtt://host:port/topic' or 'host:port/topic'."""
    ep = endpoint.replace("mqtt://", "").replace("mqtts://", "")
    if "/" in ep:
        host_port, topic = ep.split("/", 1)
    else:
        host_port = ep
        topic     = "interface-core/#"
    if ":" in host_port:
        host, port_s = host_port.rsplit(":", 1)
        try:
            port = int(port_s)
        except ValueError:
            port = 1883
    else:
        host = host_port
        port = 1883
    return host, port, topic


class MQTTProducerTransport(ProducerTransport):
    """
    Subscribes to an MQTT topic and emits received messages.
    Requires asyncio-mqtt: pip install asyncio-mqtt
    """

    def __init__(self, endpoint: str) -> None:
        self._host, self._port, self._topic = _parse_mqtt_endpoint(endpoint)
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=2048)
        self._client  = None
        self._task: Optional[asyncio.Task] = None
        self._ready   = False
        self._fallback = None

    async def _connect(self) -> bool:
        if self._ready:
            return True
        try:
            import asyncio_mqtt as aiomqtt
            self._client = aiomqtt.Client(self._host, self._port)
            self._task   = asyncio.create_task(self._listen(), name="mqtt-listen")
            self._ready  = True
            logger.info("MQTT connected: %s:%d / %s", self._host, self._port, self._topic)
            return True
        except ImportError:
            logger.warning("asyncio-mqtt not installed — MQTT transport inactive. pip install asyncio-mqtt")
            from .runner import MemoryProducerTransport
            self._fallback = MemoryProducerTransport()
            return False
        except Exception as exc:
            logger.warning("MQTT connect failed: %s", exc)
            return False

    async def _listen(self) -> None:
        try:
            async with self._client as client:
                async with client.filtered_messages(self._topic) as messages:
                    await client.subscribe(self._topic)
                    async for msg in messages:
                        try:
                            payload = json.loads(msg.payload)
                        except Exception:
                            payload = {"data": msg.payload.decode("utf-8", errors="replace")}
                        payload["_mqtt_topic"] = msg.topic
                        try:
                            self._queue.put_nowait(payload)
                        except asyncio.QueueFull:
                            pass
        except Exception as exc:
            logger.warning("MQTT listener stopped: %s", exc)
            self._ready = False

    async def read(self) -> Optional[Any]:
        await self._connect()
        if self._fallback:
            return await self._fallback.read()
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=0.1)
        except asyncio.TimeoutError:
            return None

    async def close(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass


class MQTTConsumerTransport(ConsumerTransport):
    """Publishes transformed payloads to an MQTT topic."""

    def __init__(self, endpoint: str) -> None:
        self._host, self._port, self._topic = _parse_mqtt_endpoint(endpoint)
        # Strip wildcard for publish topic
        self._pub_topic = self._topic.rstrip("/#") or "interface-core/output"
        self._client = None
        self._ready  = False

    async def _connect(self) -> bool:
        if self._ready:
            return True
        try:
            import asyncio_mqtt as aiomqtt
            self._client = aiomqtt.Client(self._host, self._port)
            self._ready  = True
            return True
        except ImportError:
            logger.warning("asyncio-mqtt not installed — MQTT output inactive.")
            return False
        except Exception as exc:
            logger.warning("MQTT consumer connect failed: %s", exc)
            return False

    async def write(self, payload: Any) -> bool:
        if not await self._connect():
            return False
        try:
            value = json.dumps(payload) if isinstance(payload, dict) else str(payload)
            async with self._client as client:
                await client.publish(self._pub_topic, value)
            return True
        except Exception as exc:
            logger.debug("MQTT write error: %s", exc)
            self._ready = False
            return False


# ─────────────────────────────────────────────
# Raw TCP Socket
# ─────────────────────────────────────────────

class SocketProducerTransport(ProducerTransport):
    """
    Reads newline-delimited JSON from a persistent TCP socket.
    Reconnects automatically on drop.
    endpoint format: 'host:port'
    """

    def __init__(self, endpoint: str) -> None:
        parts = endpoint.split(":")
        self._host   = parts[0]
        self._port   = int(parts[1]) if len(parts) > 1 else 9000
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None

    async def _connect(self) -> bool:
        if self._reader:
            return True
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self._host, self._port), timeout=3.0
            )
            logger.info("SocketProducerTransport connected: %s:%d", self._host, self._port)
            return True
        except Exception as exc:
            logger.debug("Socket connect failed %s:%d: %s", self._host, self._port, exc)
            self._reader = self._writer = None
            return False

    async def read(self) -> Optional[Any]:
        if not await self._connect():
            await asyncio.sleep(1.0)
            return None
        try:
            line = await asyncio.wait_for(self._reader.readline(), timeout=0.5)
            if not line:
                self._reader = self._writer = None
                return None
            try:
                return json.loads(line)
            except Exception:
                return {"line": line.decode("utf-8", errors="replace").strip()}
        except asyncio.TimeoutError:
            return None
        except Exception as exc:
            logger.debug("Socket read error: %s", exc)
            self._reader = self._writer = None
            return None

    async def close(self) -> None:
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
        self._reader = self._writer = None


class SocketConsumerTransport(ConsumerTransport):
    """
    Writes newline-delimited JSON to a TCP socket.
    endpoint format: 'host:port'
    """

    def __init__(self, endpoint: str) -> None:
        parts = endpoint.split(":")
        self._host   = parts[0]
        self._port   = int(parts[1]) if len(parts) > 1 else 9001
        self._writer: Optional[asyncio.StreamWriter] = None

    async def _connect(self) -> bool:
        if self._writer:
            return True
        try:
            _, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self._host, self._port), timeout=3.0
            )
            return True
        except Exception as exc:
            logger.debug("SocketConsumer connect failed: %s", exc)
            self._writer = None
            return False

    async def write(self, payload: Any) -> bool:
        if not await self._connect():
            return False
        try:
            line = json.dumps(payload) + "\n"
            self._writer.write(line.encode())
            await self._writer.drain()
            return True
        except Exception as exc:
            logger.debug("Socket write error: %s", exc)
            self._writer = None
            return False

    async def close(self) -> None:
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
            self._writer = None


# ─────────────────────────────────────────────
# WebSocket
# ─────────────────────────────────────────────

class WebSocketProducerTransport(ProducerTransport):
    """
    Reads messages from a WebSocket endpoint.
    endpoint format: 'ws://host:port/path'
    Requires: websockets (pip install websockets)
    Falls back to MemoryProducerTransport if absent.
    """

    def __init__(self, endpoint: str) -> None:
        self._endpoint  = endpoint if endpoint.startswith("ws") else f"ws://{endpoint}"
        self._ws        = None
        self._ready     = False
        self._fallback  = None
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=2048)
        self._listen_task = None

    async def _connect(self) -> bool:
        if self._ready:
            return True
        if self._fallback:
            return False
        try:
            import websockets  # type: ignore
            self._ws = await websockets.connect(self._endpoint)
            self._listen_task = asyncio.create_task(self._listen())
            self._ready = True
            logger.info("WebSocketProducerTransport connected: %s", self._endpoint)
            return True
        except ImportError:
            logger.warning(
                "websockets not installed — WebSocket transport inactive. "
                "Install with: pip install websockets"
            )
            from .runner import MemoryProducerTransport
            self._fallback = MemoryProducerTransport()
            return False
        except Exception as exc:
            logger.debug("WebSocket connect failed %s: %s", self._endpoint, exc)
            return False

    async def _listen(self) -> None:
        try:
            async for message in self._ws:
                try:
                    payload = json.loads(message) if isinstance(message, str) else {"data": message}
                except Exception:
                    payload = {"data": str(message)}
                try:
                    self._queue.put_nowait(payload)
                except asyncio.QueueFull:
                    pass
        except Exception as exc:
            logger.debug("WebSocket listener closed: %s", exc)
            self._ready = False

    async def read(self) -> Optional[Any]:
        ok = await self._connect()
        if not ok:
            return await self._fallback.read() if self._fallback else None
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=0.1)
        except asyncio.TimeoutError:
            return None

    async def close(self) -> None:
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws    = None
            self._ready = False


class WebSocketConsumerTransport(ConsumerTransport):
    """
    Sends payloads to a WebSocket endpoint.
    endpoint format: 'ws://host:port/path'
    """

    def __init__(self, endpoint: str) -> None:
        self._endpoint = endpoint if endpoint.startswith("ws") else f"ws://{endpoint}"
        self._ws       = None
        self._ready    = False

    async def _connect(self) -> bool:
        if self._ready:
            return True
        try:
            import websockets
            self._ws    = await websockets.connect(self._endpoint)
            self._ready = True
            return True
        except ImportError:
            logger.warning("websockets not installed — WebSocket output inactive.")
            return False
        except Exception as exc:
            logger.debug("WebSocket consumer connect failed: %s", exc)
            return False

    async def write(self, payload: Any) -> bool:
        if not await self._connect():
            return False
        try:
            msg = json.dumps(payload) if isinstance(payload, dict) else str(payload)
            await self._ws.send(msg)
            return True
        except Exception as exc:
            logger.debug("WebSocket write error: %s", exc)
            self._ready = False
            return False

    async def close(self) -> None:
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws    = None
            self._ready = False


# ─────────────────────────────────────────────
# AMQP (via aio-pika / RabbitMQ)
# ─────────────────────────────────────────────

class AMQPProducerTransport(ProducerTransport):
    """
    Consumes messages from an AMQP exchange/queue (RabbitMQ, etc.).
    endpoint format: 'amqp://user:pass@host:port/vhost'  or  'host:port/queue_name'
    Requires: aio-pika (pip install aio-pika)
    """

    def __init__(self, endpoint: str) -> None:
        if endpoint.startswith("amqp"):
            self._url       = endpoint
            self._queue_name = "interface-core"
        else:
            parts            = endpoint.split("/")
            self._url        = f"amqp://guest:guest@{parts[0]}/"
            self._queue_name = parts[1] if len(parts) > 1 else "interface-core"
        self._connection = None
        self._channel    = None
        self._queue_obj  = None
        self._ready      = False
        self._fallback   = None
        self._msg_queue: asyncio.Queue = asyncio.Queue(maxsize=2048)

    async def _connect(self) -> bool:
        if self._ready:
            return True
        if self._fallback:
            return False
        try:
            import aio_pika  # type: ignore
            self._connection = await aio_pika.connect_robust(self._url)
            self._channel    = await self._connection.channel()
            self._queue_obj  = await self._channel.declare_queue(
                self._queue_name, durable=True
            )
            await self._queue_obj.consume(self._on_message)
            self._ready = True
            logger.info("AMQPProducerTransport connected: %s/%s", self._url, self._queue_name)
            return True
        except ImportError:
            logger.warning(
                "aio-pika not installed — AMQP transport inactive. "
                "Install with: pip install aio-pika"
            )
            from .runner import MemoryProducerTransport
            self._fallback = MemoryProducerTransport()
            return False
        except Exception as exc:
            logger.debug("AMQP connect failed: %s", exc)
            return False

    async def _on_message(self, message: Any) -> None:
        try:
            import aio_pika
            async with message.process():
                try:
                    payload = json.loads(message.body)
                except Exception:
                    payload = {"data": message.body.decode("utf-8", errors="replace")}
                try:
                    self._msg_queue.put_nowait(payload)
                except asyncio.QueueFull:
                    pass
        except Exception as exc:
            logger.debug("AMQP message processing error: %s", exc)

    async def read(self) -> Optional[Any]:
        ok = await self._connect()
        if not ok:
            return await self._fallback.read() if self._fallback else None
        try:
            return await asyncio.wait_for(self._msg_queue.get(), timeout=0.1)
        except asyncio.TimeoutError:
            return None

    async def close(self) -> None:
        try:
            if self._connection:
                await self._connection.close()
        except Exception:
            pass
        self._connection = self._channel = self._queue_obj = None
        self._ready = False


class AMQPConsumerTransport(ConsumerTransport):
    """
    Publishes payloads to an AMQP exchange.
    endpoint format: 'amqp://user:pass@host:port/vhost' or 'host:port/routing_key'
    """

    def __init__(self, endpoint: str) -> None:
        if endpoint.startswith("amqp"):
            self._url         = endpoint
            self._routing_key = "interface-core"
        else:
            parts              = endpoint.split("/")
            self._url          = f"amqp://guest:guest@{parts[0]}/"
            self._routing_key  = parts[1] if len(parts) > 1 else "interface-core"
        self._connection = None
        self._channel    = None
        self._ready      = False

    async def _connect(self) -> bool:
        if self._ready:
            return True
        try:
            import aio_pika
            self._connection = await aio_pika.connect_robust(self._url)
            self._channel    = await self._connection.channel()
            self._ready      = True
            return True
        except ImportError:
            logger.warning("aio-pika not installed — AMQP output inactive.")
            return False
        except Exception as exc:
            logger.debug("AMQP consumer connect failed: %s", exc)
            return False

    async def write(self, payload: Any) -> bool:
        if not await self._connect():
            return False
        try:
            import aio_pika
            body    = json.dumps(payload).encode() if isinstance(payload, dict) else payload
            message = aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
            await self._channel.default_exchange.publish(
                message, routing_key=self._routing_key
            )
            return True
        except Exception as exc:
            logger.debug("AMQP write error: %s", exc)
            self._ready = False
            return False

    async def close(self) -> None:
        try:
            if self._connection:
                await self._connection.close()
        except Exception:
            pass
        self._connection = self._channel = None
        self._ready = False


# ─────────────────────────────────────────────
# ZeroMQ
# ─────────────────────────────────────────────

class ZMQProducerTransport(ProducerTransport):
    """
    Subscribes to a ZeroMQ PUB socket and yields messages as dicts.

    Supports three ZMQ patterns:
      - SUB  (subscribe to a PUB socket)  — default
      - PULL (receive from PUSH socket)
      - PAIR (bidirectional peer)

    endpoint format:
      'tcp://host:port'           — connects SUB to remote PUB
      'tcp://host:port?mode=pull' — connects PULL to remote PUSH
      'tcp://*:port'              — binds PUB locally (use for process-local)

    Requires: pyzmq (pip install pyzmq)
    Falls back to MemoryProducerTransport when absent.
    """

    def __init__(self, endpoint: str, topic: bytes = b"") -> None:
        self._raw      = endpoint
        self._topic    = topic
        self._ctx      = None
        self._sock     = None
        self._ready    = False
        self._fallback = None
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=4096)
        self._listen_task = None

        # Parse mode from query string
        self._mode = "sub"
        if "?mode=" in endpoint:
            self._raw, qs = endpoint.split("?", 1)
            for part in qs.split("&"):
                if part.startswith("mode="):
                    self._mode = part[5:].lower()

    async def _connect(self) -> bool:
        if self._ready:
            return True
        if self._fallback:
            return False
        try:
            import zmq
            import zmq.asyncio as azmq
            self._ctx  = azmq.Context.instance()
            sock_type  = {"sub": zmq.SUB, "pull": zmq.PULL, "pair": zmq.PAIR}.get(
                self._mode, zmq.SUB
            )
            self._sock = self._ctx.socket(sock_type)
            if sock_type == zmq.SUB:
                self._sock.setsockopt(zmq.SUBSCRIBE, self._topic)
            if self._raw.startswith("tcp://*"):
                self._sock.bind(self._raw)
            else:
                self._sock.connect(self._raw)
            self._listen_task = asyncio.create_task(self._listen(), name=f"zmq-sub-{self._raw}")
            self._ready = True
            logger.info("ZMQProducerTransport connected [%s] %s", self._mode, self._raw)
            return True
        except ImportError:
            logger.warning(
                "pyzmq not installed — ZeroMQ transport inactive. "
                "Install with: pip install pyzmq"
            )
            from .runner import MemoryProducerTransport
            self._fallback = MemoryProducerTransport()
            return False
        except Exception as exc:
            logger.debug("ZMQ connect failed %s: %s", self._raw, exc)
            return False

    async def _listen(self) -> None:
        try:
            while self._ready:
                try:
                    raw = await asyncio.wait_for(self._sock.recv(), timeout=0.1)
                    # Strip topic prefix for SUB sockets
                    if self._mode == "sub" and self._topic:
                        raw = raw[len(self._topic):]
                    try:
                        payload = json.loads(raw)
                    except Exception:
                        payload = {"data": raw.hex() if isinstance(raw, bytes) else str(raw)}
                    try:
                        self._queue.put_nowait(payload)
                    except asyncio.QueueFull:
                        pass
                except asyncio.TimeoutError:
                    continue
        except Exception as exc:
            logger.debug("ZMQ listener closed: %s", exc)
            self._ready = False

    async def read(self) -> Optional[Any]:
        ok = await self._connect()
        if not ok:
            return await self._fallback.read() if self._fallback else None
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=0.1)
        except asyncio.TimeoutError:
            return None

    async def close(self) -> None:
        self._ready = False
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None


class ZMQConsumerTransport(ConsumerTransport):
    """
    Publishes payloads to a ZeroMQ endpoint.

    Supports:
      - PUB  (publish to subscribers)  — default
      - PUSH (push to PULL workers)
      - PAIR (bidirectional peer)

    endpoint format:
      'tcp://host:port'             — connects PUB to remote SUB
      'tcp://*:port'                — binds PUB locally
      'tcp://host:port?mode=push'   — connects PUSH to PULL
    """

    def __init__(self, endpoint: str, topic: bytes = b"") -> None:
        self._raw   = endpoint
        self._topic = topic
        self._ctx   = None
        self._sock  = None
        self._ready = False
        self._mode  = "pub"
        if "?mode=" in endpoint:
            self._raw, qs = endpoint.split("?", 1)
            for part in qs.split("&"):
                if part.startswith("mode="):
                    self._mode = part[5:].lower()

    async def _connect(self) -> bool:
        if self._ready:
            return True
        try:
            import zmq
            import zmq.asyncio as azmq
            self._ctx  = azmq.Context.instance()
            sock_type  = {"pub": zmq.PUB, "push": zmq.PUSH, "pair": zmq.PAIR}.get(
                self._mode, zmq.PUB
            )
            self._sock = self._ctx.socket(sock_type)
            if self._raw.startswith("tcp://*"):
                self._sock.bind(self._raw)
            else:
                self._sock.connect(self._raw)
            self._ready = True
            logger.info("ZMQConsumerTransport connected [%s] %s", self._mode, self._raw)
            return True
        except ImportError:
            logger.warning("pyzmq not installed — ZeroMQ output inactive. pip install pyzmq")
            return False
        except Exception as exc:
            logger.debug("ZMQ consumer connect failed: %s", exc)
            return False

    async def write(self, payload: Any) -> bool:
        if not await self._connect():
            return False
        try:
            body = json.dumps(payload).encode() if isinstance(payload, dict) else (
                payload if isinstance(payload, bytes) else str(payload).encode()
            )
            await self._sock.send(self._topic + body)
            return True
        except Exception as exc:
            logger.debug("ZMQ write error: %s", exc)
            self._ready = False
            return False

    async def close(self) -> None:
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock  = None
            self._ready = False
