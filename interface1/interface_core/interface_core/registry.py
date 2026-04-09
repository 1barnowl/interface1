"""
Interface Core — Registry & Capability Graph
Thread-safe node registry + weighted directed graph of producer/consumer pairs.
"""

from __future__ import annotations
import threading
import time
from typing import Dict, Iterator, List, Optional, Set, Tuple

from .models import Node, NodeRole, ContractSnapshot


# ─────────────────────────────────────────────
# Node Registry
# ─────────────────────────────────────────────

class NodeRegistry:
    """
    Concurrent HashMap<node_id, Node> with read-write lock striping.
    Discovery writes and matching reads never block each other beyond
    a single stripe's contention window.
    """

    STRIPES = 16

    def __init__(self) -> None:
        self._buckets: List[Dict[str, Node]] = [{} for _ in range(self.STRIPES)]
        self._locks:   List[threading.RLock] = [threading.RLock() for _ in range(self.STRIPES)]

    def _stripe(self, node_id: str) -> int:
        return hash(node_id) % self.STRIPES

    def put(self, node: Node) -> None:
        s = self._stripe(node.id)
        with self._locks[s]:
            self._buckets[s][node.id] = node

    def get(self, node_id: str) -> Optional[Node]:
        s = self._stripe(node_id)
        with self._locks[s]:
            return self._buckets[s].get(node_id)

    def remove(self, node_id: str) -> Optional[Node]:
        s = self._stripe(node_id)
        with self._locks[s]:
            return self._buckets[s].pop(node_id, None)

    def contains(self, node_id: str) -> bool:
        s = self._stripe(node_id)
        with self._locks[s]:
            return node_id in self._buckets[s]

    def all_nodes(self) -> List[Node]:
        nodes: List[Node] = []
        for i in range(self.STRIPES):
            with self._locks[i]:
                nodes.extend(self._buckets[i].values())
        return nodes

    def producers(self) -> List[Node]:
        return [n for n in self.all_nodes() if n.role in (NodeRole.PRODUCER, NodeRole.BOTH)]

    def consumers(self) -> List[Node]:
        return [n for n in self.all_nodes() if n.role in (NodeRole.CONSUMER, NodeRole.BOTH)]

    def touch(self, node_id: str) -> None:
        node = self.get(node_id)
        if node:
            node.last_seen = time.time()
            self.put(node)

    def __len__(self) -> int:
        return sum(len(b) for b in self._buckets)


# ─────────────────────────────────────────────
# Capability Graph
# ─────────────────────────────────────────────

class GraphEdge:
    """Directed edge from producer → consumer with a compatibility weight."""

    __slots__ = ("producer_id", "consumer_id", "weight", "updated_at")

    def __init__(self, producer_id: str, consumer_id: str, weight: float = 0.0):
        self.producer_id = producer_id
        self.consumer_id = consumer_id
        self.weight      = weight
        self.updated_at  = time.time()


class CapabilityGraph:
    """
    Weighted directed graph: Node → Set[Node] (adjacency list).
    Edges go from producers to compatible consumers.
    O(1) edge lookup via a (producer_id, consumer_id) index dict.
    """

    def __init__(self) -> None:
        # adjacency: producer_id → set of consumer_ids
        self._adj:    Dict[str, Set[str]]          = {}
        # edge index for O(1) lookup
        self._edges:  Dict[Tuple[str, str], GraphEdge] = {}
        # node set (all known node ids)
        self._nodes:  Set[str]                     = set()
        self._lock    = threading.RLock()

    def add_node(self, node_id: str) -> None:
        with self._lock:
            self._nodes.add(node_id)
            if node_id not in self._adj:
                self._adj[node_id] = set()

    def remove_node(self, node_id: str) -> None:
        with self._lock:
            self._nodes.discard(node_id)
            # remove outbound edges
            for consumer_id in list(self._adj.get(node_id, [])):
                self._edges.pop((node_id, consumer_id), None)
            self._adj.pop(node_id, None)
            # remove inbound edges
            for prod_id in list(self._nodes):
                if node_id in self._adj.get(prod_id, set()):
                    self._adj[prod_id].discard(node_id)
                    self._edges.pop((prod_id, node_id), None)

    def add_edge(self, producer_id: str, consumer_id: str, weight: float) -> None:
        with self._lock:
            self._nodes.update([producer_id, consumer_id])
            if producer_id not in self._adj:
                self._adj[producer_id] = set()
            self._adj[producer_id].add(consumer_id)
            self._edges[(producer_id, consumer_id)] = GraphEdge(producer_id, consumer_id, weight)

    def get_edge_weight(self, producer_id: str, consumer_id: str) -> float:
        edge = self._edges.get((producer_id, consumer_id))
        return edge.weight if edge else 0.0

    def reweight_edges(self, node_id: str, new_weight: Optional[float] = None) -> None:
        """Mark edges involving this node as stale (weight → 0) to force re-scoring."""
        with self._lock:
            for (pid, cid), edge in self._edges.items():
                if pid == node_id or cid == node_id:
                    edge.weight = new_weight if new_weight is not None else 0.0
                    edge.updated_at = time.time()

    def producer_consumer_pairs(self) -> Iterator[Tuple[str, str]]:
        """Yield all (producer_id, consumer_id) pairs currently in the graph."""
        with self._lock:
            for prod_id, consumers in list(self._adj.items()):
                for cons_id in list(consumers):
                    yield prod_id, cons_id

    def all_node_ids(self) -> Set[str]:
        with self._lock:
            return set(self._nodes)

    def neighbors(self, node_id: str) -> Set[str]:
        with self._lock:
            return set(self._adj.get(node_id, set()))


# ─────────────────────────────────────────────
# Drift Log
# ─────────────────────────────────────────────

class DriftLog:
    """
    In-memory circular time-series of ContractSnapshots per node.
    Backed by PostgreSQL for persistence (optional; handled by persistence layer).
    """

    MAX_PER_NODE = 256

    def __init__(self) -> None:
        self._series: Dict[str, List[ContractSnapshot]] = {}
        self._lock = threading.RLock()

    def record(self, snapshot: ContractSnapshot) -> None:
        with self._lock:
            series = self._series.setdefault(snapshot.node_id, [])
            series.append(snapshot)
            if len(series) > self.MAX_PER_NODE:
                series.pop(0)

    def latest(self, node_id: str) -> Optional[ContractSnapshot]:
        with self._lock:
            series = self._series.get(node_id, [])
            return series[-1] if series else None

    def history(self, node_id: str) -> List[ContractSnapshot]:
        with self._lock:
            return list(self._series.get(node_id, []))

    def drifted_nodes(self, threshold: float = 0.15) -> List[str]:
        with self._lock:
            result = []
            for node_id, series in self._series.items():
                if series and series[-1].delta >= threshold:
                    result.append(node_id)
            return result
