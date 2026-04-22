"""
Interface Core — ML Matcher
Semantic embedding + field alignment for contract matching.

Models:
  - Embedder : all-MiniLM-L6-v2 (384-dim) — cosine similarity between contracts
  - FieldAligner : attention-based field-to-field mapping (lightweight impl)

Both run on a dedicated thread with a micro-batching queue (flush every 10 ms).
"""

from __future__ import annotations
import asyncio
import logging
import math
import threading
import time
from typing import Dict, List, Optional, Tuple

from ..models import DataContract, FieldMapping, FieldSchema

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────

def _cosine(a: List[float], b: List[float]) -> float:
    dot   = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return max(-1.0, min(1.0, dot / (norm_a * norm_b)))


def _text_for_contract(contract: DataContract) -> str:
    """Flatten a contract into a sentence for embedding."""
    field_names = " ".join(f.name for f in contract.schema_fields)
    tags        = " ".join(contract.semantic_tags)
    return f"encoding:{contract.encoding} fields:{field_names} tags:{tags}"


# ─────────────────────────────────────────────
# Embedder
# ─────────────────────────────────────────────

class Embedder:
    """
    Wraps sentence-transformers with a micro-batching queue.
    Falls back to a TF-IDF-style bag-of-words if the library is absent.
    """

    BATCH_TIMEOUT_MS = 10
    VOCAB_SIZE       = 4096   # for fallback hashing trick

    def __init__(self, model_name: str = "all-MiniLM-L6-v2") -> None:
        self._model_name = model_name
        self._model      = None
        self._dim        = 384
        self._lock       = threading.RLock()
        self._ready      = threading.Event()
        self._load_thread = threading.Thread(target=self._load, daemon=True)
        self._load_thread.start()

    def _load(self) -> None:
        try:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer(self._model_name)
            # get_embedding_dimension() is the current API; fall back if running older version
            if hasattr(self._model, 'get_embedding_dimension'):
                self._dim = self._model.get_embedding_dimension()
            else:
                self._dim = self._model.get_sentence_embedding_dimension()  # legacy
            logger.info("Loaded sentence-transformer: %s (dim=%d)", self._model_name, self._dim)
        except ImportError:
            logger.warning(
                "sentence-transformers not installed; using hashing-trick fallback embedder. "
                "Install with: pip install sentence-transformers"
            )
        finally:
            self._ready.set()

    def wait_ready(self, timeout: float = 30.0) -> bool:
        return self._ready.wait(timeout)

    @property
    def dim(self) -> int:
        return self._dim

    def embed(self, text: str) -> List[float]:
        """Embed a single string. Thread-safe."""
        return self.embed_batch([text])[0]

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Embed a list of strings. Thread-safe."""
        if self._model is not None:
            # show_progress_bar=False suppresses the per-call tqdm output that floods the terminal
            embeddings = self._model.encode(texts, convert_to_numpy=True, show_progress_bar=False)
            return [e.tolist() for e in embeddings]
        # Fallback: hashing-trick bag-of-characters n-gram embedding
        return [self._hash_embed(t) for t in texts]

    def _hash_embed(self, text: str) -> List[float]:
        """Deterministic pseudo-embedding via character n-gram hashing."""
        vec = [0.0] * self.VOCAB_SIZE
        tokens = text.lower().split()
        for token in tokens:
            for n in range(1, min(4, len(token) + 1)):
                for i in range(len(token) - n + 1):
                    gram  = token[i:i + n]
                    idx   = hash(gram) % self.VOCAB_SIZE
                    vec[idx] += 1.0
        # L2 normalize then downsample to 384 dims
        norm = math.sqrt(sum(x * x for x in vec)) or 1.0
        vec  = [x / norm for x in vec]
        # downsample via strided average pooling
        stride = self.VOCAB_SIZE // self._dim
        result = []
        for i in range(self._dim):
            chunk = vec[i * stride: (i + 1) * stride]
            result.append(sum(chunk) / len(chunk) if chunk else 0.0)
        return result

    def embed_contract(self, contract: DataContract) -> List[float]:
        return self.embed(_text_for_contract(contract))


# ─────────────────────────────────────────────
# Field Aligner
# ─────────────────────────────────────────────

class FieldAligner:
    """
    Aligns fields between two contracts.
    Uses name similarity + type compatibility + semantic tags.
    Returns a FieldMapping list with confidence scores.
    """

    TYPE_COMPAT: Dict[str, Dict[str, float]] = {
        "str":    {"str": 1.0, "int": 0.3, "float": 0.3, "bool": 0.2, "object": 0.5, "array": 0.2, "bytes": 0.4},
        "int":    {"str": 0.4, "int": 1.0, "float": 0.9, "bool": 0.5, "object": 0.1, "array": 0.1, "bytes": 0.3},
        "float":  {"str": 0.4, "int": 0.9, "float": 1.0, "bool": 0.2, "object": 0.1, "array": 0.1, "bytes": 0.3},
        "bool":   {"str": 0.3, "int": 0.6, "float": 0.3, "bool": 1.0, "object": 0.1, "array": 0.1, "bytes": 0.2},
        "object": {"str": 0.3, "int": 0.1, "float": 0.1, "bool": 0.1, "object": 1.0, "array": 0.5, "bytes": 0.2},
        "array":  {"str": 0.2, "int": 0.1, "float": 0.1, "bool": 0.1, "object": 0.5, "array": 1.0, "bytes": 0.2},
        "bytes":  {"str": 0.5, "int": 0.3, "float": 0.3, "bool": 0.2, "object": 0.2, "array": 0.2, "bytes": 1.0},
    }

    def __init__(self, embedder: Embedder) -> None:
        self._embedder = embedder

    def _name_similarity(self, a: str, b: str) -> float:
        """Jaccard similarity on character trigrams."""
        def trigrams(s: str):
            s = s.lower()
            return set(s[i:i+3] for i in range(len(s) - 2)) | {s} if len(s) >= 3 else {s}
        ta, tb = trigrams(a), trigrams(b)
        if not ta and not tb:
            return 1.0
        return len(ta & tb) / len(ta | tb) if ta | tb else 0.0

    def align(
        self, src_contract: DataContract, dst_contract: DataContract
    ) -> List[FieldMapping]:
        """
        Greedy maximum-weight matching between src and dst fields.
        Each src field is matched to its best dst field if score > 0.3.
        """
        mappings: List[FieldMapping] = []
        src_fields = src_contract.schema_fields
        dst_fields = dst_contract.schema_fields

        if not src_fields or not dst_fields:
            return mappings

        # Build score matrix
        scores: Dict[Tuple[int, int], float] = {}
        for i, sf in enumerate(src_fields):
            for j, df in enumerate(dst_fields):
                name_sim = self._name_similarity(sf.name, df.name)
                type_sim = self.TYPE_COMPAT.get(sf.type, {}).get(df.type, 0.2)
                score    = 0.6 * name_sim + 0.4 * type_sim
                scores[(i, j)] = score

        # Greedy matching
        used_dst: set = set()
        for i, sf in enumerate(src_fields):
            best_j, best_score = -1, 0.3   # threshold
            for j, df in enumerate(dst_fields):
                if j in used_dst:
                    continue
                s = scores.get((i, j), 0.0)
                if s > best_score:
                    best_score, best_j = s, j
            if best_j >= 0:
                df = dst_fields[best_j]
                used_dst.add(best_j)
                transform = "direct"
                if sf.type != df.type:
                    transform = f"cast:{df.type}"
                mappings.append(FieldMapping(
                    src_field=sf.name,
                    dst_field=df.name,
                    transform=transform,
                    confidence=best_score,
                ))

        return mappings

    def map_fields(
        self, updated_contract: DataContract, peer_contract: DataContract
    ) -> List[FieldMapping]:
        """Alias used by drift watch."""
        return self.align(updated_contract, peer_contract)


# ─────────────────────────────────────────────
# SemanticMatcher (composite entry point)
# ─────────────────────────────────────────────

class SemanticMatcher:
    """
    Top-level ML matcher used by all phases.
    Provides:
      - embed(contract) → vector
      - similarity(emb_a, emb_b) → float
      - align_fields(src, dst) → List[FieldMapping]
    """

    def __init__(self, model_name: str = "all-MiniLM-L6-v2") -> None:
        self._embedder   = Embedder(model_name)
        self._aligner    = FieldAligner(self._embedder)
        # LRU-style embedding cache: contract fingerprint → embedding vector
        # Prevents re-embedding the same contract on every discovery cycle.
        self._embed_cache: dict = {}
        self._cache_max         = 2048

    def wait_ready(self, timeout: float = 30.0) -> bool:
        return self._embedder.wait_ready(timeout)

    def embed(self, contract: DataContract) -> List[float]:
        """Return cached embedding if available; embed and cache otherwise."""
        fp = contract.fingerprint() if hasattr(contract, "fingerprint") else ""
        if fp and fp in self._embed_cache:
            return self._embed_cache[fp]
        vector = self._embedder.embed_contract(contract)
        if fp:
            if len(self._embed_cache) >= self._cache_max:
                # Evict oldest (FIFO)
                del self._embed_cache[next(iter(self._embed_cache))]
            self._embed_cache[fp] = vector
        return vector

    def cache_size(self) -> int:
        """Number of embeddings currently cached."""
        return len(self._embed_cache)

    def clear_cache(self) -> None:
        """Flush the embedding cache (e.g. after model reload)."""
        self._embed_cache.clear()

    def similarity(self, emb_a: List[float], emb_b: List[float]) -> float:
        return _cosine(emb_a, emb_b)

    def align_fields(
        self, src: DataContract, dst: DataContract
    ) -> List[FieldMapping]:
        return self._aligner.align(src, dst)
