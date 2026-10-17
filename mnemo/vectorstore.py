"""Per-tenant persisted vector index, using FAISS when available."""

import json
from pathlib import Path
from .config import settings


class TenantIndex:
    """Store vectors in distinct tenant directories and rebuild from SQLite rows."""

    def __init__(self, tenant_id: str):
        self.directory = settings.data_dir / tenant_id
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / "vectors.json"
        self.faiss_path = self.directory / "index.faiss"
        self.ids_path = self.directory / "ids.json"
        self.vectors = json.loads(self.path.read_text()) if self.path.exists() else {}

    def add(self, item_id: str, vector: list[float]) -> None:
        self.vectors[item_id] = vector
        self.path.write_text(json.dumps(self.vectors))
        self._persist_faiss()

    def _persist_faiss(self) -> None:
        try:
            import faiss
            import numpy as np
        except ImportError:
            return
        if not self.vectors:
            return
        ids = list(self.vectors)
        matrix = np.asarray([self.vectors[item] for item in ids], dtype=np.float32)
        index = faiss.IndexFlatIP(matrix.shape[1])
        index.add(matrix)
        faiss.write_index(index, str(self.faiss_path))
        self.ids_path.write_text(json.dumps(ids))

    def search(self, vector: list[float], limit: int = 10) -> list[tuple[str, float]]:
        import numpy as np
        query = np.asarray(vector, dtype=np.float32)
        if not self.vectors:
            return []
        try:
            import faiss
            if not self.faiss_path.exists() or not self.ids_path.exists():
                self._persist_faiss()
            index = faiss.read_index(str(self.faiss_path))
            ids = json.loads(self.ids_path.read_text())
            scores, positions = index.search(query.reshape(1, -1), min(limit, len(ids)))
            return [(ids[pos], float(score)) for pos, score in zip(positions[0], scores[0]) if pos >= 0]
        except ImportError:
            return sorted(((key, float(np.dot(query, value))) for key, value in self.vectors.items()), key=lambda pair: pair[1], reverse=True)[:limit]