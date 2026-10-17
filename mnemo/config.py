"""Environment-backed application settings."""

from dataclasses import dataclass
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    database_path: Path = Path(os.getenv("MNEMO_DB", "data/mnemo.sqlite3"))
    data_dir: Path = Path(os.getenv("MNEMO_DATA_DIR", "data/tenants"))
    admin_key: str = os.getenv("MNEMO_ADMIN_KEY", "local-admin-change-me")
    embedder: str = os.getenv("EMBEDDER", "sentence-transformers")
    llm: str = os.getenv("LLM", "mock")
    llm_base_url: str = os.getenv("LLM_BASE_URL", "http://localhost:1234/v1")
    llm_model: str = os.getenv("LLM_MODEL", "local-model")
    consolidation_interval: int = int(os.getenv("CONSOLIDATION_INTERVAL", "0"))
    summary_threshold: int = int(os.getenv("SUMMARY_THRESHOLD", "20"))
    graph_backend: str = os.getenv("GRAPH_BACKEND", "none")
    falkordb_host: str = os.getenv("FALKORDB_HOST", "localhost")
    falkordb_port: int = int(os.getenv("FALKORDB_PORT", "6379"))


settings = Settings()