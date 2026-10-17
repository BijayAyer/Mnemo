"""Measure lexical memory retrieval and temporal fact supersession."""

import asyncio
import json
import os
import tempfile
from pathlib import Path

os.environ.setdefault("EMBEDDER", "fake")
os.environ.setdefault("LLM", "mock")

import httpx
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from mnemo.config import settings
from mnemo.main import create_app

ROOT = Path(__file__).resolve().parent / "results"


async def run() -> dict:
    ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="mnemo-recall-") as temp:
        object.__setattr__(settings, "data_dir", Path(temp) / "tenant-indexes")
        app = create_app(Path(temp) / "recall.sqlite3")
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://bench") as client:
                tenant = (await client.post("/admin/tenants", headers={"X-Admin-Key": settings.admin_key}, json={"name": "Recall"})).json()
                headers = {"X-API-Key": tenant["api_key"]}
                targets = []
                for session in range(50):
                    for index in range(4):
                        marker = f"mnemo-needle-s{session:02d}-item{index:02d}"
                        body = {"user_id": "alice", "subject": "alice", "predicate": f"session-{session}-topic-{index}", "object": f"Remember marker {marker}"}
                        response = await client.post("/v1/facts", headers=headers, json=body)
                        response.raise_for_status()
                        if session == 0:
                            targets.append(marker)

                hits1 = hits5 = 0
                for marker in targets:
                    result = await client.post("/v1/facts/search", headers=headers, json={"user_id": "alice", "query": marker, "limit": 5})
                    ranked = result.json()
                    target_rank = next((index for index, fact in enumerate(ranked) if marker in fact["object"]), None)
                    hits1 += target_rank == 0
                    hits5 += target_rank is not None and target_rank < 5

                old = (await client.post("/v1/facts", headers=headers, json={"user_id": "alice", "subject": "alice", "predicate": "subscription", "object": "Basic"})).json()
                await client.post("/v1/facts", headers=headers, json={"user_id": "alice", "subject": "alice", "predicate": "subscription", "object": "Premium"})
                as_of = await client.get("/v1/facts", headers=headers, params={"user_id": "alice", "as_of": old["valid_from"]})
                current = await client.get("/v1/facts", headers=headers, params={"user_id": "alice"})
                old_correct = any(row["predicate"] == "subscription" and row["object"] == "Basic" for row in as_of.json())
                new_correct = any(row["predicate"] == "subscription" and row["object"] == "Premium" for row in current.json())
                report = {"facts": 202, "sessions": 50, "queries": len(targets), "recall_at_1": hits1 / len(targets), "recall_at_5": hits5 / len(targets), "supersession_correct": int(old_correct and new_correct), "supersession_trials": 1}
                (ROOT / "recall.json").write_text(json.dumps(report, indent=2) + "\n")
                figure, axis = plt.subplots(figsize=(7, 4.5))
                labels = ["Recall@1", "Recall@5", "Supersession"]
                axis.bar(labels, [report["recall_at_1"], report["recall_at_5"], report["supersession_correct"]], color=["#75c69a", "#e7bd68", "#dd7868"])
                axis.set_ylim(0, 1.05)
                axis.set_ylabel("Rate")
                axis.set_title("Mnemo recall and temporal correctness")
                figure.tight_layout()
                figure.savefig(ROOT / "recall.png", dpi=140)
                plt.close(figure)
                return report


if __name__ == "__main__":
    print(json.dumps(asyncio.run(run()), indent=2))