"""Create a development tenant through Mnemo's authenticated admin API."""

import json
import os
import httpx


def main() -> None:
    base_url = os.getenv("MNEMO_URL", "http://127.0.0.1:8000")
    admin_key = os.getenv("MNEMO_ADMIN_KEY", "local-admin-change-me")
    response = httpx.post(f"{base_url}/admin/tenants", headers={"X-Admin-Key": admin_key}, json={"name": os.getenv("MNEMO_SEED_TENANT", "Acme"), "agent_id": "triage"}, timeout=15)
    response.raise_for_status()
    print(json.dumps(response.json(), indent=2))


if __name__ == "__main__":
    main()