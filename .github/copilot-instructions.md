# Project notes

- Run tests with `EMBEDDER=fake LLM=mock uv run --extra dev pytest`.
- The MCP stdio server is implemented with the official Python SDK (`mcp`): https://github.com/modelcontextprotocol/python-sdk and https://py.sdk.modelcontextprotocol.io/.
- Tenant identity is derived only from `X-API-Key`; preserve tenant filters in every repository query.
