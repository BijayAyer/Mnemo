if (-not $env:EMBEDDER) { $env:EMBEDDER = "fake" }
if (-not $env:LLM) { $env:LLM = "mock" }
uv run python -m agents.demo
exit $LASTEXITCODE