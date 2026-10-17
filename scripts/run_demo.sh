#!/bin/sh
set -eu
EMBEDDER="${EMBEDDER:-fake}" LLM="${LLM:-mock}" uv run python -m agents.demo