#!/usr/bin/env bash
set -euo pipefail

[ "$#" -eq 1 ] || { echo "usage: $0 WHEEL" >&2; exit 2; }
wheel="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
[ -f "$wheel" ] || { echo "missing wheel: $wheel" >&2; exit 2; }
sandbox="$(mktemp -d)"
trap 'rm -rf "$sandbox"' EXIT

uv venv --python 3.12 "$sandbox/venv"
uv pip install --python "$sandbox/venv/bin/python" "$wheel"
export CONVOS_PROJECT_ROOT="$sandbox/root" CODEX_HOME="$sandbox/codex" CLAUDE_CONFIG_DIR="$sandbox/claude"
"$sandbox/venv/bin/convos" init

"$sandbox/venv/bin/python" <<'PY'
from importlib.metadata import metadata, version
from pathlib import Path
import duckdb, huggingface_hub, llama_cpp
from ai_convos import cli

requirements = metadata("convos").get_all("Requires-Dist") or []
assert version("convos") == "0.9.2"
assert any(r.startswith("llama-cpp-python") for r in requirements), requirements
assert any(r.startswith("huggingface-hub") for r in requirements), requirements
conn = duckdb.connect(str(cli.DB_PATH))
vector = [0.0] * 768; vector[0] = 1.0
conn.execute("INSERT INTO conversations (id,source,title) VALUES ('smoke-conversation','codex','Packaging decision')")
conn.execute("INSERT INTO messages (id,conversation_id,role,content,embedding) VALUES ('smoke-message','smoke-conversation','assistant','We kept semantic recall in the default install.',?)", [vector])
cli.rebuild_fts_index(conn); conn.close(); cli.embed_text = lambda *args, **kwargs: vector
hits = cli.hybrid_hits("why is semantic recall included?", limit=1)
assert hits[0]["conversation_id"] == "smoke-conversation", hits
assert Path(cli.PROJECT_ROOT).exists()
print("clean wheel: convos 0.9.2, semantic dependencies and hybrid retrieval ready")
PY
