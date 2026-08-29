#!/usr/bin/env bash
set -euo pipefail

[ "$#" -eq 1 ] || { echo "usage: $0 WHEEL" >&2; exit 2; }
wheel="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
[ -f "$wheel" ] || { echo "missing wheel: $wheel" >&2; exit 2; }
sandbox="$(mktemp -d)"
trap 'rm -rf "$sandbox"' EXIT

uv venv --python 3.12 "$sandbox/venv"
uv pip install --python "$sandbox/venv/bin/python" --only-binary :all: "$wheel"
export CONVOS_PROJECT_ROOT="$sandbox/root" CODEX_HOME="$sandbox/codex" CLAUDE_CONFIG_DIR="$sandbox/claude"
"$sandbox/venv/bin/convos" init

"$sandbox/venv/bin/python" <<'PY'
from importlib.metadata import metadata, version
from importlib.util import find_spec
from pathlib import Path
import duckdb
from ai_convos import cli
from typer.testing import CliRunner

requirements = metadata("convos").get_all("Requires-Dist") or []
assert version("convos") == "0.10.1"
assert not any("model2vec" in r or "sys_platform == \"linux\"" in r for r in requirements), requirements
assert find_spec("model2vec") is None and find_spec("llama_cpp") is None and not cli.semantic_enabled()
conn = duckdb.connect(str(cli.DB_PATH))
conn.execute("INSERT INTO conversations (id,source,title) VALUES ('smoke-conversation','codex','Packaging decision')")
conn.execute("INSERT INTO messages (id,conversation_id,role,content) VALUES ('smoke-message','smoke-conversation','assistant','The base installation needs no native compiler.')")
cli.rebuild_fts_index(conn); conn.close()
result = CliRunner().invoke(cli.app, ["search", "native compiler", "-n", "1"])
assert result.exit_code == 0 and "base installation needs no native compiler" in result.output, result.output
assert Path(cli.PROJECT_ROOT).exists()
print("clean wheel: convos 0.10.1, compiler-free Linux core and literal retrieval ready")
PY
