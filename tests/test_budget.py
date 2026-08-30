"""Tests for repository constraints."""

import ast, re, token, tokenize, tomllib
from pathlib import Path

TOKEN_WHITELIST = {token.OP, token.NAME, token.NUMBER, token.STRING}


def _loc(paths):
    return sum(len({t.start[0] for t in tokenize.generate_tokens(p.read_text().splitlines(True).__iter__().__next__)
                    if t.type in TOKEN_WHITELIST}) for p in paths)


def test_line_budget():
    """Keep the cohesive archive-writing core, including durable migrations and evidence classification, under its explicit 1300-line budget."""
    root = Path(__file__).resolve().parents[1]
    paths = sorted((root / "src" / "ai_convos").glob("*.py"))
    assert paths, "No source files found"
    loc = _loc(paths)
    assert loc < 1300, f"Code line budget exceeded: {loc} >= 1300"


def test_app_line_budgets():
    """Budget products honestly; remote includes signed alias reconciliation; never split products to evade limits."""
    root = Path(__file__).resolve().parents[1]
    for src in sorted((root / "apps").glob("*/src")):
        loc = _loc(sorted(src.rglob("*.py")))
        limit = {"changegraph": 400, "memory": 1000, "remote": 2200, "remote_server": 600}.get(src.parent.name, 200)
        assert loc < limit, f"App {src.parent.name} budget exceeded: {loc} >= {limit}"


def test_statement_packing_budget():
    """Dense expressions are welcome; statement separators are not."""
    root = Path(__file__).resolve().parents[1]
    paths = sorted([*(root/"src").rglob("*.py"), *(p for src in (root/"apps").glob("*/src") for p in src.rglob("*.py")), *(root/"scripts").rglob("*.py"), *(root/"evals").rglob("*.py")])
    separators = [(path, tok.start[0]) for path in paths for tok in tokenize.generate_tokens(path.read_text().splitlines(True).__iter__().__next__) if tok.type == token.OP and tok.string == ";"]
    assert not separators, [(str(path.relative_to(root)),line) for path,line in separators]


def test_remote_has_two_product_packages():
    root = Path(__file__).resolve().parents[1]
    assert {p.parent.name for p in (root / "apps").glob("remote*/pyproject.toml")} == {"remote", "remote_server"}


def test_changegraph_is_read_only():
    root = Path(__file__).resolve().parents[1]
    files = sorted((root/"apps/changegraph/src").rglob("*.py"))
    mutation = re.compile(r"^\s*(?:INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|REPLACE)\b", re.I)
    assert not {str(p.relative_to(root)):[n.value for n in ast.walk(ast.parse(p.read_text())) if isinstance(n,ast.Constant) and isinstance(n.value,str) and mutation.search(n.value)] for p in files if any(isinstance(n,ast.Constant) and isinstance(n.value,str) and mutation.search(n.value) for n in ast.walk(ast.parse(p.read_text())))}


def test_installable_product_versions_are_aligned():
    root = Path(__file__).resolve().parents[1]
    files = [root/"pyproject.toml", *sorted((root/"apps").glob("*/pyproject.toml"))]
    projects = {f.parent.name:tomllib.loads(f.read_text())["project"] for f in files}
    assert {p["name"] for p in projects.values()} == {"convos","convos-changegraph","convos-explore","convos-memory","convos-redact","convos-remote","convos-remote-server","convos-resume"}, projects
    assert {p["version"] for p in projects.values()} == {"0.10.1"}, projects
    major,minor=map(int,next(iter({p["version"] for p in projects.values()})).split(".")[:2])
    constrained=[d for p in projects.values() for d in [*p["dependencies"],*(d for ds in p.get("optional-dependencies",{}).values() for d in ds)] if d.startswith("convos") and ">=" in d]
    assert constrained and {d[d.index(">="):] for d in constrained} == {f">={major}.{minor},<{major}.{minor+1}"}, constrained
    assert {d for p in projects.values() for d in p["dependencies"] if d.startswith("duckdb")} == {"duckdb>=1.2.0"}
    assert not any(d.startswith("convos-changegraph") for d in projects["remote"]["dependencies"])


def test_default_semantic_runtime_is_macos_only():
    project=tomllib.loads((Path(__file__).resolve().parents[1]/"pyproject.toml").read_text())["project"]
    deps=set(project["dependencies"])
    assert "llama-cpp-python>=0.3.0; sys_platform == 'darwin'" in deps and "huggingface-hub>=0.20.0; sys_platform == 'darwin'" in deps and not any("model2vec" in dep or "sys_platform == 'linux'" in dep for dep in deps)


def test_linux_workflows_do_not_enable_optional_semantic_runtime():
    root=Path(__file__).resolve().parents[1]
    workflows=[(root/".github/workflows"/name).read_text() for name in ("tests.yml","release.yml")]
    assert all("uv sync --extra dev" in workflow and "uv sync --all-extras" not in workflow for workflow in workflows)


def test_release_has_one_trusted_publisher_per_public_product():
    workflow = (Path(__file__).resolve().parents[1]/".github/workflows/release.yml").read_text()
    public={"convos","convos-redact","convos-remote","convos-remote-server"}
    assert set(re.findall(r"packages-dir: dist/([^/]+)/",workflow)) == public
    assert set(re.findall(r"^\s+name: (pypi(?:-[a-z-]+)?)$",workflow,re.M)) == {"pypi","pypi-redact","pypi-remote","pypi-remote-server"}
    assert workflow.count("skip-existing: true") == len(public)
