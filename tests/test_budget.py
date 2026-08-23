"""Tests for repository constraints."""

import ast, re, token, tokenize, tomllib
from collections import Counter
from pathlib import Path

TOKEN_WHITELIST = {token.OP, token.NAME, token.NUMBER, token.STRING}


def _loc(paths):
    return sum(len({t.start[0] for t in tokenize.generate_tokens(p.read_text().splitlines(True).__iter__().__next__)
                    if t.type in TOKEN_WHITELIST}) for p in paths)


def test_line_budget():
    """Keep the cohesive archive-writing core under its explicit 1200-line budget."""
    root = Path(__file__).resolve().parents[1]
    paths = sorted((root / "src" / "ai_convos").glob("*.py"))
    assert paths, "No source files found"
    loc = _loc(paths)
    assert loc < 1200, f"Code line budget exceeded: {loc} >= 1200"


def test_app_line_budgets():
    """Budget products honestly; never split one product into packages to evade its limit."""
    root = Path(__file__).resolve().parents[1]
    for src in sorted((root / "apps").glob("*/src")):
        loc = _loc(sorted(src.rglob("*.py")))
        limit = {"changegraph": 400, "memory": 650, "remote": 1100, "remote_server": 400}.get(src.parent.name, 200)
        assert loc < limit, f"App {src.parent.name} budget exceeded: {loc} >= {limit}"


def test_statement_packing_budget():
    """Dense expressions are welcome; packing unrelated statements behind separators is not."""
    root = Path(__file__).resolve().parents[1]
    paths = sorted([*(root/"src").rglob("*.py"), *(root/"apps").rglob("*.py"), *(root/"scripts").rglob("*.py"), *(root/"evals").rglob("*.py")])
    separators = [(path, tok.start[0]) for path in paths for tok in tokenize.generate_tokens(path.read_text().splitlines(True).__iter__().__next__) if tok.type == token.OP and tok.string == ";"]
    packed = {f"{path.relative_to(root)}:{line}":count for (path,line),count in Counter(separators).items()}
    assert len(separators) < 1875, f"Statement separator budget exceeded: {len(separators)} >= 1875"
    assert not {where:count for where,count in packed.items() if count > 10}, packed


def test_remote_has_two_product_packages():
    root = Path(__file__).resolve().parents[1]
    assert {p.parent.name for p in (root / "apps").glob("remote*/pyproject.toml")} == {"remote", "remote_server"}


def test_changegraph_is_read_only():
    root = Path(__file__).resolve().parents[1]; files = sorted((root/"apps/changegraph/src").rglob("*.py")); mutation = re.compile(r"^\s*(?:INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|REPLACE)\b", re.I)
    assert not {str(p.relative_to(root)):[n.value for n in ast.walk(ast.parse(p.read_text())) if isinstance(n,ast.Constant) and isinstance(n.value,str) and mutation.search(n.value)] for p in files if any(isinstance(n,ast.Constant) and isinstance(n.value,str) and mutation.search(n.value) for n in ast.walk(ast.parse(p.read_text())))}


def test_installable_product_versions_are_aligned():
    root = Path(__file__).resolve().parents[1]; files = [root/"pyproject.toml", *sorted((root/"apps").glob("*/pyproject.toml"))]
    projects = {f.parent.name:tomllib.loads(f.read_text())["project"] for f in files}
    assert {p["name"] for p in projects.values()} == {"convos","convos-changegraph","convos-explore","convos-memory","convos-redact","convos-remote","convos-remote-server","convos-resume"}, projects
    assert {p["version"] for p in projects.values()} == {"0.9.0"}, projects
    assert {d for p in projects.values() for d in p["dependencies"] if d.startswith("duckdb")} == {"duckdb>=1.2.0"}
    assert not any(d.startswith("convos-changegraph") for d in projects["remote"]["dependencies"])


def test_release_has_one_trusted_publisher_per_public_product():
    workflow = (Path(__file__).resolve().parents[1]/".github/workflows/release.yml").read_text(); public={"convos","convos-redact","convos-remote","convos-remote-server"}
    assert set(re.findall(r"packages-dir: dist/([^/]+)/",workflow)) == public
    assert set(re.findall(r"^\s+name: (pypi(?:-[a-z-]+)?)$",workflow,re.M)) == {"pypi","pypi-redact","pypi-remote","pypi-remote-server"}
    assert workflow.count("skip-existing: true") == len(public)
