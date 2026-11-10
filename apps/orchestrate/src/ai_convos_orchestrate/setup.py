"""Explicit, reversible skill setup so any Claude Code or Codex session can join with /orchestrate or $orchestrate."""
import os
from pathlib import Path
from ai_convos import cli as core

def destinations():
    return [("claude-code", Path(os.environ.get("CLAUDE_CONFIG_DIR", Path.home()/".claude"))/"skills"/"orchestrate"/"SKILL.md"),
            ("codex", Path(os.environ.get("CODEX_HOME", Path.home()/".codex"))/"skills"/"orchestrate"/"SKILL.md")]

def install(remove=False):
    skill = Path(__file__).with_name("SKILL.md").read_text()
    for provider, path in destinations():
        if path.is_symlink() or path.parent.is_symlink(): raise ValueError(f"Refusing symlinked skill path: {path}")
        if path.exists() and "name: orchestrate\n" not in path.read_text(): raise ValueError(f"Unmanaged skill already exists: {path}")
    results = []
    for provider, path in destinations():
        if remove: path.unlink(missing_ok=True)
        else: (core.secure_dir(path.parent, 0o755), core.atomic_write(path, skill))
        results.append(dict(provider=provider, skill=str(path), installed=not remove))
    return results
