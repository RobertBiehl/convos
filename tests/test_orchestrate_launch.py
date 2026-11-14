from ai_convos import cli as core
from ai_convos_orchestrate import server

def test_first_launch_creates_orchestrate_root(tmp_path, monkeypatch):
    monkeypatch.setattr(core, "PROJECT_ROOT", tmp_path/"fresh")
    state = {"started": False}
    monkeypatch.setattr(server, "running", lambda: dict(port=1, token="t") if state["started"] else None)
    monkeypatch.setattr(server.subprocess, "Popen", lambda *a, **k: state.update(started=True))
    monkeypatch.setattr(server.webbrowser, "open", lambda url: None)
    assert server.launch() == "http://127.0.0.1:1/?t=t"
    assert (tmp_path/"fresh"/"orchestrate"/"server.log").is_file()
    assert (tmp_path/"fresh"/"orchestrate").stat().st_mode & 0o777 == 0o700
