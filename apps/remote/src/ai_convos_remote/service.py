"""Install a persistent per-user sync worker and remove obsolete wake hooks."""
import json, os, pathlib, plistlib, shutil, subprocess, sys, time

def _run(*command,check=False): return subprocess.run(command,capture_output=not check,check=check)
def launch(plist,label):
    _run("launchctl","bootout",f"gui/{os.getuid()}/{label}")
    if any(not _run("launchctl","bootstrap",f"gui/{os.getuid()}",str(plist)).returncode or time.sleep(.25) for _ in range(20)): return
    _run("launchctl","bootstrap",f"gui/{os.getuid()}",str(plist),check=True)
def edit_hooks(remove=False,root=None):
    configs=(pathlib.Path(os.environ.get("CLAUDE_CONFIG_DIR",pathlib.Path.home()/".claude"))/"settings.json",pathlib.Path(os.environ.get("CODEX_HOME",pathlib.Path.home()/".codex"))/"hooks.json")
    for path in configs:
        if not path.exists(): continue
        data=json.loads(path.read_text())
        data["hooks"]={name:kept for name,groups in data.get("hooks",{}).items() if (kept:=[clean for group in groups if (clean:={**group,"hooks":[h for h in group.get("hooks",[]) if not h.get("command","").endswith("convos remote hook")]})["hooks"]])}
        path.write_text(json.dumps(data))
def enable(data_dir,remove=False):
    data_dir=pathlib.Path(data_dir).resolve()
    edit_hooks()
    data_dir.mkdir(parents=True,exist_ok=True)
    root,label=str(data_dir.parent),"com.ai-convos.remote"
    if sys.platform!="darwin":
        unit=pathlib.Path.home()/".config/systemd/user/convos-remote.service"
        _run("systemctl","--user","disable","--now","convos-remote.service")
        if remove:
            unit.unlink(missing_ok=True)
            _run("systemctl","--user","daemon-reload",check=True)
            return "Remote background sync removed"
        unit.parent.mkdir(parents=True,exist_ok=True)
        unit.write_text(f"[Unit]\nDescription=Convos encrypted synchronization\n[Service]\nEnvironment={json.dumps('CONVOS_PROJECT_ROOT='+root.replace('%','%%'))}\nExecStart={json.dumps((shutil.which('convos') or 'convos').replace('%','%%'))} remote watch --interval 2\nRestart=always\n[Install]\nWantedBy=default.target\n")
        [_run(*command,check=True) for command in (("systemctl","--user","daemon-reload"),("systemctl","--user","enable","--now","convos-remote.service"))]
        return "Remote background sync enabled"
    plist=pathlib.Path.home()/"Library/LaunchAgents/com.ai-convos.remote.plist"
    if remove:
        _run("launchctl","bootout",f"gui/{os.getuid()}/{label}")
        plist.unlink(missing_ok=True)
        return "Remote background sync removed"
    plist.parent.mkdir(parents=True,exist_ok=True)
    plist.write_bytes(plistlib.dumps({"Label":label,"ProgramArguments":[shutil.which("convos") or "convos","remote","watch","--interval","2"],"EnvironmentVariables":{"CONVOS_PROJECT_ROOT":root},"KeepAlive":True,"RunAtLoad":True,"StandardOutPath":str(data_dir/"worker.log"),"StandardErrorPath":str(data_dir/"worker.log")}))
    launch(plist,label)
    return "Remote background sync enabled"
