import contextlib, hashlib, itertools, os, shutil, sqlite3, struct, subprocess, sys, tempfile
from pathlib import Path
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

# A browser is a READERS entry; an operating system is a row in these tables, never a branch in a reader.
PLATFORM=sys.platform
_CHROME_ROOTS={"darwin":{"Library/Application Support/Google/Chrome":"Chrome"},"linux":{".config/google-chrome":"chrome",".config/chromium":"chromium"}}
_CHROME_KEYRING={"darwin":(lambda app:["security","find-generic-password","-w","-a",app,"-s",f"{app} Safe Storage"],1003,b"v10"),"linux":(lambda app:["secret-tool","lookup","application",app],1,b"v11")}
_CHROME_FIXED={"linux":{b"v10":b"peanuts"}}  # Chromium's built-in password for cookies written without a keyring
_FIREFOX_ROOTS={"darwin":("Library/Application Support/Firefox/Profiles",),"linux":(".mozilla/firefox","snap/firefox/common/.mozilla/firefox",".var/app/org.mozilla.firefox/.mozilla/firefox")}

def _safari_records():
    if not (path:=next((p for p in (Path.home()/"Library/Containers/com.apple.Safari/Data/Library/Cookies/Cookies.binarycookies",Path.home()/"Library/Cookies/Cookies.binarycookies") if p.exists()),None)): return
    if (data:=path.read_bytes())[:4]!=b"cook": return
    sizes,pages=(sizes:=[struct.unpack(">I",data[8+i*4:12+i*4])[0] for i in range(struct.unpack(">I",data[4:8])[0])]),[data[start:start+size] for start,size in zip(itertools.accumulate([8+4*len(sizes),*sizes]),sizes)]
    yield from ((text(fields[0]),text(fields[1]),text(struct.unpack("<I",page[off+28:off+32])[0])) for page in pages if page[:4]==b"\0\0\1\0" for i in range(struct.unpack("<I",page[4:8])[0]) for off in [struct.unpack("<I",page[8+i*4:12+i*4])[0]] for fields,text in [(struct.unpack("<III",page[off+16:off+28]),lambda pos,page=page,off=off:page[off+pos:page.find(b"\0",off+pos)].decode(errors="ignore"))])
def read_safari_cookies(domain: str, profile: str | None = None) -> dict[str,str]:
    target=domain.lstrip(".").lower()
    try: return {name:value for host,name,value in _safari_records() or () if target in (clean:=host.lstrip(".").lower()) or clean in target or clean.endswith(target) or target.endswith(clean)}
    except PermissionError as e: raise ValueError("Safari cookies are not readable. Grant Full Disk Access to your terminal or use -b chrome.") from e
def safari_cookie_domains(): return {host for host,_,_ in _safari_records() or ()}

def _chrome_roots(): return [(Path.home()/root,app) for root,app in _CHROME_ROOTS.get(PLATFORM,{}).items()]
def _chrome_path(profile=None): return (lambda name:next(((path,app) for root,app in _chrome_roots() for path in (root/name/"Cookies",root/name/"Network/Cookies") if path.exists()),None))(profile or os.environ.get("CONVOS_CHROME_PROFILE","Default"))
def _keyring(command):
    with contextlib.suppress(OSError,subprocess.SubprocessError):
        if not (result:=subprocess.run(command,capture_output=True,text=True,timeout=10)).returncode and result.stdout.strip(): return result.stdout.strip().encode()
def _chrome_keys(app):
    command,rounds,tag=_CHROME_KEYRING[PLATFORM]
    secrets={**_CHROME_FIXED.get(PLATFORM,{}),**({tag:secret} if (secret:=_keyring(command(app))) else {})}
    return {prefix:hashlib.pbkdf2_hmac("sha1",secret,b"saltysalt",rounds,16) for prefix,secret in secrets.items()}

def read_chrome_cookies(domain: str, profile: str | None = None) -> dict[str, str]:
    if not (found:=_chrome_path(profile)) or not (keys:=_chrome_keys(found[1])): return {}
    with contextlib.closing(sqlite3.connect(f"file:{found[0]}?mode=ro&nolock=1",uri=True)) as db: return {name:(value[32:] if not value[:32].isascii() else value).decode(errors="ignore") for name,encrypted,host in db.execute("SELECT name,encrypted_value,host_key FROM cookies WHERE host_key LIKE ?",(f"%{domain}%",)) if (key:=keys.get(encrypted[:3])) for decryptor in [Cipher(algorithms.AES(key),modes.CBC(b" "*16)).decryptor()] for decrypted in [decryptor.update(encrypted[3:])+decryptor.finalize()] for value in [decrypted[:-decrypted[-1]]]}
def chrome_cookie_domains(profile: str | None = None):
    if not (found:=_chrome_path(profile)): return set()
    with contextlib.closing(sqlite3.connect(f"file:{found[0]}?mode=ro&nolock=1",uri=True)) as db: return {r[0] for r in db.execute("SELECT DISTINCT host_key FROM cookies")}
def chrome_profiles() -> list[str]: return list(dict.fromkeys(p.name for root,_ in _chrome_roots() if root.exists() for p in root.iterdir() if p.is_dir() and ((p/"Cookies").exists() or (p/"Network/Cookies").exists())))

def _firefox_paths(profile=None): return (lambda name:[path for root in _FIREFOX_ROOTS.get(PLATFORM,()) if (base:=Path.home()/root).is_dir() for folder in ([base/name] if name else base.iterdir()) if (path:=folder/"cookies.sqlite").exists()])(profile or os.environ.get("CONVOS_FIREFOX_PROFILE"))
def _firefox_rows(sql, args=(), profile=None):
    if not (path:=max(_firefox_paths(profile),key=lambda p:p.stat().st_mtime,default=None)): return []  # without a named profile, the one Firefox wrote last
    with tempfile.TemporaryDirectory() as tmp:  # Firefox locks the live file and keeps recent writes in the WAL, so read a copy of both
        [shutil.copy2(src,Path(tmp)/src.name) for src in (path,path.with_name(f"{path.name}-wal")) if src.exists()]
        with contextlib.closing(sqlite3.connect(Path(tmp)/path.name)) as db: return db.execute(sql,args).fetchall()
def read_firefox_cookies(domain: str, profile: str | None = None) -> dict[str, str]: return dict(_firefox_rows("SELECT name,value FROM moz_cookies WHERE host LIKE ? ORDER BY lastAccessed",(f"%{domain}%",),profile))
def firefox_cookie_domains(profile: str | None = None): return {host for host, in _firefox_rows("SELECT DISTINCT host FROM moz_cookies",(),profile)}

READERS={"safari":read_safari_cookies,"chrome":read_chrome_cookies,"firefox":read_firefox_cookies}
def get_cookies(domain: str, browser: str = "safari", profile: str | None = None) -> dict[str, str]: return READERS[browser](domain, profile)
def get_cookies_any(domains: list[str], browser: str = "safari", profile: str | None = None) -> dict[str, str]: return {name:value for domain in domains for name,value in get_cookies(domain,browser,profile=profile).items()}
