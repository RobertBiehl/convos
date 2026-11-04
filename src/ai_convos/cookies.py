import configparser, contextlib, hashlib, itertools, os, shutil, sqlite3, struct, subprocess, sys, tempfile
from pathlib import Path
from cryptography.hazmat.primitives import ciphers, padding

# A browser is a READERS entry; an operating system is a row in these tables, never a branch in a reader.
PLATFORM=sys.platform
_CHROME_ROOTS={"darwin":{"Library/Application Support/Google/Chrome":("Chrome","")},"linux":{"$XDG/google-chrome":("chrome",""),"$XDG/chromium":("chromium","chromium"),"snap/chromium/common/chromium":("chromium","snap-chromium"),".var/app/com.google.Chrome/config/google-chrome":("chrome","flatpak-chrome"),".var/app/org.chromium.Chromium/config/chromium":("chromium","flatpak-chromium")}}  # root: (keyring app, profile id prefix)
_CHROME_KEYRING={"darwin":(lambda app:["security","find-generic-password","-w","-a",app,"-s",f"{app} Safe Storage"],1003,b"v10"),"linux":(lambda app:["secret-tool","lookup","application",app],1,b"v11")}
_CHROME_FIXED={"linux":{b"v10":b"peanuts"}}  # Chromium's built-in password for cookies written without a keyring
_FIREFOX_ROOTS={"darwin":("Library/Application Support/Firefox",),"linux":(".mozilla/firefox","$XDG/mozilla/firefox","snap/firefox/common/.mozilla/firefox",".var/app/org.mozilla.firefox/.mozilla/firefox")}

def _safari_records():
    if not (path:=next((p for p in (Path.home()/"Library/Containers/com.apple.Safari/Data/Library/Cookies/Cookies.binarycookies",Path.home()/"Library/Cookies/Cookies.binarycookies") if p.exists()),None)): return
    if (data:=path.read_bytes())[:4]!=b"cook": return
    sizes,pages=(sizes:=[struct.unpack(">I",data[8+i*4:12+i*4])[0] for i in range(struct.unpack(">I",data[4:8])[0])]),[data[start:start+size] for start,size in zip(itertools.accumulate([8+4*len(sizes),*sizes]),sizes)]
    yield from ((text(fields[0]),text(fields[1]),text(struct.unpack("<I",page[off+28:off+32])[0])) for page in pages if page[:4]==b"\0\0\1\0" for i in range(struct.unpack("<I",page[4:8])[0]) for off in [struct.unpack("<I",page[8+i*4:12+i*4])[0]] for fields,text in [(struct.unpack("<III",page[off+16:off+28]),lambda pos,page=page,off=off:page[off+pos:page.find(b"\0",off+pos)].decode(errors="ignore"))])
def host_match(host: str, domain: str) -> bool: return (h:=host.lstrip(".").lower())==(d:=domain.lstrip(".").lower()) or h.endswith(f".{d}") or d.endswith(f".{h}")  # the site, its subdomains and its parents; never notclaude.ai for claude.ai

def read_safari_cookies(domain: str, profile: str | None = None) -> dict[str,str]:
    try: return {name:value for host,name,value in _safari_records() or () if host_match(host,domain)}
    except PermissionError as e: raise ValueError("Safari cookies are not readable. Grant Full Disk Access to your terminal or use -b chrome.") from e
def safari_cookie_domains(): return {host for host,_,_ in _safari_records() or ()}

def _root(root): return Path(os.environ.get("XDG_CONFIG_HOME") or Path.home()/".config")/root[5:] if root[:5]=="$XDG/" else Path.home()/root
def _chrome_roots(): return [(_root(root),app,tag) for root,(app,tag) in _CHROME_ROOTS.get(PLATFORM,{}).items()]
def _chrome_found(): return [(f"{tag}/{d.name}" if tag else d.name,path,app) for root,app,tag in _chrome_roots() if root.is_dir() for d in sorted(root.iterdir()) if d.is_dir() and (path:=next((p for p in (d/"Cookies",d/"Network/Cookies") if p.exists()),None))]
def _chrome_paths(profile=None): return (lambda want,found:[f for f in found if f[0]==want] or [f for f in found if f[0].rpartition("/")[2]==(want or "Default")])(profile or os.environ.get("CONVOS_CHROME_PROFILE"),_chrome_found())  # a bare name matches that profile in every install
def _keyring(command):
    with contextlib.suppress(OSError,subprocess.SubprocessError):
        if not (result:=subprocess.run(command,capture_output=True,text=True,timeout=10)).returncode and result.stdout.strip(): return result.stdout.strip().encode()
def _decrypt(key,encrypted,host,hashed):  # padding, the host hash (cookie db version >= 24) and UTF-8 must all check out
    with contextlib.suppress(ValueError):
        if (plain:=(u:=padding.PKCS7(128).unpadder()).update((d:=ciphers.Cipher(ciphers.algorithms.AES(key),ciphers.modes.CBC(b" "*16)).decryptor()).update(encrypted[3:])+d.finalize())+u.finalize())[:32]==hashlib.sha256(host.encode()).digest() or not hashed: return plain[32*hashed:].decode()
    raise ValueError(f"a cookie of {host} does not decrypt (wrong keyring secret?)")
def _chrome_cookies(path,app,secrets,domain=None):  # one install's (host, name, value); the keyring is asked only when a matching row needs it, once per app and operation
    command,rounds,tag=_CHROME_KEYRING[PLATFORM]
    with contextlib.closing(sqlite3.connect(f"file:{path}?mode=ro&nolock=1",uri=True)) as db: hashed,rows=int(db.execute("SELECT value FROM meta WHERE key='version'").fetchone()[0])>=24,[r for r in db.execute("SELECT host_key,name,encrypted_value FROM cookies") if not domain or host_match(r[0],domain)]
    if any(e[:3]==tag for *_,e in rows) and (secrets[app] if app in secrets else secrets.setdefault(app,_keyring(command(app)))) is None: raise ValueError(_KEYRING_ERROR)
    keys={prefix:hashlib.pbkdf2_hmac("sha1",secret,b"saltysalt",rounds,16) for prefix,secret in {**_CHROME_FIXED.get(PLATFORM,{}),tag:secrets.get(app)}.items() if secret}
    return [(host,name,_decrypt(keys[e[:3]],e,host,hashed)) for host,name,e in rows if e[:3] in keys]
_KEYRING_ERROR="Chrome keyring unavailable: no secret from `secret-tool lookup application chrome|chromium` (Linux; unlock the keyring) or the Keychain (macOS)"

def read_chrome_cookies(domain: str, profile: str | None = None) -> dict[str, str]:  # the first install whose matching cookies all decrypt; the others' failures count only when none does
    errors,secrets=[],{}
    for pid,path,app in _chrome_paths(profile):
        try:
            if rows:=_chrome_cookies(path,app,secrets,domain): return {name:value for _,name,value in rows}
        except (ValueError,sqlite3.Error) as e: errors.append(f"{pid}: {e}")
    if errors: raise ValueError(" | ".join(errors))
    return {}
def chrome_cookie_domains(profile: str | None = None):  # every row must decrypt, so a readable cookie cannot hide a locked one of the same host
    try: return {host for secrets in [{}] for _,path,app in _chrome_paths(profile) for host,*_ in _chrome_cookies(path,app,secrets)}
    except (ValueError,sqlite3.Error) as e: raise PermissionError(str(e)) from e
def chrome_profiles() -> list[str]: return [profile for profile,_,_ in _chrome_found()]

def _firefox_profiles():  # (cookies.sqlite, is a profile Firefox starts with) for profiles.ini's profiles and any profile folder
    for base in map(_root,_FIREFOX_ROOTS.get(PLATFORM,())):
        (ini:=configparser.RawConfigParser(strict=False)).read(base/"profiles.ini")
        default={base/s["Default"] for n,s in ini.items() if n.startswith("Install") and "Default" in s} or {base/s["Path"] for n,s in ini.items() if n.startswith("Profile") and s.get("Default")=="1"}
        yield from ((path,path.parent in default) for path in dict.fromkeys([*(base/s["Path"]/"cookies.sqlite" for n,s in ini.items() if n.startswith("Profile") and "Path" in s),*base.glob("*/cookies.sqlite"),*base.glob("Profiles/*/cookies.sqlite")]) if path.exists())
def _firefox_path(profile=None): return (lambda want:max(((path,default) for path,default in _firefox_profiles() if not want or path.parent.name==want),key=lambda c:(c[1],max(f.stat().st_mtime for f in (c[0],c[0].with_name(f"{c[0].name}-wal")) if f.exists())),default=(None,))[0])(profile or os.environ.get("CONVOS_FIREFOX_PROFILE"))  # a named profile, else profiles.ini's default, else the one whose db or WAL was written last
def _backup(source,db):  # Python's backup would retry a lock that Firefox never releases, forever
    with contextlib.closing(sqlite3.connect(source,uri=True,timeout=0)) as src: src.backup(db,progress=lambda status,*_:status in (sqlite3.SQLITE_BUSY,sqlite3.SQLITE_LOCKED) and (_ for _ in ()).throw(sqlite3.OperationalError("database is locked")))
def _snapshot(path,db):  # SQLite's online backup is consistent; while Firefox holds the file exclusively, back up a copy of db and WAL taken with no checkpoint in between
    with contextlib.suppress(sqlite3.OperationalError): return _backup(f"file:{path}?mode=ro",db)
    for _ in range(3):
        with tempfile.TemporaryDirectory() as tmp,contextlib.suppress(FileNotFoundError):  # the WAL can vanish in a checkpoint between exists() and the copy
            stamp,_=(path.stat().st_mtime_ns,path.stat().st_size),[shutil.copy2(src,Path(tmp)/src.name) for src in (path,path.with_name(f"{path.name}-wal")) if src.exists()]
            if stamp==(path.stat().st_mtime_ns,path.stat().st_size): return _backup(f"file:{Path(tmp)/path.name}",db)
    raise ValueError("Firefox cookies kept changing while being copied; retry")
def _firefox_rows(sql, args=(), profile=None):
    if not (path:=_firefox_path(profile)): return []
    with contextlib.closing(sqlite3.connect(":memory:")) as db: return (_snapshot(path,db),db.execute(sql,args).fetchall())[1]
def read_firefox_cookies(domain: str, profile: str | None = None) -> dict[str, str]: return {name:value for name,value,host in _firefox_rows("SELECT name,value,host FROM moz_cookies WHERE originAttributes='' ORDER BY lastAccessed",(),profile) if host_match(host,domain)}  # the default container only
def firefox_cookie_domains(profile: str | None = None): return {host for host, in _firefox_rows("SELECT DISTINCT host FROM moz_cookies WHERE originAttributes=''",(),profile)}

READERS={"safari":read_safari_cookies,"chrome":read_chrome_cookies,"firefox":read_firefox_cookies}
def get_cookies(domain: str, browser: str = "safari", profile: str | None = None) -> dict[str, str]: return READERS[browser](domain, profile)
def get_cookies_any(domains: list[str], browser: str = "safari", profile: str | None = None) -> dict[str, str]: return {name:value for domain in domains for name,value in get_cookies(domain,browser,profile=profile).items()}
