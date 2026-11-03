import contextlib, hashlib, itertools, os, shutil, sqlite3, struct, subprocess, sys, tempfile
from pathlib import Path
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

# A browser is a READERS entry; an operating system is a row in these tables, never a branch in a reader.
PLATFORM=sys.platform
_CHROME_ROOTS={"darwin":{"Library/Application Support/Google/Chrome":("Chrome","")},"linux":{"$XDG/google-chrome":("chrome",""),"$XDG/chromium":("chromium","chromium"),"snap/chromium/common/.config/chromium":("chromium","snap-chromium"),".var/app/com.google.Chrome/config/google-chrome":("chrome","flatpak-chrome"),".var/app/org.chromium.Chromium/config/chromium":("chromium","flatpak-chromium")}}  # root: (keyring app, profile id prefix)
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

def _chrome_roots(): return [(Path(os.environ.get("XDG_CONFIG_HOME") or Path.home()/".config")/root[5:] if root[:5]=="$XDG/" else Path.home()/root,app,tag) for root,(app,tag) in _CHROME_ROOTS.get(PLATFORM,{}).items()]
def _chrome_found(): return [(f"{tag}/{d.name}" if tag else d.name,path,app) for root,app,tag in _chrome_roots() if root.is_dir() for d in sorted(root.iterdir()) if d.is_dir() and (path:=next((p for p in (d/"Cookies",d/"Network/Cookies") if p.exists()),None))]
def _chrome_paths(profile=None): return (lambda want,found:[(p,a) for i,p,a in found if i==want] or [(p,a) for i,p,a in found if i.rpartition("/")[2]==(want or "Default")])(profile or os.environ.get("CONVOS_CHROME_PROFILE"),_chrome_found())  # a bare name matches that profile in every install
def _keyring(command):
    with contextlib.suppress(OSError,subprocess.SubprocessError):
        if not (result:=subprocess.run(command,capture_output=True,text=True,timeout=10)).returncode and result.stdout.strip(): return result.stdout.strip().encode()
def _chrome_keys(app):
    command,rounds,tag=_CHROME_KEYRING[PLATFORM]
    secrets={**_CHROME_FIXED.get(PLATFORM,{}),**({tag:secret} if (secret:=_keyring(command(app))) else {})}
    return {prefix:hashlib.pbkdf2_hmac("sha1",secret,b"saltysalt",rounds,16) for prefix,secret in secrets.items()}
def _decrypt(key,encrypted): return next((value[32:] if not value[:32].isascii() else value).decode(errors="ignore") for d in [Cipher(algorithms.AES(key),modes.CBC(b" "*16)).decryptor()] for plain in [d.update(encrypted[3:])+d.finalize()] for value in [plain[:-plain[-1]]])
def _chrome_rows(path,app,domain=""):  # (host, name, value); value is None when its key sits in a keyring that did not answer
    with contextlib.closing(sqlite3.connect(f"file:{path}?mode=ro&nolock=1",uri=True)) as db: return [(host,name,_decrypt(key,encrypted) if key else None) for keys in [_chrome_keys(app)] for host,name,encrypted in db.execute("SELECT host_key,name,encrypted_value FROM cookies WHERE host_key LIKE ?",(f"%{domain}%",)) if (key:=keys.get(encrypted[:3])) or encrypted[:3]==_CHROME_KEYRING[PLATFORM][2]]
_KEYRING_ERROR="Chrome keyring unavailable: no secret from `secret-tool lookup application chrome|chromium` (Linux; unlock the keyring) or the Keychain (macOS)"

def read_chrome_cookies(domain: str, profile: str | None = None) -> dict[str, str]:  # the first install whose matching cookies all decrypt
    found=[rows for path,app in _chrome_paths(profile) if (rows:=_chrome_rows(path,app,domain))]
    if (rows:=next((rows for rows in found if None not in (value for *_,value in rows)),None)) is None and found: raise ValueError(_KEYRING_ERROR)
    return {name:value for _,name,value in rows or ()}
def chrome_cookie_domains(profile: str | None = None):
    if None in (values:={host:value for path,app in _chrome_paths(profile) for host,_,value in _chrome_rows(path,app)}).values(): raise PermissionError(_KEYRING_ERROR)
    return set(values)
def chrome_profiles() -> list[str]: return [profile for profile,_,_ in _chrome_found()]

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
