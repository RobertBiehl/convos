---
summary: "Web ingest regression diagnosis -- a Chrome cookie-decryption bug (not anti-bot) -- with the verified fix and the remaining ChatGPT backend-api follow-up."
read_when:
  - Fixing web sync (chatgpt / claude)
  - Touching read_chrome_cookies / browser cookie extraction
  - Deciding the fate of browser.py / Playwright
status: diagnosed; Claude fix verified end-to-end (2026-06-06)
---

# Web ingest: regression diagnosis + fix (spec 03)

Part of [00-overview](00-overview.md). Web sync (ChatGPT + Claude, ~75% of the
archive: 976 + 962 of 2,597 convos) was failing. Live-tested 2026-06-06; root
cause found, and the Claude fix verified through the real code path. It is **not
anti-bot, and Playwright is not needed.**

## Symptoms observed

- urllib (current transport): Claude `403`, ChatGPT `400`.
- curl_cffi (Chrome TLS impersonation): Claude `403` bare / `400` with cookies;
  ChatGPT `400`. So TLS impersonation alone did not fix it.
- Site roots (`claude.ai/`, `chatgpt.com/`) load `200`; `example.com` /
  `api.github.com` `200` -> the network is fine.
- Auth cookies present and unexpired (`sessionKey`, `__Secure-next-auth.session-token`, `cf_clearance`).

## Root cause: Chrome cookie decryption drops a 32-byte prefix

Recent Chrome on macOS prepends a **32-byte SHA256(host) hash** to the cookie
plaintext before encryption. `read_chrome_cookies` strips the PKCS7 padding but
**not** this prefix, so every decrypted value is `[32 garbage bytes] + [real
value]`.

Evidence (`sessionKey`):
- strip 0 (current): `'6%\xda\x82\x16\x06jWXEu+E...sk-ant-sid02-L...'`
- strip 32: `'sk-ant-sid02-L7afKzXsRXim_BAmuA4...'` (correct)

A corrupted `cf_clearance` -> Cloudflare `403`/`400`; a corrupted `sessionKey`
-> `401`. This is the entire regression -- it broke when Chrome updated, which
is why the DB already holds ~1,900 web convos from when it worked.

## Fix (core, ~1 line, no new dependency)

In `read_chrome_cookies`, after removing PKCS7 padding, strip the leading 32
bytes for the new Chrome format before decoding. Guard for older Chrome (no
prefix) -- e.g. strip only when the leading 32 bytes are not printable ASCII, or
key off Chrome version. No transport change, no new dep; budget-neutral
(modifies the existing decode line).

**Verified:** with the strip, **plain urllib** restores Claude through the real
path -- `fetch_claude(chrome, limit=1)` -> 1 conv, 6 msgs; `/api/organizations`
-> `200`. curl_cffi and Playwright are unnecessary.

## ChatGPT accessToken expiry (RESOLVED 2026-06-07)

After the cookie fix, ChatGPT `/api/auth/session` returns a session (correct
user email), but the embedded `accessToken` is **expired**: `/backend-api/*`
returns `401` with `{"code": "token_expired", "message": "Provided
authentication token is expired. Please try signing in again."}` -- even though
the `__Secure-next-auth.session-token` cookie is valid for months. The cached
OpenAI access JWT simply hasn't been refreshed (the browser app refreshes it; a
direct `/api/auth/session` GET returns the stale one).

This is **not** a convos bug and **not** the cookie bug. Resolution: the user
re-opens chatgpt.com in Chrome (sign in / load the app) to refresh the token,
then `convos sync` works for ChatGPT. **Confirmed 2026-06-07:** after the
refresh, `fetch_chatgpt` works end-to-end (listed `total=101` convs). Claude is
unaffected and fully restored by the cookie fix alone.

Optional enhancement: detect `token_expired` in the fetch path and surface a
clear "your ChatGPT session expired -- open chatgpt.com and sign in, then retry"
message instead of swallowing it (see the silent-failure note above).

## Linux: TLS fingerprint challenge (2026-10-30)

On Linux, claude.ai answered urllib with `403` and `cf-mitigated: challenge`
while the same, correctly decrypted cookies worked in the browser. Unlike the
June macOS case above, the cookies were fine: Cloudflare challenged Python's TLS
handshake. Since #162, web requests go through `curl-cffi` impersonating the
browser the cookies came from (TLS fingerprint and User-Agent), on every OS.

- macOS: the June finding stands (plain urllib worked once the cookies
  decrypted); the switch to curl-cffi there is not yet verified by a live
  `convos sync` with Safari and Chrome (maintainer run before merge).
- curl-cffi reads proxies only from the environment; when it sets none, convos
  passes the proxies urllib would use (macOS system settings), unless
  `urllib.request.proxy_bypass` exempts the request's host.

## Implications

- **Playwright is not needed** for the current sources -> `browser.py`'s
  Playwright code can be retired and the dep dropped ([01](01-foundation-core.md)
  sec 0), reclaiming ~125 core LoC. (Reverses the earlier "maybe we need a real
  browser" worry -- TLS/anti-bot was not the problem on macOS in June; for
  the later Linux TLS challenge see above.)
- Ship the cookie fix regardless of the feature roadmap: it is a real bug that
  silently broke the majority of ingest.
- Add a **regression test**: assert decrypted cookie values are printable ASCII
  (or match a known prefix like `sk-ant-sid` for Claude). This catches the next
  Chrome cookie-format change before it silently breaks sync again -- and a
  silent breakage is exactly what happened here (`fetch_chatgpt` even swallows
  the error unless `CONVOS_CHATGPT_DEBUG` is set; consider surfacing fetch
  failures by default).

## Safari note

Separately, Safari cookies are unreadable without Full Disk Access (`doctor`:
"no access"). Unrelated to this bug; Chrome is the working path.
