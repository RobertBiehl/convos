---
summary: "Install, operate, recover, and use the self-hosted encrypted Convos remote."
read_when:
  - Setting up personal multi-computer synchronization
  - Operating the self-hosted relay
  - Creating a team workspace or managing membership
  - Backing up or recovering remote state
---

# Encrypted remote

The Remote application synchronizes signed encrypted events. The relay
never receives conversation plaintext, file paths, repository names, embeddings,
attachments, or workspace keys. Recall and archive queries remain local.

The next storage and recovery architecture is specified in
[Remote state v1](remote-state-v1.md). It makes canonical provenance part of
DuckDB, limits settled SQLite state to rebuildable sync metadata, and advances
the client and relay together without an old-protocol compatibility layer.

This is a security-sensitive preview. It uses established primitives through
`cryptography` and has protocol/acceptance tests, but has not received the
independent review required before calling it production-grade encryption.
Payloads and keys remain opaque to a passive relay or database compromise.
Membership, roles, device certificates, removals, history entitlements, epoch
key commitments, and approvals are carried in a client-signed hash chain.
Clients pin the chain they have observed and reject rollback, forks, invalid
transitions, relay metadata that disagrees with the signed head, and keys
outside the signed device entitlement or commitment. A workspace omitted by
the relay is excluded from upload rather than used with stale state. Version 1 does
not provide cross-client gossip or an external transparency log, so a malicious
relay can still withhold updates or partition clients that have not compared
their pinned heads.

Epoch-changing controls also pin the relay history boundary: the exact tail and
latest `(sequence, event_id)` for every author. Recovery must reach every signed
checkpoint with no unexplained sequence gap before publication becomes ready.

## Install

Install the core and client applications together:

```bash
uv tool install convos --with convos-remote
```

For a local checkout, `uv sync --all-extras` installs the same workspace.

Runnable personal and team demonstrations are available under
[`examples/remote`](../examples/remote/README.md). They use synthetic temporary
data and a loopback relay, never the normal conversation archive.

## Run the server

The relay is a single process backed by SQLite. Run it behind an HTTPS reverse
proxy; device bearer tokens must not traverse an untrusted network over HTTP.

```bash
uv tool install convos-remote-server
install -d -m 700 ~/.local/share/convos-server
convos-server serve \
  --db ~/.local/share/convos-server/server.db \
  --host 127.0.0.1 --port 8787
```

For a persistent Linux user service, place this in
`~/.config/systemd/user/convos-server.service`:

```ini
[Unit]
Description=Convos encrypted relay

[Service]
ExecStart=%h/.local/bin/convos-server serve --db %h/.local/share/convos-server/server.db --host 127.0.0.1 --port 8787
Restart=on-failure

[Install]
WantedBy=default.target
```

Enable it with `systemctl --user daemon-reload` followed by
`systemctl --user enable --now convos-server`. On a headless host, enable user
service startup without an interactive login using `loginctl enable-linger`.

Terminate TLS with Caddy, nginx, or another reverse proxy and expose only the
HTTPS endpoint. The server itself has no TLS or public-network configuration.
The client rejects plaintext HTTP except on loopback;
`CONVOS_REMOTE_INSECURE=1` is only for a trusted test network.

## Personal multi-computer setup

On the first computer:

```bash
convos remote setup https://convos.example.com robert --device macbook
convos remote enable
convos doctor
```

`setup` creates the user root, device identity, personal workspace, epoch key,
and recovery key. Store the printed recovery key offline and share the printed
user ID through an authenticated channel before joining a team. Personal policy
is always `all`: no path or repository allowlist is required.

On another computer:

```bash
convos remote recover https://convos.example.com robert --device workstation
convos remote enable
```

The CLI prompts for the recovery key without placing it in shell history or the
process list. Recovery enrolls a new independently signed device, restores the
complete personal history, and rotates the personal workspace. It never copies
a DuckDB file. Team keys are deliberately not in the recovery bundle. A
recovered device remains pending in each team workspace until the same user
authorizes it from an existing device or the other represented team members
approve it.

`enable` installs the standard Convos conversation-capture hooks and a
persistent user service: launchd on macOS and systemd on Linux. Each lifecycle
event has one `convos capture <agent>` command; obsolete remote wake-only hooks
are removed automatically. The hook only updates the local archive. The worker
observes those changes and performs Git inspection, encryption, network I/O,
pulling, and projection. Normal work never requires `convos remote sync`.

### Memory on multiple computers

Install and enable `convos-memory` on each computer if canonical agent memory
should travel with the personal archive. The remote client discovers it as an
optional adapter; the server and wire protocol need no memory-specific
installation or account:

```bash
uv tool install --reinstall "git+https://github.com/RobertBiehl/convos.git" \
  --with "convos-memory @ git+https://github.com/RobertBiehl/convos.git#subdirectory=apps/memory" \
  --with "convos-remote @ git+https://github.com/RobertBiehl/convos.git#subdirectory=apps/remote"
convos memory enable
```

The background worker then sends canonical revisions and tombstones through the
personal workspace automatically. Memory never enters team workspaces.
Sequential changes converge without a prompt; concurrent semantic changes stay
pending locally for `convos memory review` or the installed agent workflow.
The relay sees ciphertext and envelope metadata, not memory text or absolute
checkout paths. Preserve the recovery key and continue taking private
`convos memory backup` snapshots: the remote synchronizes canonical state, not
native Codex/Claude files, hooks, or the complete device-local ledger.
Each canonical is one signed object and one encrypted row replica, subject to
the relay's 48 MiB row-replica ceiling.

Forgetting a user-owned canonical publishes a root-signed bodyless descendant
containing the complete known ancestry. Recipients accept it only after proof
verification, and it defeats stale active ancestors without relay-specific
deletion infrastructure. Locally revised, projected, or provider-backed state
is retained. Any authorized holder can repair the tombstone on a replacement
relay, but it cannot erase historical plaintext or ciphertext already retained
by a peer, relay operator, filesystem snapshot, or backup.

## Team workspaces

Users create their own account before an administrator adds them:

```bash
# New team member, on one of their devices
convos remote setup https://convos.example.com alice --device laptop

# Administrator; use Alice's out-of-band user ID, not a directory name
convos remote workspace backend
convos remote invite backend ALICE_USER_ID
convos remote link ~/src/backend backend

# Per-member contribution policy (defaults shown)
convos remote config backend --auto-contribute --match cwd,edit
```

Name lookup is a convenience for a trusted relay. An out-of-band user ID binds
the invitation to the intended root key even if the relay directory is later
compromised. Before wrapping any workspace or history key, clients verify each
device's user-root-signed certificate and its signing and encryption keys.

Linking a Git checkout publishes its stable repository identity; core resolves
every known clone and worktree for that identity. A linked non-Git path uses an
opaque policy token whose absolute root remains a machine-local `config.json`
binding. Team repository links auto-contribute for every member by default;
each member can disable links created by teammates with
`--no-auto-contribute`, restore the team default with `--inherit`, or keep the
default explicitly with `--auto-contribute`. A member's own explicit links
remain active either way. Non-Git path links never auto-bind on another device.

Conversation matching defaults to both the captured starting directory (`cwd`)
and captured file edits (`edit`). `--match cwd`, `--match edit`, or
`--match none` narrows or disables future automatic contribution. The settings
are root-signed, encrypted in the team workspace, and shared across that
member's authorized devices. The client applies them passively during sync;
there is no hook prompt or review queue. When either enabled signal matches,
the complete conversation is routed once to that workspace. Repository policy
never silently slices turns or creates partial conversation history.

The client requires `convos-redact` and runs it inside the team `publish`
boundary before event signing and encryption. High-confidence credential spans
become typed markers without secret-derived hashes; personal workspaces remain
lossless. Team binary bodies are not read; their attachment records become
explicit `[REDACTED:attachment]` placeholders rather than disappearing.
`convos redact status` shows only
the local workspace, record, field path, line, and secret kind. The relay cannot
scan because it receives no plaintext. See [local secret
protection](redact.md) for supported forms and the non-retroactive boundary.

A conversation may span several repositories. A match routes the whole
conversation, including all of its turns, tools, edits, and provenance. The
workspace membership is therefore the trust boundary; use a separate
conversation when work must remain outside it.

Membership and history:

```bash
convos remote grant-all backend alice
convos remote remove backend alice
convos remote remove-device backend DEVICE_ID
```

New members receive no old events or keys by default. `grant-all` wraps old
epoch keys to their devices and resets their history boundary. There is no
per-row visibility mode: use a separate workspace for a different audience.
User or device removal rotates the epoch.
Device removal is workspace-specific, so it does not disable the device's
personal workspace or unrelated teams. It cannot erase plaintext or keys
already obtained.

### Device approval

A workspace is one independently encrypted sync scope: the automatically
created personal workspace or one named team workspace such as `backend`. It
has its own signed member/role map, authorized device roster, removal
tombstones, key epochs, and history policy. Approval for one workspace grants
nothing in another.

On the pending device:

```bash
convos remote request-device backend
```

The request signs the exact workspace ID, current signed-state hash and epoch,
user ID, device ID, root-certified signing/encryption keys, certificate hash,
nonce, activation time, and expiry. It grants nothing by itself.

An existing device belonging to the same user can approve it immediately:

```bash
convos remote approve-device backend DEVICE_ID
```

The new device inherits that device's workspace access, the user's existing
role, and the same history-inheritance flag. Complete-history epoch keys are
rewrapped to it when applicable. No administrator action is needed. An explicit
rejection invalidates the proposal, and an explicitly removed device ID cannot
use this path or be reauthorized.

If the user has no authorized device in the workspace, authorization requires a
strict majority of the other users represented by authorized devices in the
signed roster. Each user gets one vote even if they have several devices; the
requesting user is excluded. Every voter runs the same `approve-device`
command. The final vote atomically advances the signed state and rotates the
workspace epoch. In a two-user team this is one vote from the other user, with
a one-hour activation delay enforced against the relay's clock and stored
proposal window; a client-supplied approval timestamp cannot bypass or revive
it. A one-user team with no authorized device has no electorate and cannot use
team voting.

Majority recovery is future-only. It restores the user's existing membership
and role but does not silently release older keys. Complete history remains
administrator-controlled with `grant-all`. Alternatively,
the recovered device can ask the other represented users to activate the
history entitlement it already had:

```bash
# Recovered device
convos remote request-history backend

# Other represented users, one vote each
convos remote approve-history backend DEVICE_ID
```

History activation is a separate signed majority decision and does not change
membership or role. It can only install epoch keys held by the device that
finalizes the vote; voting cannot recreate material that no remaining device
has.
On the recovered device, the next ordinary sync detects that its earliest
available epoch moved backward, rewinds the delivery cursor, and idempotently
imports all newly decryptable events. `convos remote approvals backend` shows
active device and history proposals.

## Daily operation

```bash
convos doctor
convos remote doctor
convos remote fetch                 # materialize deferred large events
convos remote sync                  # explicit repair/backfill only
```

The worker writes errors to `<root>/remote/last_error` (by default,
`~/.convos/remote/last_error`). Queries never wait for the server. `doctor`
reports connectivity, identity, workspaces, epochs, pending uploads, deferred
events, and last successful synchronization.

## Backup and restore

Relay backups are optional recovery acceleration and preservation of the old
workspace authority, not canonical archive backups. Surviving authorized peers
can rebuild current rows, proofs, memory objects, tombstones, and blobs on a
fresh relay.

Back up a consistent server snapshot while it is running:

```bash
convos-server backup \
  --db ~/.local/share/convos-server/server.db \
  --output ~/backups/convos-server.db
```

Restore by stopping the relay, replacing its database with the snapshot, and
starting it again. The backup contains ciphertext, ACL metadata, key envelopes,
and delivery cursors, but no workspace key. Clients can safely retry uploads and
pulls after rollback because replica insertion and local projection are
idempotent. Signed history checkpoints detect a restored relay that is missing
an already-bound auxiliary-event prefix; without gossip, a newest suffix after
the latest signed boundary can still be withheld.

A backup taken before a memory forget operation still contains the older
encrypted active replica. Treat relay-backup retention as part of the deletion
policy; restoring such a backup without a surviving tombstone can make that
historical object current again.

If the relay itself is lost, a surviving device can establish fresh relay
credentials without changing its signing identity, then explicitly re-found a
team from the copies held by the members it still trusts:

```bash
convos remote rehome https://replacement.example.com
convos remote origins
convos remote workspace Replacement
convos remote invite Replacement TRUSTED_USER_ID
convos remote refound Replacement OLD_WORKSPACE_ID
```

`rehome` creates fresh personal/workspace keys and prints a new recovery key.
It does not recreate the old relay's authority. `refound` binds one verified old
signed control chain into the new workspace once; ordinary sync then advertises
flat opaque proof identities and uploads only rows the new relay lacks. Any
surviving authorized holder can deliver those rows without the original
author's private key. Only the replacement workspace's explicitly invited
members receive its encryption key. Old plaintext already held by an excluded
member cannot be revoked.

Loss of every copy of a row is permanent data loss. Loss of every enrolled
device and recovery key also prevents recovery of the old personal authority,
although intact rows and original proofs held by teammates remain repairable
into an explicitly re-founded workspace.

`state.db` is disposable synchronization metadata. Device `config.json` pins
the last successfully synchronized DuckDB identity and generation, so deleting
only `state.db` cannot erase archive rollback detection. If `convos.db` is
missing or truly empty, sync restores personal rows from the relay under their
native IDs without republishing them. If a non-empty archive has a different ID
or an older generation, sync keeps it, restores relay rows additively under
foreign IDs, and remains blocked. Preserve that suspect database, install a
fresh empty archive, and sync again before manually reconciling any local-only
rows from the preserved copy.

## Local files

Remote client state lives under `<root>/remote/` (by default,
`~/.convos/remote/`):

- `config.json`: mode `0600`, device private keys, token, encrypted-workspace
  keyring, local workspace labels and non-Git path bindings, pinned controls,
  and the last successfully synchronized DuckDB archive ID/generation
- `state.db`: content-free receipts, cursors, heads, exact sequence metadata,
  sharing policy, and working-file manifests
- `outbox/`: unacknowledged encrypted envelopes; removed after acknowledgement
- `backups/state-*/`: exact private state cutover bundles, retained until the
  user deliberately removes them
- `worker.log`, `last_error`: operational state

Absolute checkout roots remain local in core checkout mappings or device
configuration. They are not placed in event payloads, server storage, repository
fixtures, CI artifacts, or logs.
