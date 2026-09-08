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

Schema initialization runs at startup. Each request opens an existing database
without running migrations or acquiring a writer lock for connection setup.
Reads use one SQLite snapshot, so a response cannot mix membership, keys, and
signed controls from different rotations. Mutations authorize and commit within
one write transaction; a failed batch is rolled back completely, and concurrent
uploads cannot cross a signed epoch history boundary. WAL readers can continue
while another request holds the write transaction.

The relay defaults to 32 request workers; set `CONVOS_SERVER_WORKERS` in the
service environment to adjust the concurrency and memory allowance. Excess
connections receive HTTP 503 with `Retry-After: 1`. Each body is limited to
64 MiB, with exactly one nonnegative `Content-Length` and no transfer encoding.
Idle sockets close after 30 seconds; a 120-second connection deadline also
closes clients that keep sending slowly. The deadline uses the server's
existing accept loop and does not allocate a timer thread for each request.
Unexpected failures are logged on the server and return a generic error.

Large-event pages return manifests without materializing ciphertext bodies in
Python; clients fetch those bodies individually. Ledger heads are computed
from the author-sequence index. The bounded synthetic
[`benchmark_relay.py`](../scripts/benchmark_relay.py) records startup, complete
HTTP reads and writes, concurrent reads, writer contention, and large-page
allocations; see [b7 relay measurements](relay-b7-hardening.md).

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
Each canonical is one signed object and one encrypted semantic replica, subject
to the relay's 48 MiB replica ceiling. Semantic replicas use a separate opaque
relay table: released clients request only logical-row replicas, while clients
that advertise the current pull path receive both. This keeps mixed-version
team sync compatible when new signed semantic object kinds are introduced.

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

Linking a Git checkout publishes a stable opaque grant plus normalized Git
evidence; the grant is not a remote URL or a repository row ID. Core resolves
known clones and worktrees from exact checkout or evidence matches. A linked
non-Git path uses the same opaque grant model while its absolute root remains a
machine-local `config.json` binding. Team repository links auto-contribute for every member by default;
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

Repository lifecycle is passive and conservative:

- adding, removing, renaming, or changing a remote does not disable an already
  bound checkout; SSH and HTTPS forms of the same host/path normalize equally;
- moving the same checkout reattaches through its local checkout identity, while
  a new checkout must match the grant's original Git evidence exactly;
- replacing a checkout at the same path does not inherit the old local binding;
- a new fork or unrelated remote is not silently merged into an existing grant;
- removing `.git` makes the repository binding dormant instead of turning it
  into a recursive path share;
- when an exactly linked non-Git directory later becomes a Git repository, its
  local path grant stays active; after the first commit establishes immutable
  lineage, one portable repository grant is published for future conversations;
  earlier conversations are not reclassified;
- overlapping path/repository grants still select a conversation only once in
  one workspace. A nested repository is classified by Git's deepest enclosing
  root.

Starting-directory and edit classification freeze the resolved path and Git
checkout marker during ingestion. Delayed Git enrichment is accepted only when
that marker still matches; pre-upgrade scope without a trustworthy snapshot is
treated as unknown. Later
filesystem changes therefore cannot retroactively move an old conversation in
or out of scope. Ordinary directories remain path-bound, so moving one requires
an explicit new link.

During the state-v1 cutover, released untyped local path bindings are rewritten
to typed path bindings. A locally owned proofless repository policy is reissued
with immutable evidence only when one unambiguous live repository match exists;
foreign, missing, or ambiguous proofless policies remain dormant.

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
convos remote audit                 # verify signed proofs against typed projections
convos remote repull                # reconcile received rows without deleting existing data
convos remote repull --from-backup /absolute/path/convos.db.pre-remote-repull.bak
convos remote fetch                 # materialize deferred large events
convos remote sync                  # run one foreground incremental sync
convos remote sync --repair         # verify and restore the full retained replica set
```

After one full successful cycle, an unchanged client completes ordinary sync with
one authenticated `state` request. The response carries indexed per-workspace
channel tails; any changed tail, local archive generation, bridge state, policy,
pending work, or uncertainty falls back to the complete idempotent cycle.

The worker writes errors to `<root>/remote/last_error` (by default,
`~/.convos/remote/last_error`). Queries never wait for the server. `doctor`
reports connectivity, identity, workspaces, epochs, upload-blocked rows, pending
uploads, deferred events, and last successful synchronization. `remote audit`
checks surviving origins and current signed proof heads, so missing origins do
not hide lost bodies. It distinguishes exact projections, retained signed
variants, and unavailable bodies; incomplete projections exit nonzero.

`remote repull` is additive and resumable. It verifies authorized relay data,
reconciles received projections, retains conflicting native content and signed
variants, and audits preservation. It never deletes relay orphans, origin-less
children, or the live archive before downloading. An interruption preserves both
existing rows and committed receive progress. `remote/repull.json` records its
phase; the same command resumes it. Manual repull requests cooperative background
sync to yield at a request boundary; a noncooperative holder is reported promptly.

For damage from an older destructive repull, `--from-backup` accepts a read-only
backup with the same archive identity. It restores missing rows in 500-row
transactions without replacing newer rows, and recovers available attachment
bodies and exact legacy signed-body donors. The donor is never deleted: it can
still contain a divergent original version or attachments unavailable elsewhere.
Repull success establishes preservation of the inventoried signed heads, not
that every conflict has a resolved searchable projection. `retained_bodies` and
`remote audit` expose that distinction. Existing ambiguous duplicate identities
are not automatically deleted or merged.

Same-user receive identity no longer depends on temporary recovery flags.
Existing native rows and received bindings are reused. Native differences are
retained rather than overwritten, and compact local publication bases distinguish
an acknowledged old upload from a newer unresolved remote revision. Such a
revision is not silently re-signed as its receiving machine's local content.
Missing provenance dependencies retain their exact signed body and retry when
the referenced edit/turn/file arrives, capped at 500 affected facts per receive
page. Larger backlogs remain retained and can be replayed by explicit repull.
A verified causal successor can update the
association; independent conflicting facts remain retained. Authorization and
signature failures still fail closed.

Unchanged blocked aliases do not by themselves force an archive-wide sync.
`doctor` continues reporting them; relevant changes or explicit repair retry them.

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

The command opens an existing source read-only, verifies the staged snapshot
with SQLite `quick_check`, and publishes it with private `0600` permissions.
It fsyncs the file before replacement and the containing directory afterward.
Missing sources and output paths that refer to the source, including symlinks
and hardlinks, are rejected. A failed copy or validation leaves an existing
backup intact. Choose a fresh output path if the old output has SQLite `-wal`,
`-shm`, or `-journal` files.

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

### Binary storage migration and replica compression

The relay stores encrypted event, row, semantic, and origin payloads as SQLite
`BLOB` values, with small JSON headers kept separately. Attachment ciphertext
already uses binary storage. HTTP envelopes still use base64url, so moving to
binary storage preserves the exact envelopes that existing clients receive.
Quota accounting charges the stored header and ciphertext bytes; transfer limits
continue to account for the larger HTTP representation.

Storage schema 1 must be migrated before starting this server version. Create a
verified, compacted copy with:

```bash
convos-server migrate --db /path/to/server.db --output /path/to/server.binary.db
```

The output path must be new. The command opens the source read-only, takes a
consistent SQLite snapshot, converts payloads in bounded batches, verifies every
reconstructed envelope against its retained wire digest, rebuilds storage usage,
and compacts the copy. It preserves ledger cursors, replica identities, authority
records, timestamps, and signatures. Only a complete copy that passes SQLite
integrity checks is published, with private permissions and durable file and
directory synchronization. A failed migration preserves the original database.

A copy made while the relay runs is suitable for validation. For the final
cutover, stop the relay, run the migration again to a fresh output path, configure
the service to use that output, and restart it. This prevents writes after the
snapshot from being left behind. Keep the original database as a recovery copy;
after new writes, switching back to an older snapshot is not a lossless rollback.

New clients read both legacy uncompressed replicas and compressed replicas.
Compression is disabled for uploads until selected for a workspace on a device:

```bash
convos remote compression personal --codec zstd
convos remote compression personal --codec zstd --migrate
```

Upgrade every receiving client in that workspace before enabling compression.
The relay advertises its supported codecs, but it cannot translate an encrypted
compressed replica for an old client. Client upgrade readiness is an operational
requirement; the relay does not currently enforce a workspace minimum version.

Zstd level 1 is used without external dictionaries, independently for each
replica. Incompressible records retain the uncompressed representation. Envelope
version 2 carries `compression: "zstd"` and `plaintext_size`; these fields are
authenticated by AES-GCM along with the rest of the header. The signed logical
row format remains version 1. Compression level is an encoder choice and is not
needed to decode a frame. Future codecs require explicit reader support.

`--migrate` reads this device's retained row and semantic replicas, checks their
authenticated plaintext, and verifies exact decompression before uploading a
smaller representation. The replacement names the expected old wire digest and
is atomic: a concurrent change cannot be overwritten. The original uploader,
workspace, key epoch, logical identity, signed bytes, and ledger cursor remain
unchanged. Another device's retained copy is not replaced. An acknowledgment
loss is safe to retry; a content-free local cursor resumes completed pages.
Use `--restart` with `--migrate` to rescan the device's earlier retained copies.
Selecting `--codec none` affects future uploads and leaves retained compressed
replicas readable by upgraded clients.

Both relay and client bound pages by expanded payload size as well as transfer
size. Clients authenticate before decompressing, limit frame size and window
memory, and reject unknown codecs, truncated or concatenated frames, trailing
data, and size mismatches. Compression does not alter event history, workspace
controls, or attachment encodings.

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
