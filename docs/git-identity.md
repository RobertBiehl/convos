# Git repository identity

Convos joins exact conversation and file evidence across sessions, worktrees,
and devices. A short-lived credential proxy must not split that history, and
normalization must not change who authored the evidence.

## Identity and checkout bindings

Repository identity is a digest of Git lineage (root commits) and the sorted set
of canonical fetch and push remotes. A file identity is a digest of repository
identity and relative path. Branch, checkout path, and proxy session are not
portable identity. Different remote sets, including forks or an added upstream,
remain distinct unless the user explicitly maps them.

Git selects URLs through `git remote -v`, including aliases, longest-prefix
rewrites, fetch selection, and push selection. Convos substitutes the configured
URL only when Git's selected result is a loopback proxy, then normalizes it.
It does not rerun a competing `insteadOf` rule. Direct loopback URLs and aliases
that cannot be resolved without a proxy contribute no remote evidence unless
an explicit mapping resolves them. Proxy tokens never enter new identity facts.

When remote evidence exists, the canonical digest wins over a stored checkout
binding. Otherwise an existing binding with matching lineage is a local fallback.
A binding lives in the archive's existing `repository_checkouts` and
`repository_aliases` tables; it is not a synced identity fact.

Why canonical wins: two clones with the same lineage and remotes must converge.
Keeping an old bound ID would let one checkout preserve a proxy-derived or
pre-mapping ID forever while every fresh clone computes something different.
An ordinary server rename can still split historical and new observations;
a deliberate prefix rule can join the old and new names.

Sharing grants remain separate authorization objects. An explicit grant bound
to the same checkout marker and lineage survives a remote change; an unrelated
replacement checkout cannot inherit it. Canonicalizing identity does not create
a new grant or widen another user's sharing policy.

## Explicit URL mappings

Standard SSH/HTTPS forms and the public GitHub/Bitbucket SSH endpoints normalize
automatically. Deployment-specific host, port, and path rules belong in
`~/.convos/config.json` (or `$CONVOS_PROJECT_ROOT/config.json`):

```json
{
  "git_identity": {
    "host_aliases": {"git.example.com": "code.example.com"},
    "url_prefixes": {
      "https://code.example.com:7999/": "https://code.example.com/scm/"
    }
  }
}
```

`host_aliases` replaces the host. `url_prefixes` replaces a normalized URL prefix;
its longest matching prefix wins and takes precedence over host aliases. Prefixes
start with `https://` and end with `/`. Port 7999 has no built-in path mapping:
custom server layouts must remain a user decision. Teams that want the same
IDs must agree on rules; the config is local and is not silently distributed.

## Migration and signed retirement

Capture and remote sync compare a fingerprint of the built-in identity recipe
and configured rules with the last completed fingerprint in `core_migrations`.
Unchanged rules return before Git inspection, backup, or a write connection.
Changed rules collect live Git evidence outside the write connection, validate
that the archive generation and config did not change, create a verified private
backup, and commit the local rewrite plus fingerprint atomically. Schema remains
17; there is no persistent old-to-new mapping table or mapping fact.

Only locally observed facts migrate. Repository IDs and dependent file, version,
checkpoint, and checkpoint-link IDs are recalculated; local edit associations,
capture scopes, checkout bindings, and aliases follow. Original conversation,
tool, and file-edit evidence stays intact. A crash before commit leaves the old
state and fingerprint available for retry.

For example, device A previously published a repository and file under an SSH
URL. A new rule maps that URL to its HTTPS form:

1. A writes the canonical repository/file facts and removes its old local claims.
2. Sync signs the canonical facts and signed deletes for obsolete facts authored
   by this device. The deletes extend the existing revision chains.
3. B verifies and applies A's revisions. B never calculates A's migration.
4. A delayed old replica cannot resurrect a deleted claim. Fresh replay reaches
   the same canonical graph, even with individual records delivered out of order.

A fact here is one signed repository, file, version, edit association, checkpoint,
or checkpoint-link observation. A delete retires only its author's claim. If
another author still owns the same old physical row, that row stays until the
other author retires it. After every author upgrades and synchronizes, obsolete
active facts disappear; compact deletion proofs remain for replay protection.
Superseded bodies are no longer re-exported as current facts.

Retirement requires an explicit local change notice in the existing
`archive_changes` table. Missing local ownership alone is never deletion intent:
a recovered archive can contain this user's signed facts without local capture
markers. Both incremental and full scans find pending retirements, including
after sync state is rebuilt or a crash occurs before signing.

## Limits reviewers should preserve

- Old clients must upgrade to consume provenance deletes. A partial rollout can
  temporarily retain old IDs belonging to devices that have not migrated.
- Proxy recovery requires a unique same-lineage match by the longest remote path,
  using stored canonical candidates or a live checkout whose marker and lineage
  still match. Ambiguous or unavailable evidence stays unresolved.
- Mappings are user-controlled and may be removed. A lossy rule cannot be inverted
  from already canonicalized stored URLs. Supply a forward rule for the stored
  form when changing destinations; removing a rule can split future captures.
- Same-lineage forks and different remote sets are not automatically merged.
- This contract covers core archive provenance. The optional Memory product
  currently uses a separate repository-scope recipe; applying these mappings
  there requires a separate migration of its existing scope bindings.
- Signed deletes change active state, not historical ciphertext already stored
  by a relay or backup. Fresh relay pulls may download superseded replicas;
  relay garbage collection is a separate feature.
