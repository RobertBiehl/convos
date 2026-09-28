# Repository identity review evidence

Review scope: PR #140, based on `20a56a3`, including the subsequent working-tree
fixes recorded with this report. Public examples use synthetic Git repositories,
users, relay state, and `example.com` URLs. No live conversation archive was
used for validation.

## Product motivation

Convos retrieves exact prior decisions, commands, and edits so agents can resume
work across sessions and devices. Proxy-session identities defeat that purpose;
receiver-side rewriting also loses the distinction between an author's evidence
and a peer's interpretation. Canonical local evidence plus signed author
retirement preserves convergence, exact source content, and attribution.

## Reproduction and verification

- Initial focused baseline: 169 passing tests.
- Four added regressions failed against the initial implementation: two competing
  proxy-rewrite cases, canonical evidence versus a stale checkout binding, and
  author-signed provenance deletion. All four pass after the changes.
- Three additional failures exposed inferred deletion of recovered own facts and
  missing pending retirements during full scans after state loss. Explicit local
  retirement notices fixed all three.
- The full gate exposed a first-sync failure when the read-only database did not
  yet exist. The existing two-device source-ownership fixture reproduced it;
  the complete sync-client suite subsequently passed (130 existing tests).
- The new real relay/client-path regression changes only identity configuration
  after settled sync, verifies both devices converge, then repulls old replicas.
  The canonical graph and original conversation/edit content survive. Receiver
  assertions preserve the existing portable-path representation separately from
  the author's local paths.
- Full final gate: `CONVOS_TEST_SHARD=N/4 uv run --no-sync pytest -m 'not integration' -q`
  for N=0..3: **270 + 273 + 275 + 274 = 1,092 passed**. Nine live integration cases
  were excluded. Tests cover interrupted backups, unchanged-rule no-ops, config
  validation, graph dependencies, shared-author claims, same-user different-device
  delivery, reversed single-record replay, signature rollback, checkout replacement,
  and existing sharing grants. Assertions check projected state and preserved
  evidence; they do not merely assert mocked calls.
- Core token-counted lines: **1,724 < 1,725**; Remote: **2,469 < 2,850**. Statement
  packing and the archive connection ledger pass with the full gate.
- `uv build --all-packages --wheel`: all eight packages build at 0.11.6b18.
- `scripts/smoke-wheel.sh` cannot install on this macOS host: its Linux-oriented
  binary-only check requires a `llama-cpp-python` wheel unavailable for macOS.
  Linux smoke and installed customer lifecycle checks remain PR CI checks.

## Independent review and publication

Clawpatch reviewed the core and remote feature boundaries against the identity
contract and product invariants. The final core pass included explicit retirement
notices and the recovery regressions. No in-scope findings remained. Review
records describe dirty-tree review at the recorded base SHA, not a pristine
review of that base commit.

Machine-local root paths and prompt paths in generated review metadata are
redacted for publication. The project metadata names the public repository;
review findings and statuses are changed only through Clawpatch's triage command.

## Separate follow-up

Optional Memory derives repository scope from its own effective-origin recipe.
A synthetic Git checkout with unchanged configured origin but a different
loopback proxy port produces a different Memory repository identity. This code
predates and is outside the archive-provenance diff. Unification needs a separate
scope-binding migration decision; the Clawpatch finding records that boundary.
