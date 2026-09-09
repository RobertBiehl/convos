# Archive recovery

Two source-checkout tools, with no package-install side effects:

- `diagnose.py` runs the canonical signed-row audit and exports exact unavailable
  claims, their proofs, and physical relationship counts. It is read-only and
  offline. Exit 1 means the report contains unresolved integrity issues.
- `repair.py` repairs only proven legacy scope placeholders and restores selected,
  exact signed bodies from an earlier backup. Its default target is an isolated
  copy. Live application requires the archive owner and an attachment backup.

Use the [recovery procedure](../../docs/archive-recovery.md). Runtime imports,
Remote synchronization, and signing remain in the normal product commands.
These tools do not wrap those commands, read signing credentials, or contact a
relay. They are supplied with repository source, not wheels or sdists.

Core owns durable migrations, proof/body validation, archive writes, and change
tracking. The tools own backup, planning, private reports, and orchestration of
explicit repair. Installed products never import this directory. Regression
fixtures belong in tests; reporter-specific packaging and signing experiments
are not maintained operator tools.

Names describe behavior. A version on which a defect was reproduced is evidence
of an affected release, not evidence of when the defect was introduced.
