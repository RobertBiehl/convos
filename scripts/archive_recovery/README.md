# Archive recovery tools

Incident-specific repair, diagnosis, simulation, verification, and report
packaging live together here. These are source-checkout operator tools, not
installed commands or dependencies of the core CLI. Run them from the exact
reviewed release checkout using its Convos Python environment.

| Tool | Purpose |
| --- | --- |
| `repair_b7_archive.py` | Back up, plan, and simulate repair; explicit owner-only live apply |
| `diagnose_b7_rows.py` | Read-only inventory of unavailable signed bodies |
| `verify_b7_attestation.py` | Verify recorded-branch attestation in an isolated simulation |
| `verify_b7_recovery.py` | Audit a repair target; relay recovery requires explicit `--repull` |
| `package_b7_recovery.py` | Build a patch and allowlisted maintainer-report bundle |
| `b7_minimal_fixture.py` | Build and check the portable synthetic regression fixture |

The b7 names identify the incident; these tools also cover the affected b8
archives. Follow [the recovery procedure](../../docs/b7-archive-recovery.md)
and [release boundaries](../../docs/archive-hardening.md).

## Write ownership

Core owns durable schema migrations and canonical archive transactions. The
tools call core's validated repair/writer functions; they do not provide an
independent SQL mutation path. Core and installed products never import this
directory. Backups, private exports, precondition checks, simulation, and operator
workflow stay here. Synthetic fixture construction is test data only.

The narrowly scoped v3 placeholder correction remains beside core ingestion
because it is also part of the supported old-archive upgrade path. Signed-body
restoration and retention are ongoing archive integrity responsibilities.
Neither is moved to an optional tool that could leave a gap when absent.

## Distribution

The package wheels and sdists do not include these scripts. Use the matching
tagged repository source and its dependencies; copying only this directory is
insufficient. Do not add private simulation outputs or donor archives here.
Future incident-specific scripts should use this directory, while durable
migrations stay with the product that owns the affected database.
