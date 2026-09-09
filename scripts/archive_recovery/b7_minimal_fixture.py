"""Build/check one fully synthetic b7 archive; never open a user's live archive."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import sys
import tempfile

STAMP = "2000-01-01T00:00:00.000000"
WORKSPACE = "synthetic-workspace"


def load_code(checkout: Path) -> None:
    global core, projection, protocol
    checkout = checkout.resolve()
    sys.path[:0] = [str(checkout / "src"), str(checkout / "apps/remote/src")]
    from ai_convos import cli as core
    from ai_convos_remote import projection, protocol
    if Path(core.__file__).resolve() != checkout / "src/ai_convos/cli.py" or Path(projection.__file__).resolve() != checkout / "apps/remote/src/ai_convos_remote/projection.py":
        raise ValueError("Requested checkout was not loaded; refusing an ambiguous test")


def write_json(path: Path, value: object) -> None:
    with path.open("x") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, default=str)
        stream.write("\n")


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_identity(name: str) -> dict:
    # Public, deterministic TEST seeds. No machine credentials or random user identity.
    sign = protocol.ed25519.Ed25519PrivateKey.from_private_bytes(hashlib.sha256(f"convos-b7-public-fixture/{name}/sign".encode()).digest())
    box = protocol.x25519.X25519PrivateKey.from_private_bytes(hashlib.sha256(f"convos-b7-public-fixture/{name}/box".encode()).digest())
    return dict(id=protocol.digest(sign.public_key().public_bytes_raw())[:32], name=name,
                sign_private=protocol._raw(sign), sign_public=protocol._raw(sign.public_key()),
                box_private=protocol._raw(box), box_public=protocol._raw(box.public_key()))


def material() -> dict:
    root, device = test_identity("synthetic-root"), test_identity("synthetic-device")
    user = protocol.public_id(root["sign_public"])
    cert = protocol._signed(dict(v=1, user=user, device=protocol.public(device), issued_at=STAMP + "Z"), root["sign_private"])
    signer = dict(user=user, root_public=root["sign_public"], device=protocol.public(device), certificate=cert, history=True)
    control = dict(workspace=WORKSPACE, revision=1, epoch=1, devices={device["id"]: signer})
    cfg = dict(user=user, device=device, workspaces={WORKSPACE: dict(kind="personal", epoch=1)},
               controls={WORKSPACE: control}, server_state=dict(workspaces=[dict(id=WORKSPACE, controls=[control])]))
    fid = protocol.digest(dict(repository=None, path="example.txt"))
    payload = dict(id="e", turn="m", file=fid, repository=None, old_content_hash=None,
                   new_content_hash=protocol.digest(b"alpha\n"), evidence="captured_exact")
    observation = dict(kind="edit.observed", entity="e", payload=payload, observed_at=STAMP)
    current = protocol.logical_fact(observation)
    alternate = current | {"data": current["data"] | {"evidence": "legacy_scope_conflict"}}
    incoming = observation | {"payload": payload | {"new_content_hash": protocol.digest(b"beta\n")}}
    version_id = protocol.digest(dict(file=fid, content=protocol.digest(b"alpha\n")))
    version = dict(v=1, kind="file.version", id=version_id, state="active",
                   data=dict(file=fid, content_hash=protocol.digest(b"alpha\n"), observed_at=STAMP))
    rows = [current, alternate, version]
    proofs = [protocol.row_proof(device, user, WORKSPACE, 1, row) for row in rows]
    return dict(cfg=cfg, signer=signer, file=fid, rows=rows, proofs=proofs, incoming=incoming)


def database_rows(path: Path) -> dict:
    with core.open_db(path, read_only=True, purpose="fixture.export") as db:
        tables = db.execute("SELECT table_schema,table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema IN ('main','provenance','remote') ORDER BY 1,2").fetchall()
        result = {}
        for schema, table in tables:
            cursor = db.execute(f'SELECT * FROM "{schema}"."{table}" ORDER BY ALL')
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            if rows:
                result[f"{schema}.{table}"] = dict(columns=columns, rows=rows)
    return json.loads(json.dumps(result, default=str))


def build(directory: Path) -> dict:
    directory.mkdir()
    path, data = directory / "convos.db", material()
    cfg, signer, proofs = data["cfg"], data["signer"], data["proofs"]
    with core.open_db(path, purpose="fixture.build") as db:
        core.init_schema(db)
        db.execute("UPDATE archive_state SET archive_id='00000000-0000-0000-0000-000000000001',generation=0")
        db.execute("INSERT INTO conversations(id,source,title,created_at,updated_at,model,metadata) VALUES ('c','codex','Synthetic example',?,?,'synthetic-model','{}')", [STAMP, STAMP])
        db.execute("INSERT INTO messages(id,conversation_id,role,content,created_at,model,metadata) VALUES ('m','c','assistant','Synthetic answer.',?,'synthetic-model','{}')", [STAMP])
        db.execute("INSERT INTO file_edits VALUES ('e','m','example.txt','write','beta\n',?,NULL)", [STAMP])
        db.execute("INSERT INTO provenance.file_edit_evidence VALUES ('e','confirmed','synthetic_fixture',NULL)")
        db.execute("INSERT INTO provenance.files VALUES (?,NULL,'example.txt','external')", [data["file"]])
        db.execute("INSERT INTO provenance.file_edit_files VALUES ('e',?,NULL,?,'captured_exact')", [data["file"], protocol.digest(b"alpha\n")])
        db.execute("INSERT INTO provenance.file_edit_scopes(file_edit_id,path) VALUES ('e',?)", [f"external/{protocol.digest('e')[:24]}/unknown"])
        db.execute("INSERT INTO provenance.pending VALUES ('file_edits','e',0)")
        core.project_row_proofs(db, proofs, signer["root_public"], signer["certificate"])
        core._insert_pages(db, "remote.row_conflicts", [(protocol.digest(proof), json.dumps(row, sort_keys=True, separators=(",", ":"))) for row, proof in zip(data["rows"], proofs)])
        db.execute("INSERT INTO remote.local_row_bases VALUES ('edit.observed','e',?,?)", [cfg["user"], proofs[0]["revision"]])
        db.execute("INSERT INTO remote.provenance_origins VALUES ('file.version',?,?,?,?,?)", [data["rows"][2]["id"], WORKSPACE, cfg["user"], data["rows"][2]["id"], protocol.digest(proofs[2])])
        db.execute("CHECKPOINT")
    rows = database_rows(path)
    write_json(directory / "rows.json", rows)
    result = dict(format="convos-b7-minimal-synthetic-v1", schema=12, production_data_used=False,
                  account="synthetic-user", device="synthetic-device", timestamp=STAMP,
                  archive_counts={table: len(rows[f"main.{table}"]["rows"]) if f"main.{table}" in rows else 0 for table in core.ARCHIVE_COLUMNS},
                  table_counts={table: len(value["rows"]) for table, value in rows.items()},
                  sha256={name: file_hash(directory / name) for name in ("convos.db", "rows.json")})
    write_json(directory / "manifest.json", result)
    return result


def scope_case(path: Path) -> dict:
    # Replay the faulty v3 initializer independently of the two signing cases.
    with core.open_db(path, purpose="fixture.scope-migration") as db:
        db.execute("DELETE FROM provenance.file_edit_scopes")
        db.execute("UPDATE core_schema SET version=2")
        core.init_schema(db)
    try:
        records = core.capture_provenance(path)
    except ValueError as error:
        if not str(error).startswith("provenance edit scope conflict: edit=e "):
            raise
        return dict(healthy=False, error=str(error))
    assert any(r["kind"] == "edit.observed" for r in records)
    assert not any(r["kind"] == "edit.observed" for r in core.capture_provenance(path))
    return dict(healthy=True, capture_passes=2, repeat_edit_observations=0)


def attestation_case(path: Path) -> dict:
    data = material()
    try:
        added = projection.attest_rows(path, data["cfg"], WORKSPACE, [data["incoming"]])
    except ValueError as error:
        if str(error) != "row revision conflict: edit.observed:e":
            raise
        return dict(healthy=False, error=str(error))
    repeated = projection.attest_rows(path, data["cfg"], WORKSPACE, [data["incoming"]])
    assert (added, repeated) == (1, 0)
    with core.open_db(path, read_only=True, purpose="fixture.attestation-check") as db:
        previous = db.execute("SELECT previous_revision FROM remote.row_proofs WHERE row_kind='edit.observed' AND content_hash=?", [protocol.digest(protocol.logical_fact(data["incoming"]))]).fetchone()[0]
        assert previous == data["proofs"][0]["revision"]
        assert db.execute("SELECT count(*) FROM remote.row_conflicts WHERE proof_id=?", [protocol.digest(data["proofs"][1])]).fetchone()[0] == 1
    return dict(healthy=True, successor_proofs=added, repeat_proofs=repeated, competing_body_preserved=True)


def retention_case(path: Path) -> dict:
    data = material()
    cfg, signer, old = data["cfg"], data["signer"], data["proofs"][2]
    before = projection.audit_rows(path, local_user=cfg["user"])["totals"]
    assert before["unavailable"] == 0
    row = data["rows"][2]
    successor = row | {"data": row["data"] | {"observed_at": "2000-01-02T00:00:00.000000"}}
    proof = protocol.row_proof(cfg["device"], cfg["user"], WORKSPACE, 1, successor, old["revision"])
    protocol.verify_row_proof(proof, successor, signer["certificate"], signer["root_public"])
    with core.open_db(path, purpose="fixture.retention") as db:
        original = db.execute("SELECT * FROM remote.row_proofs WHERE id=?", [protocol.digest(old)]).fetchone()
        core.project_attested_rows(db, [(successor, proof)], signer["root_public"], signer["certificate"])
        assert db.execute("SELECT * FROM remote.row_proofs WHERE id=?", [protocol.digest(old)]).fetchone() == original
        retained = db.execute("SELECT count(*) FROM remote.row_conflicts WHERE proof_id=?", [protocol.digest(old)]).fetchone()[0]
    after = projection.audit_rows(path, local_user=cfg["user"])["totals"]
    assert (retained, after["unavailable"]) in ((1, 0), (0, 1))
    return dict(healthy=after["unavailable"] == 0, unavailable_before=0, unavailable_after=after["unavailable"], original_proof_unchanged=True, original_body_retained=bool(retained))


def check(directory: Path, expected: str) -> dict:
    manifest = json.loads((directory / "manifest.json").read_text())
    if manifest["format"] != "convos-b7-minimal-synthetic-v1" or manifest["production_data_used"] is not False:
        raise ValueError("Not a synthetic fixture manifest")
    assert all(file_hash(directory / name) == value for name, value in manifest["sha256"].items())
    assert database_rows(directory / "convos.db") == json.loads((directory / "rows.json").read_text())
    data = material()
    for row, proof in zip(data["rows"], data["proofs"]):
        protocol.verify_row_proof(proof, row, data["signer"]["certificate"], data["signer"]["root_public"])
    results = {}
    with tempfile.TemporaryDirectory(prefix="convos-b7-fixture-check-") as temporary:
        for name, operation in (("scope_migration", scope_case), ("edit_attestation", attestation_case), ("retained_history", retention_case)):
            path = Path(temporary) / name / "convos.db"
            path.parent.mkdir()
            shutil.copyfile(directory / "convos.db", path)
            results[name] = operation(path)
    assert all(file_hash(directory / name) == value for name, value in manifest["sha256"].items())
    return dict(expected=expected, cases=results, fixture_unchanged=True, fixture_sha256=manifest["sha256"]["convos.db"],
                success=all(value["healthy"] == (expected == "fixed") for value in results.values()))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("build", "check"))
    parser.add_argument("directory", type=Path, help="New output directory for build, existing fixture for check")
    parser.add_argument("--checkout", type=Path, required=True, help="Convos b7 checkout to exercise, patched or unpatched")
    parser.add_argument("--expect", choices=("fixed", "broken"), default="fixed", help="Check exits nonzero when any case disagrees")
    parser.add_argument("--report", type=Path, help="New JSON result; refuses to overwrite")
    args = parser.parse_args()
    if args.report and args.report.exists():
        raise SystemExit("Report already exists")
    load_code(args.checkout)
    result = build(args.directory.resolve()) if args.action == "build" else check(args.directory.resolve(), args.expect)
    if args.report:
        write_json(args.report, result)
    print(json.dumps(result, indent=2, sort_keys=True))
    if args.action == "check" and not result["success"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
