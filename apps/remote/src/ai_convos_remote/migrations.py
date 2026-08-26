import sqlite3

PRESERVE={"outbox","receipts","publication_heads","lazy_events","deferred_events","event_sequences","sequence_gaps","replica_receipts","blob_outbox","blob_receipts","origin_bindings","control_dependencies","sync_states","meta"}

def migrate_state(path,new,version):
    if version not in ("1","2","3"): return False
    old=sqlite3.connect(path)
    tables={r[0] for r in old.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    schema=lambda db,table:[tuple(r[1:4])+tuple(r[5:6]) for r in db.execute(f"PRAGMA table_info({table})")]
    compatible=PRESERVE<=tables and all(schema(old,table)==schema(new,table) for table in PRESERVE)
    old.close()
    if not compatible: return False
    new.execute("ATTACH DATABASE ? AS legacy",(str(path),))
    for table in PRESERVE-{"meta"}: new.execute(f"INSERT INTO {table} SELECT * FROM legacy.{table}")
    new.execute("INSERT INTO meta SELECT key,value FROM legacy.meta WHERE key NOT IN ('state_schema','state_cutover','last_sync') AND key NOT LIKE 'core_generation:%'")
    new.commit()
    new.execute("DETACH DATABASE legacy")
    return True
