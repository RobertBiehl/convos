def _bad(db,sql,message):
    if db.execute(sql).fetchone(): raise ValueError(message)


def remote_id_migration_scope(db,remote_id=None):
    if not db.execute("SELECT 1 FROM information_schema.tables WHERE table_schema='remote' AND table_name='row_origins'").fetchone(): return set()
    return {r[0] for r in db.execute("SELECT DISTINCT table_name FROM remote.row_origins WHERE physical_row_id<>substr(sha256(to_json(author_user_id||':'||table_name||':'||source_row_id)),1,16)").fetchall()}


def _fts_direct(db):
    expected={"dict":(("termid","BIGINT"),("term","VARCHAR"),("df","BIGINT")),"docs":(("docid","BIGINT"),("name","VARCHAR"),("len","BIGINT")),"fields":(("fieldid","BIGINT"),("field","VARCHAR")),"stats":(("num_docs","BIGINT"),("avgdl","DOUBLE")),"stopwords":(("sw","VARCHAR"),),"terms":(("docid","BIGINT"),("fieldid","BIGINT"),("termid","BIGINT"))}
    actual={}
    [actual.setdefault(t,[]).append((c,k)) for t,c,k in db.execute("SELECT table_name,column_name,data_type FROM information_schema.columns WHERE table_schema='fts_main_messages' ORDER BY table_name,ordinal_position").fetchall()]
    if {k:tuple(v) for k,v in actual.items()}!=expected: return False
    return not db.execute("SELECT 1 FROM (SELECT m.id,count(d.docid) n FROM messages m LEFT JOIN fts_main_messages.docs d ON d.name=m.id GROUP BY m.id HAVING n<>1 UNION ALL SELECT d.name,count(m.id) FROM fts_main_messages.docs d LEFT JOIN messages m ON m.id=d.name GROUP BY d.name HAVING count(m.id)<>1) LIMIT 1").fetchone()


def fts_needs_rebuild(db): return bool(db.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name='fts_main_messages'").fetchone()) and not _fts_direct(db)


def _relation(db,name,id_column,kind):
    target=f"_convos_{name.replace('.','_')}"
    keys=','.join('x.'+r[0] for r in db.execute(f"DESCRIBE {name}").fetchall() if r[0]!=id_column and r[3]=='PRI')
    partition=","+keys if keys else ""
    db.execute(f"CREATE OR REPLACE TEMP TABLE {target} AS SELECT x.* REPLACE (COALESCE(o.new_id,x.{id_column}) AS {id_column}) FROM {name} x LEFT JOIN _convos_origins o ON o.table_name='{kind}' AND o.physical_row_id=x.{id_column} QUALIFY row_number() OVER (PARTITION BY COALESCE(o.new_id,x.{id_column}){partition} ORDER BY COALESCE((SELECT winner FROM _convos_rows w WHERE w.kind='{kind}' AND w.old_id=x.{id_column}),FALSE) DESC)=1")


def migrate_remote_ids(db,archive_columns):
    _bad(db,"SELECT 1 FROM remote.row_origins WHERE author_user_id IS NULL OR source_row_id IS NULL","remote origin identity is incomplete")
    db.execute("DROP TABLE IF EXISTS _convos_heads; DROP TABLE IF EXISTS _convos_origins; DROP TABLE IF EXISTS _convos_rows")
    db.execute("""CREATE TEMP TABLE _convos_heads AS WITH leaves AS (
      SELECT DISTINCT p.row_kind kind,p.author_user_id author,p.source_row_id source_id,p.revision,p.state
      FROM remote.row_proofs p WHERE NOT EXISTS (SELECT 1 FROM remote.row_proofs c WHERE (c.row_kind,c.author_user_id,c.source_row_id)=(p.row_kind,p.author_user_id,p.source_row_id) AND c.previous_revision=p.revision))
      SELECT kind,author,source_id,count(*) leaf_count,min(revision) leaf_revision,min(state) leaf_state FROM leaves GROUP BY kind,author,source_id""")
    db.execute("""CREATE TEMP TABLE _convos_origins AS WITH base AS (
      SELECT o.*,substr(sha256(to_json(o.author_user_id||':'||o.table_name||':'||o.source_row_id)),1,16) new_id,p.revision proof_revision,p.content_hash proof_content_hash,h.leaf_count,h.leaf_revision,h.leaf_state
      FROM remote.row_origins o LEFT JOIN remote.row_proofs p ON p.id=o.proof_id LEFT JOIN _convos_heads h ON (h.kind,h.author,h.source_id)=(o.table_name,o.author_user_id,o.source_row_id))
      SELECT *,row_number() OVER (PARTITION BY table_name,author_user_id,source_row_id ORDER BY CASE WHEN leaf_count=1 AND proof_revision=leaf_revision THEN 0 ELSE 1 END,CASE WHEN physical_row_id=new_id THEN 0 ELSE 1 END,physical_row_id) origin_rank FROM base""")
    _bad(db,"SELECT 1 FROM (SELECT table_name,new_id,count(DISTINCT struct_pack(author_user_id,source_row_id)) n FROM _convos_origins GROUP BY table_name,new_id HAVING n>1)","remote physical ID collision")
    db.execute("CREATE TEMP TABLE _convos_rows(kind VARCHAR,old_id VARCHAR,new_id VARCHAR,winner BOOLEAN)")
    for table in archive_columns:
        _bad(db,f"SELECT 1 FROM _convos_origins o JOIN {table} x ON x.id=o.new_id LEFT JOIN _convos_origins owned ON owned.table_name='{table}' AND owned.physical_row_id=x.id WHERE o.table_name='{table}' AND owned.physical_row_id IS NULL",f"remote {table} ID collides with a local row")
        _bad(db,f"""WITH live AS (SELECT o.* FROM _convos_origins o JOIN {table} x ON x.id=o.physical_row_id WHERE o.table_name='{table}') SELECT 1 FROM live GROUP BY table_name,author_user_id,source_row_id,leaf_count,leaf_revision,leaf_state HAVING count(*)>1 AND (COALESCE(leaf_count,0)<>1 OR count(*) FILTER (proof_revision=leaf_revision)=0) AND (count(proof_content_hash)<>count(*) OR count(DISTINCT proof_content_hash)<>1)""",f"remote {table} has irreconcilable pre-v2 identities")
        _bad(db,f"""SELECT 1 FROM _convos_origins o JOIN _convos_heads h ON (h.kind,h.author,h.source_id)=(o.table_name,o.author_user_id,o.source_row_id) LEFT JOIN {table} x ON x.id=o.physical_row_id WHERE o.table_name='{table}' AND h.leaf_count=1 AND h.leaf_state='active' GROUP BY o.table_name,o.author_user_id,o.source_row_id HAVING count(x.id)=0""",f"remote {table} current body is unavailable")
        db.execute(f"""INSERT INTO _convos_rows WITH ranked AS (SELECT o.table_name kind,o.physical_row_id old_id,o.new_id,o.leaf_count,o.leaf_state,row_number() OVER (PARTITION BY o.table_name,o.author_user_id,o.source_row_id ORDER BY CASE WHEN o.leaf_count=1 AND o.proof_revision=o.leaf_revision THEN 0 ELSE 1 END,CASE WHEN o.physical_row_id=o.new_id THEN 0 ELSE 1 END,o.physical_row_id) rn FROM _convos_origins o JOIN {table} x ON x.id=o.physical_row_id WHERE o.table_name='{table}') SELECT kind,old_id,new_id,NOT COALESCE(leaf_count=1 AND leaf_state='deleted',FALSE) AND rn=1 FROM ranked""")
    changed={r[0] for r in db.execute("SELECT DISTINCT table_name FROM _convos_origins WHERE physical_row_id<>new_id").fetchall()}
    has_fts=bool(db.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name='fts_main_messages'").fetchone())
    direct=has_fts and "messages" in changed and not fts_needs_rebuild(db)
    rebuild=has_fts and "messages" in changed and not direct
    if direct:
        db.execute("DELETE FROM fts_main_messages.terms USING fts_main_messages.docs d,_convos_rows r WHERE terms.docid=d.docid AND r.kind='messages' AND r.old_id=d.name AND NOT r.winner; DELETE FROM fts_main_messages.docs USING _convos_rows r WHERE r.kind='messages' AND r.old_id=docs.name AND NOT r.winner; UPDATE fts_main_messages.docs SET name=r.new_id FROM _convos_rows r WHERE r.kind='messages' AND r.winner AND docs.name=r.old_id")
        if db.execute("SELECT 1 FROM _convos_rows WHERE kind='messages' AND NOT winner").fetchone(): db.execute("UPDATE fts_main_messages.dict d SET df=COALESCE(x.df,0) FROM (SELECT d.termid,count(DISTINCT t.docid) df FROM fts_main_messages.dict d LEFT JOIN fts_main_messages.terms t ON t.termid=d.termid GROUP BY d.termid) x WHERE x.termid=d.termid; UPDATE fts_main_messages.stats SET num_docs=(SELECT count(*) FROM fts_main_messages.docs),avgdl=COALESCE((SELECT avg(len) FROM fts_main_messages.docs),0)")
    db.execute("""CREATE TEMP TABLE _convos_origin_keep AS WITH ranked AS (SELECT o.*,row_number() OVER (PARTITION BY o.table_name,o.author_user_id,o.source_row_id ORDER BY CASE WHEN r.winner THEN 0 ELSE 1 END,o.origin_rank) rn FROM _convos_origins o LEFT JOIN _convos_rows r ON r.kind=o.table_name AND r.old_id=o.physical_row_id) SELECT table_name,new_id physical_row_id,workspace_id,author_user_id,author_device_id,source_row_id,source_event_id,content_key,observed_at,proof_id FROM ranked WHERE rn=1""")
    _relation(db,"attachment_bodies","attachment_id","attachments")
    _relation(db,"provenance.file_edit_files","file_edit_id","file_edits")
    _relation(db,"provenance.checkpoint_edits","file_edit_id","file_edits")
    db.execute("CREATE TEMP TABLE _convos_links AS SELECT sha256(json_object('checkpoint',checkpoint_id,'edit',file_edit_id)) old_id,sha256(json_object('checkpoint',checkpoint_id,'edit',COALESCE(o.new_id,file_edit_id))) new_id FROM provenance.checkpoint_edits c LEFT JOIN _convos_origins o ON o.table_name='file_edits' AND o.physical_row_id=c.file_edit_id")
    _bad(db,"SELECT 1 FROM remote.provenance_origins p LEFT JOIN _convos_links l ON l.old_id=p.physical_entity WHERE p.kind='checkpoint.link' AND l.old_id IS NULL","remote checkpoint link body is unavailable")
    db.execute("""CREATE TEMP TABLE _convos_provenance AS WITH mapped AS (SELECT p.*,CASE WHEN p.kind='edit.observed' THEN substr(sha256(to_json(p.author_user_id||':file_edits:'||p.source_entity)),1,16) WHEN p.kind='checkpoint.link' THEN l.new_id ELSE p.physical_entity END new_entity FROM remote.provenance_origins p LEFT JOIN _convos_links l ON l.old_id=p.physical_entity),ranked AS (SELECT *,row_number() OVER (PARTITION BY kind,new_entity,workspace_id,author_user_id ORDER BY CASE WHEN physical_entity=new_entity THEN 0 ELSE 1 END,physical_entity) rn FROM mapped) SELECT kind,new_entity physical_entity,workspace_id,author_user_id,source_entity,proof_id FROM ranked WHERE rn=1""")
    db.execute("CREATE TEMP TABLE _convos_entity_map AS SELECT DISTINCT table_name kind,physical_row_id old_id,new_id FROM _convos_origins UNION SELECT DISTINCT p.kind,p.physical_entity,n.physical_entity FROM remote.provenance_origins p JOIN _convos_provenance n USING(kind,workspace_id,author_user_id,source_entity); CREATE TEMP TABLE _convos_changes AS SELECT c.kind,COALESCE(m.new_id,c.entity) entity,max(c.generation) generation FROM archive_changes c LEFT JOIN _convos_entity_map m ON (m.kind,m.old_id)=(c.kind,c.entity) GROUP BY c.kind,COALESCE(m.new_id,c.entity)")
    fks={"messages":(("conversation_id","conversations"),("parent_id","messages")),"tool_calls":(("message_id","messages"),),"attachments":(("message_id","messages"),),"artifacts":(("conversation_id","conversations"),),"file_edits":(("message_id","messages"),)}
    for table,parents in fks.items():
        for column,parent in parents: db.execute(f"UPDATE {table} x SET {column}=o.new_id FROM _convos_origins o WHERE o.table_name='{parent}' AND x.{column}=o.physical_row_id AND o.physical_row_id<>o.new_id")
    db.execute("""UPDATE messages w SET embedding=l.embedding FROM _convos_rows rw,_convos_rows rl,messages l,_convos_origins ow,_convos_origins ol WHERE rw.kind='messages' AND rw.winner AND rl.kind='messages' AND NOT rl.winner AND rw.new_id=rl.new_id AND w.id=rw.old_id AND l.id=rl.old_id AND w.embedding IS NULL AND l.embedding IS NOT NULL AND ow.table_name='messages' AND ow.physical_row_id=rw.old_id AND ol.table_name='messages' AND ol.physical_row_id=rl.old_id AND ow.proof_content_hash=ol.proof_content_hash""")
    for table in archive_columns: db.execute(f"DELETE FROM {table} USING _convos_rows r WHERE r.kind='{table}' AND NOT r.winner AND id=r.old_id; UPDATE {table} SET id=r.new_id FROM _convos_rows r WHERE r.kind='{table}' AND r.winner AND id=r.old_id AND r.old_id<>r.new_id")
    for name in ("attachment_bodies","provenance.file_edit_files","provenance.checkpoint_edits"):
        target=f"_convos_{name.replace('.','_')}"
        db.execute(f"DELETE FROM {name}; INSERT INTO {name} SELECT * FROM {target}")
    db.execute("DELETE FROM remote.row_origins; INSERT INTO remote.row_origins SELECT * FROM _convos_origin_keep; DELETE FROM remote.provenance_origins; INSERT INTO remote.provenance_origins SELECT * FROM _convos_provenance")
    generation=db.execute("UPDATE archive_state SET generation=generation+1 WHERE singleton RETURNING generation").fetchone()[0]
    db.execute("DELETE FROM archive_changes; INSERT INTO archive_changes SELECT * FROM _convos_changes; INSERT OR REPLACE INTO archive_changes SELECT DISTINCT kind,new_id,? FROM _convos_entity_map WHERE old_id<>new_id",[generation])
    return changed,rebuild
