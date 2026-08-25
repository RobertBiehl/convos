from contextlib import contextmanager,suppress

_BATCH,_MEMORY=50_000,1536*2**20


def _bad(db,sql,message):
    if db.execute(sql).fetchone(): raise ValueError(message)


def _bytes(value): return float(value.split()[0])*{"B":1,"bytes":1,"KiB":2**10,"MiB":2**20,"GiB":2**30,"TiB":2**40}[value.split()[1]]


@contextmanager
def migration_memory(db):
    if _bytes(limit:=db.execute("SELECT current_setting('memory_limit')").fetchone()[0])<=_MEMORY: return (yield)
    db.execute("SET memory_limit=?",[f"{_MEMORY}B"])
    try: yield
    except BaseException:
        with suppress(BaseException): db.execute("SET memory_limit=?",[limit])
        raise
    else: db.execute("SET memory_limit=?",[limit])


def _batches(db,kind,table="_convos_origins",column="table_name"): return range((maximum if (maximum:=db.execute(f"SELECT max(batch) FROM {table} WHERE {column}=?",[kind]).fetchone()[0]) is not None else -1)+1)


def _parts(db,kind): return max(1,(db.execute("SELECT greatest((SELECT count(*) FROM remote.row_origins WHERE table_name=?),(SELECT count(*) FROM remote.row_proofs WHERE row_kind=?))",[kind,kind]).fetchone()[0]+_BATCH-1)//_BATCH)


def _table_parts(db,table): return max(1,(db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]+_BATCH-1)//_BATCH)


def remote_id_migration_scope(db,remote_id=None):
    return {r[0] for r in db.execute("SELECT DISTINCT table_name FROM remote.row_origins WHERE physical_row_id<>substr(sha256(to_json(author_user_id||':'||table_name||':'||source_row_id)),1,16)").fetchall()} if db.execute("SELECT 1 FROM information_schema.tables WHERE table_schema='remote' AND table_name='row_origins'").fetchone() else set()


def _fts_direct(db):
    expected,actual={"dict":(("termid","BIGINT"),("term","VARCHAR"),("df","BIGINT")),"docs":(("docid","BIGINT"),("name","VARCHAR"),("len","BIGINT")),"fields":(("fieldid","BIGINT"),("field","VARCHAR")),"stats":(("num_docs","BIGINT"),("avgdl","DOUBLE")),"stopwords":(("sw","VARCHAR"),),"terms":(("docid","BIGINT"),("fieldid","BIGINT"),("termid","BIGINT"))},(lambda rows:{t:tuple((c,k) for x,c,k in rows if x==t) for t in {x for x,_,_ in rows}})(db.execute("SELECT table_name,column_name,data_type FROM information_schema.columns WHERE table_schema='fts_main_messages' ORDER BY table_name,ordinal_position").fetchall())
    return {k:tuple(v) for k,v in actual.items()}==expected and not db.execute("SELECT 1 FROM (SELECT m.id,count(d.docid) n FROM messages m LEFT JOIN fts_main_messages.docs d ON d.name=m.id GROUP BY m.id HAVING n<>1 UNION ALL SELECT d.name,count(m.id) FROM fts_main_messages.docs d LEFT JOIN messages m ON m.id=d.name GROUP BY d.name HAVING count(m.id)<>1) LIMIT 1").fetchone()


def fts_needs_rebuild(db): return bool(db.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name='fts_main_messages'").fetchone()) and not _fts_direct(db)


def _relation(db,name,id_column,kind):
    target,keys=f"core_remote_{name.replace('.','_')}",','.join('x.'+r[0] for r in db.execute(f"DESCRIBE {name}").fetchall() if r[0]!=id_column and r[3]=='PRI')
    partition=","+keys if keys else ""; parts=_table_parts(db,name); db.execute(f"CREATE OR REPLACE TABLE {target} AS SELECT *,0::UINTEGER _batch FROM {name} WHERE FALSE; CREATE OR REPLACE TEMP TABLE _convos_relation_origins AS SELECT physical_row_id,new_id FROM _convos_origins WHERE table_name='{kind}'; CREATE OR REPLACE TEMP TABLE _convos_relation_rows AS SELECT old_id,winner FROM core_remote_id_rows WHERE kind='{kind}'")
    return ([(db.execute(f"INSERT INTO {target} SELECT x.* REPLACE (COALESCE(o.new_id,x.{id_column}) AS {id_column}),{batch} FROM {name} x LEFT JOIN _convos_relation_origins o ON o.physical_row_id=x.{id_column} WHERE hash(COALESCE(o.new_id,x.{id_column}){partition})%{parts}={batch} QUALIFY row_number() OVER (PARTITION BY COALESCE(o.new_id,x.{id_column}){partition} ORDER BY COALESCE((SELECT winner FROM _convos_relation_rows w WHERE w.old_id=x.{id_column}),FALSE) DESC)=1"),db.execute("COMMIT; BEGIN")) for batch in range(parts)],target)[1]


def migrate_remote_ids(db,archive_columns):
    _bad(db,"SELECT 1 FROM remote.row_origins WHERE author_user_id IS NULL OR source_row_id IS NULL","remote origin identity is incomplete")
    db.execute("DROP TABLE IF EXISTS _convos_heads; DROP TABLE IF EXISTS _convos_origins; DROP TABLE IF EXISTS core_remote_id_rows; CREATE TEMP TABLE _convos_heads(kind VARCHAR,author VARCHAR,source_id VARCHAR,leaf_count UBIGINT,leaf_revision VARCHAR,leaf_state VARCHAR)")
    db.execute("""CREATE TEMP TABLE _convos_origins AS WITH base AS (
      SELECT o.*,substr(sha256(to_json(o.author_user_id||':'||o.table_name||':'||o.source_row_id)),1,16) new_id,p.revision proof_revision,p.content_hash proof_content_hash,h.leaf_count,h.leaf_revision,h.leaf_state
      FROM remote.row_origins o LEFT JOIN remote.row_proofs p ON p.id=o.proof_id LEFT JOIN _convos_heads h ON (h.kind,h.author,h.source_id)=(o.table_name,o.author_user_id,o.source_row_id))
      SELECT *,0::BIGINT origin_rank,0::UINTEGER batch FROM base WHERE FALSE""")
    for table in archive_columns:
        for batch in range(parts:=_parts(db,table)):
            db.execute(f"""INSERT INTO _convos_heads WITH scoped AS (SELECT * FROM remote.row_proofs WHERE row_kind='{table}' AND hash(author_user_id,source_row_id)%{parts}={batch}),leaves AS (SELECT DISTINCT p.row_kind kind,p.author_user_id author,p.source_row_id source_id,p.revision,p.state FROM scoped p WHERE NOT EXISTS (SELECT 1 FROM scoped c WHERE (c.author_user_id,c.source_row_id)=(p.author_user_id,p.source_row_id) AND c.previous_revision=p.revision)) SELECT kind,author,source_id,count(*),min(revision),min(state) FROM leaves GROUP BY kind,author,source_id""")
            db.execute(f"""INSERT INTO _convos_origins WITH base AS (SELECT o.*,substr(sha256(to_json(o.author_user_id||':'||o.table_name||':'||o.source_row_id)),1,16) new_id,p.revision proof_revision,p.content_hash proof_content_hash,h.leaf_count,h.leaf_revision,h.leaf_state FROM remote.row_origins o LEFT JOIN remote.row_proofs p ON p.id=o.proof_id LEFT JOIN _convos_heads h ON (h.kind,h.author,h.source_id)=(o.table_name,o.author_user_id,o.source_row_id) AND hash(h.author,h.source_id)%{parts}={batch} WHERE o.table_name='{table}' AND hash(o.author_user_id,o.source_row_id)%{parts}={batch}) SELECT *,row_number() OVER (PARTITION BY table_name,author_user_id,source_row_id ORDER BY CASE WHEN leaf_count=1 AND proof_revision=leaf_revision THEN 0 ELSE 1 END,CASE WHEN physical_row_id=new_id THEN 0 ELSE 1 END,physical_row_id),{batch}::UINTEGER FROM base; COMMIT; BEGIN""")
    for batch in range(parts:=_table_parts(db,"_convos_origins")): _bad(db,f"SELECT 1 FROM (SELECT table_name,new_id,count(DISTINCT struct_pack(author_user_id,source_row_id)) n FROM _convos_origins WHERE hash(table_name,new_id)%{parts}={batch} GROUP BY table_name,new_id HAVING n>1)","remote physical ID collision")
    db.execute("CREATE TABLE core_remote_id_rows(kind VARCHAR,old_id VARCHAR,new_id VARCHAR,winner BOOLEAN,batch UINTEGER,content_hash VARCHAR)")
    for table in archive_columns:
        for batch in _batches(db,table):
            _bad(db,f"SELECT 1 FROM _convos_origins o JOIN {table} x ON x.id=o.new_id LEFT JOIN _convos_origins owned ON owned.table_name='{table}' AND owned.physical_row_id=x.id WHERE {(scope:=f'''o.table_name='{table}' AND o.batch={batch}''')} AND owned.physical_row_id IS NULL",f"remote {table} ID collides with a local row")
            _bad(db,f"""WITH live AS (SELECT o.* FROM _convos_origins o JOIN {table} x ON x.id=o.physical_row_id WHERE {scope}) SELECT 1 FROM live GROUP BY table_name,author_user_id,source_row_id,leaf_count,leaf_revision,leaf_state HAVING count(*)>1 AND (COALESCE(leaf_count,0)<>1 OR count(*) FILTER (proof_revision=leaf_revision)=0) AND (count(proof_content_hash)<>count(*) OR count(DISTINCT proof_content_hash)<>1)""",f"remote {table} has irreconcilable pre-v2 identities")
            _bad(db,f"""SELECT 1 FROM _convos_origins o JOIN _convos_heads h ON (h.kind,h.author,h.source_id)=(o.table_name,o.author_user_id,o.source_row_id) LEFT JOIN {table} x ON x.id=o.physical_row_id WHERE {scope} GROUP BY o.table_name,o.author_user_id,o.source_row_id,h.leaf_count,h.leaf_state HAVING h.leaf_count=1 AND h.leaf_state='active' AND count(x.id)=0""",f"remote {table} current body is unavailable")
            db.execute(f"""INSERT INTO core_remote_id_rows WITH ranked AS (SELECT o.table_name kind,o.physical_row_id old_id,o.new_id,o.leaf_count,o.leaf_state,o.batch,o.proof_content_hash,row_number() OVER (PARTITION BY o.table_name,o.author_user_id,o.source_row_id ORDER BY CASE WHEN o.leaf_count=1 AND o.proof_revision=o.leaf_revision THEN 0 ELSE 1 END,CASE WHEN o.physical_row_id=o.new_id THEN 0 ELSE 1 END,o.physical_row_id) rn FROM _convos_origins o JOIN {table} x ON x.id=o.physical_row_id WHERE {scope}) SELECT kind,old_id,new_id,NOT COALESCE(leaf_count=1 AND leaf_state='deleted',FALSE) AND rn=1,batch,proof_content_hash FROM ranked; COMMIT; BEGIN""")
    changed,has_fts={r[0] for r in db.execute("SELECT DISTINCT table_name FROM _convos_origins WHERE physical_row_id<>new_id").fetchall()},bool(db.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name='fts_main_messages'").fetchone())
    direct,rebuild=(direct:=has_fts and "messages" in changed and not fts_needs_rebuild(db)),has_fts and "messages" in changed and not direct
    db.execute("CREATE OR REPLACE TABLE core_remote_origin_keep AS SELECT table_name,new_id physical_row_id,workspace_id,author_user_id,author_device_id,source_row_id,source_event_id,content_key,observed_at,proof_id,0::UINTEGER batch FROM _convos_origins WHERE FALSE")
    for table,batch in ((table,batch) for table in archive_columns for batch in _batches(db,table)): db.execute(f"""INSERT INTO core_remote_origin_keep WITH ranked AS (SELECT o.*,row_number() OVER (PARTITION BY o.table_name,o.author_user_id,o.source_row_id ORDER BY CASE WHEN r.winner THEN 0 ELSE 1 END,o.origin_rank) rn FROM _convos_origins o LEFT JOIN core_remote_id_rows r ON r.kind=o.table_name AND r.old_id=o.physical_row_id WHERE o.table_name='{table}' AND o.batch={batch}) SELECT table_name,new_id,workspace_id,author_user_id,author_device_id,source_row_id,source_event_id,content_key,observed_at,proof_id,{batch} FROM ranked WHERE rn=1; COMMIT; BEGIN""")
    [_relation(db,*relation) for relation in (("attachment_bodies","attachment_id","attachments"),("provenance.file_edit_files","file_edit_id","file_edits"),("provenance.checkpoint_edits","file_edit_id","file_edits"))]
    db.execute("CREATE TEMP TABLE _convos_links AS SELECT sha256(json_object('checkpoint',checkpoint_id,'edit',file_edit_id)) old_id,sha256(json_object('checkpoint',checkpoint_id,'edit',COALESCE(o.new_id,file_edit_id))) new_id FROM provenance.checkpoint_edits c LEFT JOIN _convos_origins o ON o.table_name='file_edits' AND o.physical_row_id=c.file_edit_id")
    _bad(db,"SELECT 1 FROM remote.provenance_origins p LEFT JOIN _convos_links l ON l.old_id=p.physical_entity WHERE p.kind='checkpoint.link' AND l.old_id IS NULL","remote checkpoint link body is unavailable")
    db.execute("CREATE OR REPLACE TABLE core_remote_provenance AS SELECT *,0::UINTEGER batch FROM remote.provenance_origins WHERE FALSE")
    for batch in range(parts:=_table_parts(db,"remote.provenance_origins")): db.execute(f"""INSERT INTO core_remote_provenance WITH mapped AS (SELECT p.*,CASE WHEN p.kind='edit.observed' THEN substr(sha256(to_json(p.author_user_id||':file_edits:'||p.source_entity)),1,16) WHEN p.kind='checkpoint.link' THEN l.new_id ELSE p.physical_entity END new_entity FROM remote.provenance_origins p LEFT JOIN _convos_links l ON l.old_id=p.physical_entity),ranked AS (SELECT *,row_number() OVER (PARTITION BY kind,new_entity,workspace_id,author_user_id ORDER BY CASE WHEN physical_entity=new_entity THEN 0 ELSE 1 END,physical_entity) rn FROM mapped WHERE hash(kind,new_entity,workspace_id,author_user_id)%{parts}={batch}) SELECT kind,new_entity,workspace_id,author_user_id,source_entity,proof_id,{batch} FROM ranked WHERE rn=1; COMMIT; BEGIN""")
    db.execute(f"CREATE OR REPLACE TABLE core_remote_id_map AS SELECT table_name kind,physical_row_id old_id,new_id,batch FROM _convos_origins UNION ALL SELECT p.kind,p.physical_entity,n.physical_entity,0 FROM remote.provenance_origins p JOIN core_remote_provenance n USING(kind,workspace_id,author_user_id,source_entity); INSERT OR REPLACE INTO core_migrations VALUES ('remote_ids','{'data_direct' if direct else 'data'}'); COMMIT; BEGIN")
    migrate_remote_data(db,archive_columns,direct)
    return changed,rebuild


def migrate_remote_data(db,archive_columns,direct=False):
    if direct:
        for batch in _batches(db,"messages","core_remote_id_rows","kind"): db.execute(f"DELETE FROM fts_main_messages.terms USING fts_main_messages.docs d,core_remote_id_rows r WHERE terms.docid=d.docid AND r.kind='messages' AND r.batch={batch} AND r.old_id=d.name AND NOT r.winner; DELETE FROM fts_main_messages.docs USING core_remote_id_rows r WHERE r.kind='messages' AND r.batch={batch} AND r.old_id=docs.name AND NOT r.winner; UPDATE fts_main_messages.docs SET name=r.new_id FROM core_remote_id_rows r WHERE r.kind='messages' AND r.batch={batch} AND r.winner AND docs.name=r.old_id; COMMIT; BEGIN")
        if db.execute("SELECT 1 FROM core_remote_id_rows WHERE kind='messages' AND NOT winner").fetchone(): db.execute("UPDATE fts_main_messages.dict d SET df=COALESCE(x.df,0) FROM (SELECT d.termid,count(DISTINCT t.docid) df FROM fts_main_messages.dict d LEFT JOIN fts_main_messages.terms t ON t.termid=d.termid GROUP BY d.termid) x WHERE x.termid=d.termid; UPDATE fts_main_messages.stats SET num_docs=(SELECT count(*) FROM fts_main_messages.docs),avgdl=COALESCE((SELECT avg(len) FROM fts_main_messages.docs),0); COMMIT; BEGIN")
    fks={"messages":(("conversation_id","conversations"),("parent_id","messages")),"tool_calls":(("message_id","messages"),),"attachments":(("message_id","messages"),),"artifacts":(("conversation_id","conversations"),),"file_edits":(("message_id","messages"),)}
    for table,column,parent,batch in ((table,column,parent,batch) for table,parents in fks.items() for column,parent in parents for batch in _batches(db,parent,"core_remote_id_map","kind")): db.execute(f"UPDATE {table} x SET {column}=o.new_id FROM core_remote_id_map o WHERE o.kind='{parent}' AND o.batch={batch} AND x.{column}=o.old_id AND o.old_id<>o.new_id; COMMIT; BEGIN")
    if db.execute("SELECT 1 FROM core_remote_id_rows WHERE kind='messages' AND NOT winner").fetchone():
        for batch in _batches(db,"messages","core_remote_id_rows","kind"): db.execute(f"""UPDATE messages w SET embedding=l.embedding FROM core_remote_id_rows rw,core_remote_id_rows rl,messages l WHERE rw.kind='messages' AND rw.batch={batch} AND rw.winner AND rl.kind='messages' AND NOT rl.winner AND rw.new_id=rl.new_id AND w.id=rw.old_id AND l.id=rl.old_id AND w.embedding IS NULL AND l.embedding IS NOT NULL AND rw.content_hash=rl.content_hash; COMMIT; BEGIN""")
    for table,batch in ((table,batch) for table in archive_columns for batch in _batches(db,table,"core_remote_id_rows","kind")): db.execute(f"DELETE FROM {table} USING core_remote_id_rows r WHERE r.kind='{table}' AND r.batch={batch} AND NOT r.winner AND id=r.old_id; UPDATE {table} SET id=r.new_id FROM core_remote_id_rows r WHERE r.kind='{table}' AND r.batch={batch} AND r.winner AND id=r.old_id AND r.old_id<>r.new_id; COMMIT; BEGIN")
    for name,target in (("attachment_bodies","core_remote_attachment_bodies"),("provenance.file_edit_files","core_remote_provenance_file_edit_files"),("provenance.checkpoint_edits","core_remote_provenance_checkpoint_edits")):
        db.execute(f"DELETE FROM {name}; COMMIT; BEGIN")
        for batch in range(db.execute(f"SELECT coalesce(max(_batch),-1)+1 FROM {target}").fetchone()[0]): db.execute(f"INSERT INTO {name} SELECT * EXCLUDE(_batch) FROM {target} WHERE _batch={batch}; COMMIT; BEGIN")
    db.execute("DELETE FROM remote.row_origins; COMMIT; BEGIN")
    for table,batch in ((table,batch) for table in archive_columns for batch in _batches(db,table,"core_remote_origin_keep")): db.execute(f"INSERT INTO remote.row_origins SELECT * EXCLUDE(batch) FROM core_remote_origin_keep WHERE table_name='{table}' AND batch={batch}; COMMIT; BEGIN")
    db.execute("DELETE FROM remote.provenance_origins; COMMIT; BEGIN")
    for batch in range(db.execute("SELECT coalesce(max(batch),-1)+1 FROM core_remote_provenance").fetchone()[0]): db.execute(f"INSERT INTO remote.provenance_origins SELECT * EXCLUDE(batch) FROM core_remote_provenance WHERE batch={batch}; COMMIT; BEGIN")
    db.execute("DROP TABLE core_remote_id_rows; DROP TABLE core_remote_origin_keep; DROP TABLE core_remote_attachment_bodies; DROP TABLE core_remote_provenance_file_edit_files; DROP TABLE core_remote_provenance_checkpoint_edits; DROP TABLE core_remote_provenance; INSERT OR REPLACE INTO core_migrations VALUES ('remote_ids','changes'); COMMIT; BEGIN")
    migrate_remote_changes(db)


def migrate_remote_changes(db):
    generation=db.execute("UPDATE archive_state SET generation=generation+1 WHERE singleton RETURNING generation").fetchone()[0]
    for batch in range(parts:=_table_parts(db,"core_remote_id_map")):
        db.execute(f"""INSERT INTO archive_changes SELECT m.kind,m.new_id,max(c.generation) FROM core_remote_id_map m JOIN archive_changes c ON (c.kind,c.entity)=(m.kind,m.old_id) WHERE {(scope:=f'm.old_id<>m.new_id AND hash(m.kind,m.old_id)%{parts}={batch}')} GROUP BY m.kind,m.new_id ON CONFLICT(kind,entity) DO UPDATE SET generation=greatest(archive_changes.generation,excluded.generation)""")
        db.execute(f"DELETE FROM archive_changes c USING core_remote_id_map m WHERE (c.kind,c.entity)=(m.kind,m.old_id) AND {scope}")
        db.execute(f"INSERT OR REPLACE INTO archive_changes SELECT DISTINCT m.kind,m.new_id,? FROM core_remote_id_map m WHERE {scope}",[generation])
        if batch+1<parts: db.execute("COMMIT; BEGIN")
    db.execute("DROP TABLE core_remote_id_map")
