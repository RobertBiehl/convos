import json


def remote_id_migration_scope(db,remote_id):
    if not db.execute("SELECT 1 FROM information_schema.tables WHERE table_schema='remote' AND table_name='row_origins'").fetchone(): return set()
    return {table for table,physical,author,source in db.execute("SELECT table_name,physical_row_id,author_user_id,source_row_id FROM remote.row_origins").fetchall() if physical!=remote_id(author,table,source)}


def migrate_remote_ids(db,archive_columns,digest,remote_id):
    fields=("table_name","physical_row_id","workspace_id","author_user_id","author_device_id","source_row_id","source_event_id","content_key","observed_at","proof_id")
    origins=[dict(zip(fields,r)) for r in db.execute(f"SELECT {','.join(fields)} FROM remote.row_origins").fetchall()]
    if not origins: return set()
    groups={}
    [groups.setdefault((o["table_name"],o["author_user_id"],o["source_row_id"]),[]).append(o) for o in origins]
    targets={key:remote_id(key[1],key[0],key[2]) for key in groups}
    if len({(key[0],target) for key,target in targets.items()})!=len(targets): raise ValueError("remote physical ID collision")
    mappings={}
    for key,values in groups.items():
        for origin in values:
            if mappings.setdefault((key[0],origin["physical_row_id"]),targets[key])!=targets[key]: raise ValueError("remote physical ID collision")
    proof_rows=db.execute("SELECT id,row_kind,source_row_id,author_user_id,revision,previous_revision,state FROM remote.row_proofs").fetchall()
    proofs={r[0]:r[4:] for r in proof_rows}
    chains={}
    for r in proof_rows: chains.setdefault((r[1],r[3],r[2]),{})[r[4]]=(r[5],r[6])
    leaves={key:next(iter(heads)) for key,nodes in chains.items() if len(heads:=set(nodes)-{v[0] for v in nodes.values() if v[0]})==1}
    selected,changed=[],[]
    fks={"messages":(("conversation_id","conversations"),("parent_id","messages")),"tool_calls":(("message_id","messages"),),"attachments":(("message_id","messages"),),"artifacts":(("conversation_id","conversations"),),"file_edits":(("message_id","messages"),)}
    for table in archive_columns:
        related={key:values for key,values in groups.items() if key[0]==table}
        ids={o["physical_row_id"] for values in related.values() for o in values}
        cur=db.execute(f"SELECT * FROM {table} WHERE id IN (SELECT UNNEST(?))",[list(ids)])
        columns=[d[0] for d in cur.description]
        rows={r[0]:list(r) for r in cur.fetchall()}
        occupied={r[0] for r in db.execute(f"SELECT id FROM {table} WHERE id IN (SELECT UNNEST(?))",[list({targets[k] for k in related})]).fetchall()}-ids
        if occupied: raise ValueError(f"remote {table} ID collides with a local row")
        output=[]
        for key,values in related.items():
            target=targets[key]
            live=[o for o in values if o["physical_row_id"] in rows]
            leaf=leaves.get(key)
            leaf_state=chains.get(key,{}).get(leaf,(None,None))[1]
            normalized=[]
            for origin in live:
                row=rows[origin["physical_row_id"]].copy()
                row[columns.index("id")]=target
                [row.__setitem__(columns.index(column),mappings.get((parent,row[columns.index(column)]),row[columns.index(column)])) for column,parent in fks.get(table,()) if row[columns.index(column)] is not None]
                normalized.append((origin,row))
            comparable=lambda row:json.dumps([v for c,v in zip(columns,row) if c!="embedding"],sort_keys=True,default=str)
            equal=len({comparable(row) for origin,row in normalized})<=1
            current=[o for o in live if leaf and proofs.get(o["proof_id"],(None,))[0]==leaf]
            if not leaf and not equal: raise ValueError(f"remote {table} has irreconcilable pre-v2 identities")
            winner=None if leaf_state=="deleted" else next((o for o in current if o["physical_row_id"]==target),None) or (current[0] if current else next((o for o in live if o["physical_row_id"]==target),None))
            if live and winner is None and leaf_state!="deleted":
                if not equal: raise ValueError(f"remote {table} has irreconcilable pre-v2 identities")
                winner=live[0]
            if leaf and leaf_state=="active" and not winner: raise ValueError(f"remote {table} current body is unavailable")
            if winner:
                row=next(row for origin,row in normalized if origin is winner)
                if table=="messages" and equal and row[columns.index("embedding")] is None: row[columns.index("embedding")]=next((r[columns.index("embedding")] for origin,r in normalized if r[columns.index("embedding")] is not None),None)
                output.append(row)
            chosen=winner or next((o for o in values if leaf and proofs.get(o["proof_id"],(None,))[0]==leaf),None) or next((o for o in values if o["physical_row_id"]==target),None) or values[0]
            selected.append({**chosen,"physical_row_id":target})
            if any(o["physical_row_id"]!=target for o in values): changed.append((table,target))
        if ids: db.execute(f"DELETE FROM {table} WHERE id IN (SELECT UNNEST(?))",[list(ids)])
        if output: db.executemany(f"INSERT INTO {table} VALUES ({','.join('?'*len(columns))})",output)
    def relation(name,transform,key):
        rows=[transform(list(r)) for r in db.execute(f"SELECT * FROM {name}").fetchall()]
        unique={key(r):r for r in rows}
        db.execute(f"DELETE FROM {name}")
        if rows: db.executemany(f"INSERT INTO {name} VALUES ({','.join('?'*len(rows[0]))})",unique.values())
        return rows
    relation("attachment_bodies",lambda r:[mappings.get(("attachments",r[0]),r[0]),*r[1:]],lambda r:r[0])
    relation("provenance.file_edit_files",lambda r:[mappings.get(("file_edits",r[0]),r[0]),*r[1:]],lambda r:tuple(r[:2]))
    old_checkpoints=db.execute("SELECT * FROM provenance.checkpoint_edits").fetchall()
    checkpoints=relation("provenance.checkpoint_edits",lambda r:[r[0],mappings.get(("file_edits",r[1]),r[1]),*r[2:]],lambda r:tuple(r[:2]))
    links={digest({"checkpoint":old[0],"edit":old[1]}):digest({"checkpoint":new[0],"edit":new[1]}) for old,new in zip(old_checkpoints,checkpoints)}
    provenance,provmap={},{}
    for raw in db.execute("SELECT * FROM remote.provenance_origins").fetchall():
        row=list(raw)
        if row[0]=="checkpoint.link" and row[1] not in links: raise ValueError("remote checkpoint link body is unavailable")
        row[1]=remote_id(row[3],"file_edits",row[4]) if row[0]=="edit.observed" else links.get(row[1],row[1]) if row[0]=="checkpoint.link" else row[1]
        key=(row[0],row[1],row[2],row[3])
        if key not in provenance or raw[1]==row[1]: provenance[key]=row
        provmap[(row[0],raw[1])]=row[1]
        if row[1]!=raw[1]: changed.append((row[0],row[1]))
    db.execute("DELETE FROM remote.row_origins; DELETE FROM remote.provenance_origins")
    if selected: db.executemany("INSERT INTO remote.row_origins VALUES (?,?,?,?,?,?,?,?,?,?)",[tuple(o[f] for f in fields) for o in selected])
    if provenance: db.executemany("INSERT INTO remote.provenance_origins VALUES (?,?,?,?,?,?)",provenance.values())
    changes=[(kind,mappings.get((kind,entity),provmap.get((kind,entity),entity)),generation) for kind,entity,generation in db.execute("SELECT * FROM archive_changes").fetchall()]
    generation=db.execute("UPDATE archive_state SET generation=generation+1 WHERE singleton RETURNING generation").fetchone()[0]
    db.execute("DELETE FROM archive_changes")
    merged={(kind,entity):(kind,entity,g) for kind,entity,g in changes}
    merged.update({(kind,entity):(kind,entity,generation) for kind,entity in changed})
    if merged: db.executemany("INSERT INTO archive_changes VALUES (?,?,?)",merged.values())
    return {kind for kind,entity in changed if kind in archive_columns}
