#!/usr/bin/env python3
"""Bounded synthetic relay timings; optional --baseline loads an older server source file."""
import argparse, concurrent.futures, hashlib, http.client, importlib.util, json, sqlite3, statistics, tempfile, threading, time, tracemalloc
from contextlib import closing
from pathlib import Path

import ai_convos_remote_server as current
from ai_convos_remote.control import record, sign
from ai_convos_remote.protocol import certificate, digest, event, identity, registration_proof, seal_event, seal_key, sign_control


def measure(fn,count):
    values=[]
    for i in range(count):
        started=time.perf_counter()
        fn(i)
        values.append(time.perf_counter()-started)
    return distribution(values)
def distribution(values): return {"calls":len(values),"median_ms":round(1000*statistics.median(values),4),"p95_ms":round(1000*sorted(values)[int(.95*(len(values)-1))],4)}
def create(module,db,actor,ws):
    device,root,key=actor["device"],actor["root"],bytes(32)
    control=sign(device,{"v":1,"kind":"workspace.state","workspace":ws,"scope":"personal","revision":1,"prev":None,"epoch":1,"boundary":{"epoch":1,"tail":0,"heads":{}},"key_commitment":digest(key),"members":{root["id"]:{"role":"admin","joined":1,"history_from":1}},"devices":{device["id"]:record(root["id"],root["sign_public"],device,actor["certificate"])},"removed":[],"action":"create","approval":None,"approved_at":0})
    module.action(db,sign_control(device,{"op":"create","workspace":ws,"kind":"personal","control":control,"envelopes":{device["id"]:seal_key(key,device["box_public"],f"workspace:{ws}:epoch:1")}}),actor["token"])
def fixture(module,db):
    root,device=identity("benchmark root"),identity("benchmark device")
    cert=certificate(root,root["id"],device)
    challenge=module.action(db,{"op":"register_challenge","root_public":root["sign_public"],"certificate":cert})["challenge"]
    registered=module.action(db,{"op":"register","user_name":"benchmark","root_public":root["sign_public"],"certificate":cert,"challenge":challenge,"proof":registration_proof(device,challenge,root["sign_public"],cert)})
    actor={"root":root,"device":device,"certificate":cert,"token":registered["token"]}
    for ws in ("ledger","large"): create(module,db,actor,ws)
    return actor
def envelope(actor,seq,ws="ledger",size=0): return seal_event(event(actor["device"],seq,"benchmark.event",str(seq),{"body":"x"*size},[]),ws,1,bytes(32))
def seed(db,count,authors):
    # Synthetic opaque rows exercise SQL scaling; end-to-end cryptographic delivery has separate acceptance tests.
    values=[(i+1,"ledger",hashlib.sha256(str(i).encode()).hexdigest(),f"seed-author-{i%authors}",1,i//authors+1,"{}","synthetic",0) for i in range(count)]
    db.executemany("INSERT INTO ledger_cursors VALUES (?)",[(i+1,) for i in range(count)])
    db.executemany("INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?)",values)
    db.commit()
def peak(fn):
    tracemalloc.start()
    try:
        value=fn()
        return value,tracemalloc.get_traced_memory()[1]
    finally: tracemalloc.stop()
def run(module,path,args):
    started=time.perf_counter()
    db=module.connect(path)
    result={"startup_ms":round(1000*(time.perf_counter()-started),4)}
    actor=fixture(module,db)
    seed(db,args.events,args.authors)
    heads,memory=peak(lambda:module.action(db,{"op":"ledger","workspace":"ledger"},actor["token"]))
    assert heads["tail"]==args.events and len(heads["heads"])==min(args.authors,args.events)
    result["ledger"]={**measure(lambda _:module.action(db,{"op":"ledger","workspace":"ledger"},actor["token"]),5),"rows":args.events,"heads":len(heads["heads"]),"python_peak_bytes":memory}
    large=[envelope(actor,i+1,"large",args.large_bytes) for i in range(args.large_events)]
    module.action(db,{"op":"upload_many","envelopes":large},actor["token"])
    page,memory=peak(lambda:module.action(db,{"op":"pull","workspace":"large"},actor["token"]))
    assert len(page["events"])==args.large_events and all(v["lazy"] and "envelope" not in v for v in page["events"])
    result["large_page"]={**measure(lambda _:module.action(db,{"op":"pull","workspace":"large"},actor["token"]),5),"rows":args.large_events,"ciphertext_bytes":sum(len(v["ciphertext"]) for v in large),"python_peak_bytes":memory}
    db.close()
    module.DB=path
    with getattr(module,"Server",module.ThreadingHTTPServer)(("127.0.0.1",0),module.Handler) as server:
        worker=threading.Thread(target=server.serve_forever,kwargs={"poll_interval":.01},daemon=True)
        worker.start()
        def request(body):
            began=time.perf_counter()
            with closing(http.client.HTTPConnection(*server.server_address,timeout=40)) as client:
                client.request("POST","/v1",current.canon(body),{"Authorization":"Bearer "+actor["token"],"Content-Type":"application/json"})
                response=client.getresponse()
                content=response.read()
                assert response.status==200,(response.status,content)
            return time.perf_counter()-began
        try:
            result["state_http"]=distribution([request({"op":"state"}) for _ in range(args.requests)])
            started=time.perf_counter()
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool: durations=list(pool.map(lambda _:request({"op":"state"}),range(args.requests)))
            result["state_concurrent_http"]={**distribution(durations),"workers":args.concurrency,"requests_per_second":round(len(durations)/(time.perf_counter()-started),2)}
            uploads=[envelope(actor,i+1) for i in range(args.requests)]
            result["event_upload_http"]=distribution([request({"op":"upload","envelope":value}) for value in uploads])
            ready=threading.Event()
            def hold_writer():
                with closing(sqlite3.connect(path)) as writer:
                    writer.execute("BEGIN IMMEDIATE")
                    ready.set()
                    time.sleep(.3)
                    writer.rollback()
            writer=threading.Thread(target=hold_writer)
            writer.start()
            assert ready.wait(5)
            result["state_with_300ms_writer_ms"]=round(1000*request({"op":"state"}),4)
            writer.join(5)
        finally:
            server.shutdown()
            worker.join(5)
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("PRAGMA quick_check").fetchone()[0]=="ok"
        result["final_events"]=db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        assert result["final_events"]==args.events+args.large_events+args.requests
    return {**result,"database_bytes":path.stat().st_size,"http_requests":3*args.requests+1,"server_source_sha256":hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest(),"sqlite_version":sqlite3.sqlite_version}
def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline",type=Path)
    for option,default in (("events",20000),("authors",20),("large-events",8),("large-bytes",1024**2),("requests",100),("concurrency",8)): parser.add_argument("--"+option,type=int,default=default)
    args=parser.parse_args()
    if not 1<=args.events<=100000 or not 1<=args.authors<=100 or not 1<=args.large_events<=20 or not 65536<=args.large_bytes<=2*1024**2 or not 1<=args.requests<=1000 or not 1<=args.concurrency<=20: parser.error("counts exceed the bounded synthetic benchmark limits")
    modules={"current":current}
    if args.baseline:
        spec=importlib.util.spec_from_file_location("relay_baseline",args.baseline)
        baseline=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(baseline)
        modules={"baseline":baseline,**modules}
    with tempfile.TemporaryDirectory(prefix="convos-relay-benchmark-") as directory: print(json.dumps({name:run(module,Path(directory)/f"{name}.db",args) for name,module in modules.items()},indent=2,sort_keys=True))
if __name__=="__main__": main()
