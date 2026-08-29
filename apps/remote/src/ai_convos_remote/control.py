"""Signed workspace control state, device proposals, votes, and transition checks."""
import time

from .protocol import V, _signed, _verified, digest, public, public_id, verify_certificate

CONTROL_V=1

def sign(device,body): return _signed({**body,"author":device["id"]},device["sign_private"])
def verify(value,sign_public):
    _verified(value,sign_public)
    return value
def record(user,root_public,device,certificate,history=True): return {"user":user,"root_public":root_public,"device":public(device),"certificate":certificate,"history":history}
def verify_record(value):
    body,device=verify_certificate(value["certificate"],value["root_public"]),value["device"]
    if public_id(value["root_public"])!=value["user"] or public_id(device["sign_public"])!=device["id"] or body["user"]!=value["user"] or body["device"]!=device: raise ValueError("device record mismatch")
    return value
def state_hash(value): return digest(value)
def proposal(device,workspace,base,target,expires,not_before=0,nonce=None,kind="device.proposal"):
    return sign(device,{"v":V,"kind":kind,"workspace":workspace,"base":state_hash(base),"epoch":base["epoch"],"target":verify_record(target),"certificate_hash":digest(target["certificate"]),"nonce":nonce or digest(__import__("os").urandom(32)),"not_before":not_before,"expires":expires})
def vote(device,user,request,approve=True): return sign(device,{"v":V,"kind":"device.vote","proposal":state_hash(request),"workspace":request["workspace"],"base":request["base"],"voter":user,"approve":approve})
def electorate(base,target): return sorted({d["user"] for d in base["devices"].values() if d["user"]!=target})
def verify_proposal(base,request,now=None,kind="device.proposal"):
    verify(request,verify_record(request["target"])["device"]["sign_public"])
    if request["v"]!=V or request["kind"]!=kind or request["base"]!=state_hash(base) or request["workspace"]!=base["workspace"] or request["epoch"]!=base["epoch"] or request["certificate_hash"]!=digest(request["target"]["certificate"]): raise ValueError("proposal state mismatch")
    if not request["not_before"]<=float(now if now is not None else time.time())<request["expires"]: raise ValueError("proposal is not active")
    return request["target"]
def approved(base,request,votes,now=None,kind="device.proposal"):
    verify_proposal(base,request,now,kind)
    eligible,by_user=set(electorate(base,request["target"]["user"])),{}
    for item in votes:
        voter,device=item["voter"],base["devices"].get(item["author"])
        if item["v"]!=V or item["kind"]!="device.vote" or type(item["approve"]) is not bool or not device or device["user"]!=voter or voter not in eligible: raise ValueError("ineligible vote")
        verify(item,device["device"]["sign_public"])
        if (item["proposal"],item["workspace"],item["base"])!=(state_hash(request),request["workspace"],request["base"]): raise ValueError("vote proposal mismatch")
        if voter in by_user and by_user[voter]!=item["approve"]: raise ValueError("conflicting user votes")
        by_user[voter]=item["approve"]
    needed=len(eligible)//2+1
    if not eligible or sum(value is True for value in by_user.values())<needed: raise ValueError(f"approval requires {needed} of {len(eligible)} votes")
    return needed,len(eligible)
def _same(a,b,*keys): return all(a[k]==b[k] for k in keys)
def verify_state(value,previous=None):
    if value["v"]!=CONTROL_V or value["kind"]!="workspace.state": raise ValueError("unsupported workspace state")
    boundary,heads=value["boundary"],value["boundary"]["heads"]
    if value["scope"] not in ("personal","team") or len(value["key_commitment"])!=64 or set(boundary)!={"epoch","tail","heads"} or boundary["epoch"]!=value["epoch"] or not isinstance(boundary["tail"],int) or isinstance(boundary["tail"],bool) or boundary["tail"]<0 or not isinstance(heads,dict) or any(set(h)!={"seq","event"} or not isinstance(h["seq"],int) or isinstance(h["seq"],bool) or h["seq"]<1 or not isinstance(h["event"],str) or len(h["event"])!=64 for h in heads.values()) or any(set(m)!={"role","joined","history_from"} or m["role"] not in ("admin","member") or any(not isinstance(m[k],int) or isinstance(m[k],bool) or not 1<=m[k]<=value["epoch"] for k in ("joined","history_from")) for m in value["members"].values()) or any(d["user"] not in value["members"] for d in value["devices"].values()) or set(value["devices"])&set(value["removed"]): raise ValueError("invalid workspace state")
    author=(value["devices"] if previous is None else previous["devices"]).get(value["author"])
    if previous is not None and value["action"]=="personal_recover": author=value["devices"].get(value["author"])
    if not author: raise ValueError("state author is not authorized")
    verify(value,verify_record(author)["device"]["sign_public"])
    [verify_record(d) for d in value["devices"].values()]
    if previous is None:
        if value["revision"]!=1 or value["prev"] is not None or value["epoch"]!=1 or boundary!={"epoch":1,"tail":0,"heads":{}} or value["author"] not in value["devices"] or value["action"]!="create" or value["members"][author["user"]]["role"]!="admin" or set(value["members"])!={author["user"]} or set(value["devices"])!={value["author"]} or value["removed"]: raise ValueError("invalid genesis state")
        return value
    if not _same(value,previous,"workspace","scope") or value["revision"]!=previous["revision"]+1 or value["prev"]!=state_hash(previous): raise ValueError("workspace state chain mismatch")
    if value["epoch"]==previous["epoch"] and boundary!=previous["boundary"] or value["epoch"]>previous["epoch"] and (boundary["tail"]<previous["boundary"]["tail"] or any(author not in heads or heads[author]["seq"]<head["seq"] or heads[author]["seq"]==head["seq"] and heads[author]["event"]!=head["event"] for author,head in previous["boundary"]["heads"].items())): raise ValueError("workspace history boundary mismatch")
    action,admin=value["action"],previous["members"][author["user"]]["role"]=="admin"
    if action in ("membership","remove") and not admin: raise ValueError("admin control required")
    if action in ("self_approve","quorum_approve","personal_recover"):
        if value["members"]!=previous["members"] or value["removed"]!=previous["removed"] or value["epoch"]!=previous["epoch"]+1: raise ValueError("approval changed workspace policy")
        added=set(value["devices"])-set(previous["devices"])
        if len(added)!=1 or set(previous["devices"])-set(value["devices"]): raise ValueError("approval must add exactly one device")
        target,device=value["approval"]["proposal"]["target"],next(iter(added))
        if device!=target["device"]["id"] or {k:v for k,v in value["devices"][device].items() if k!="history"}!={k:v for k,v in target.items() if k!="history"} or device in previous["removed"]: raise ValueError("approval target mismatch")
        if action=="self_approve" and (author["user"]!=target["user"] or value["devices"][device]["history"]!=author["history"]): raise ValueError("self approval permission mismatch")
        if action in ("self_approve","personal_recover"): verify_proposal(previous,value["approval"]["proposal"],value["approved_at"])
        if action=="quorum_approve" and value["devices"][device]["history"] is not False: raise ValueError("quorum approval must be future-only")
        if action=="quorum_approve": approved(previous,value["approval"]["proposal"],value["approval"]["votes"],value["approved_at"])
        if action=="personal_recover" and (previous.get("scope")!="personal" or target["user"] not in previous["members"]): raise ValueError("personal recovery mismatch")
    elif action=="history":
        if set(value["devices"])!=set(previous["devices"]) or any({k:v for k,v in d.items() if k!="history"}!={k:v for k,v in previous["devices"][i].items() if k!="history"} or previous["devices"][i]["history"] and not d["history"] for i,d in value["devices"].items()) or value["removed"]!=previous["removed"] or value["epoch"]!=previous["epoch"] or value["key_commitment"]!=previous["key_commitment"] or set(value["members"])!=set(previous["members"]) or any(not _same(m,previous["members"][u],"role","joined") or m["history_from"]>previous["members"][u]["history_from"] for u,m in value["members"].items()) or not admin: raise ValueError("invalid history transition")
    elif action=="history_activate":
        target,device,expected=(target:=value["approval"]["proposal"]["target"]),(device:=target["device"]["id"]),{**previous["devices"],device:{**previous["devices"][device],"history":True}}
        if target["history"] is not True or previous["devices"].get(device,{}).get("history") is not False or value["members"]!=previous["members"] or value["devices"]!=expected or value["removed"]!=previous["removed"] or value["epoch"]!=previous["epoch"] or value["key_commitment"]!=previous["key_commitment"]: raise ValueError("invalid history activation")
        approved(previous,value["approval"]["proposal"],value["approval"]["votes"],value["approved_at"],"history.proposal")
    elif action=="remove":
        if value["members"]!=previous["members"] or value["epoch"]!=previous["epoch"]+1: raise ValueError("invalid device removal")
        removed=set(previous["devices"])-set(value["devices"])
        if not removed or not removed<=set(value["removed"]) or not set(previous["removed"])<=set(value["removed"]) or any(value["devices"].get(d)!=r for d,r in previous["devices"].items() if d not in removed): raise ValueError("removed device tombstone missing")
    elif action=="membership":
        added_users,removed_users,added,removed=set(value["members"])-set(previous["members"]),set(previous["members"])-set(value["members"]),set(value["devices"])-set(previous["devices"]),set(previous["devices"])-set(value["devices"])
        if value["epoch"]!=previous["epoch"]+1 or value["scope"]=="personal" and value["members"]!=previous["members"] or set(value["devices"])&set(value["removed"]) or any(r["user"] not in added_users for d,r in value["devices"].items() if d in added) or any(r["user"] not in removed_users for d,r in previous["devices"].items() if d in removed) or any(value["devices"].get(d)!=r for d,r in previous["devices"].items() if r["user"] not in removed_users) or any(not _same(m,previous["members"][u],"joined","history_from") for u,m in value["members"].items() if u not in added_users) or any((m["joined"],m["history_from"])!=(value["epoch"],value["epoch"]) for u,m in value["members"].items() if u in added_users) or not set(previous["removed"])<=set(value["removed"]) or not removed<=set(value["removed"]): raise ValueError("invalid membership transition")
    else: raise ValueError("unknown workspace action")
    return value
