"""Expected checkout lifecycle and Git failures must not block conversation capture."""
import json, subprocess
from pathlib import Path

from ai_convos import cli
from tests.test_hooks import hooks, transcript, enqueue


def repository(path):
    path.mkdir()
    subprocess.run(['git','-C',str(path),'init','-q'],check=True)
    subprocess.run(['git','-C',str(path),'-c','user.name=Test','-c','user.email=test@example.invalid','commit','--allow-empty','-qm','initial'],check=True)
    return path


def session(path,cwd,text):
    transcript(path,text)
    path.write_text(path.read_text().replace('"/repo"',json.dumps(str(cwd))))
    return path


def ownership_failure(monkeypatch,root):
    run=cli._git_run
    def guarded(path,*args):
        if Path(path).resolve()==root.resolve(): raise subprocess.CalledProcessError(128,('git','-C',str(path),*args),stderr=b'fatal: detected dubious ownership in repository')
        return run(path,*args)
    monkeypatch.setattr(cli,'_git_run',guarded)
    return run


def test_ownership_failure_finishes_capture_and_does_not_poison_other_repository(hooks,tmp_path,monkeypatch):
    sessions,data=hooks
    bad,good=repository(tmp_path/'bad'),repository(tmp_path/'good')
    enqueue(session(sessions/'bad.jsonl',bad,'bad repo message'))
    enqueue(session(sessions/'good.jsonl',good,'good repo message'))
    run=ownership_failure(monkeypatch,bad)
    assert cli.drain_hooks()==2
    assert json.loads(cli.HOOK_PROGRESS.read_text())['pending']==0
    with cli.open_db(read_only=True,purpose='test.capture') as db:
        assert {r[0] for r in db.execute('SELECT content FROM messages').fetchall()}=={'bad repo message','good repo message'}
        assert db.execute('SELECT c.cwd FROM provenance.pending p JOIN conversations c ON c.id=p.entity').fetchall()==[(str(bad),)]
        assert db.execute('SELECT repository IS NOT NULL FROM provenance.conversation_scopes WHERE cwd=?',[str(good)]).fetchone()==(True,)
        assert db.execute('SELECT checkout LIKE \'pending:%\' FROM provenance.conversation_scopes WHERE cwd=?',[str(bad)]).fetchone()==(True,)
    monkeypatch.setattr(cli,'_git_run',run)
    cli.capture_provenance(edit_ids=set(),conversation_ids=set(),strict=False)
    with cli.open_db(read_only=True,purpose='test.recovered') as db:
        assert db.execute('SELECT count(*) FROM provenance.pending').fetchone()==(0,)
        assert db.execute('SELECT count(*) FROM messages').fetchone()==(2,)


def test_full_sync_saves_parser_checkpoint_despite_unavailable_git(hooks,tmp_path,monkeypatch):
    sessions,data=hooks
    root=repository(tmp_path/'repo')
    path=session(sessions/'ownership.jsonl',root,'captured before Git')
    monkeypatch.setattr(cli,'STATE_PATH',data/'sync_state.json')
    ownership_failure(monkeypatch,root)
    cli.sync(False,300,False,True,True,False,True)
    assert str(path) in cli.load_state()['local']['codex']['files']
    with cli.open_db(read_only=True,purpose='test.checkpoint') as db:
        assert db.execute('SELECT content FROM messages').fetchall()==[('captured before Git',)]
        assert db.execute('SELECT count(*) FROM provenance.pending').fetchone()==(1,)


def test_missing_git_executable_does_not_repeat_completed_capture(hooks,tmp_path,monkeypatch):
    sessions,data=hooks
    root=repository(tmp_path/'repo')
    enqueue(session(sessions/'without-git.jsonl',root,'durable without Git'))
    def unavailable(*args): raise FileNotFoundError(2,'git executable unavailable','git')
    monkeypatch.setattr(cli,'_git_run',unavailable)
    assert cli.drain_hooks()==1
    with cli.open_db(read_only=True,purpose='test.missing-git') as db:
        assert db.execute('SELECT content FROM messages').fetchall()==[('durable without Git',)]
        assert db.execute('SELECT count(*) FROM provenance.pending').fetchone()==(1,)


def worktree(tmp_path):
    root=repository(tmp_path/'repo')
    path=root/'.koder'/'worktrees'/'task'; path.parent.mkdir(parents=True)
    subprocess.run(['git','-C',str(root),'worktree','add','--detach',str(path),'HEAD'],check=True,capture_output=True)
    return root,path


def remove(root,path): subprocess.run(['git','-C',str(root),'worktree','remove',str(path)],check=True,capture_output=True)


def test_first_import_after_worktree_removal_never_inherits_parent_checkout(hooks,tmp_path,monkeypatch):
    sessions,data=hooks
    root,path=worktree(tmp_path)
    transcript_path=session(sessions/'removed.jsonl',path,'worktree history')
    remove(root,path)
    monkeypatch.setattr(cli,'STATE_PATH',data/'sync_state.json')
    for _ in range(2): cli.sync(False,300,False,True,True,False,True)
    with cli.open_db(read_only=True,purpose='test.removed') as db:
        assert db.execute('SELECT cwd FROM conversations').fetchall()==[(str(path),)]
        assert db.execute('SELECT repository,root,checkout FROM provenance.conversation_scopes').fetchall()==[(None,None,None)]
        assert db.execute('SELECT content FROM messages').fetchall()==[('worktree history',)]
    assert cli.repository(path) is None


def test_removal_preserves_previously_observed_scope_through_full_reimport(hooks,tmp_path,monkeypatch):
    sessions,data=hooks
    root,path=worktree(tmp_path)
    enqueue(session(sessions/'removed.jsonl',path,'recorded worktree'))
    assert cli.drain_hooks()==1
    with cli.open_db(read_only=True,purpose='test.before') as db: scope=db.execute('SELECT * FROM provenance.conversation_scopes').fetchall()
    assert scope[0][2] is not None
    remove(root,path)
    monkeypatch.setattr(cli,'STATE_PATH',data/'sync_state.json')
    for _ in range(2): cli.sync(False,300,False,True,True,False,True)
    with cli.open_db(read_only=True,purpose='test.after') as db:
        assert db.execute('SELECT * FROM provenance.conversation_scopes').fetchall()==scope
        assert db.execute('SELECT content FROM messages').fetchall()==[('recorded worktree',)]
        assert db.execute('SELECT count(*) FROM provenance.pending').fetchone()==(0,)


def test_worktree_disappearing_during_lookup_cannot_inherit_parent(tmp_path,monkeypatch):
    root,path=worktree(tmp_path)
    def disappears(probe):
        remove(root,path)
        return root
    monkeypatch.setattr(cli,'_git_root',disappears)
    assert cli.repository(path,refresh=False) is None


def test_failed_repository_filling_retry_page_does_not_starve_later_work(hooks,tmp_path,monkeypatch):
    bad,good=repository(tmp_path/'bad'),repository(tmp_path/'good')
    with cli.open_db(purpose='test.retry.backlog') as db:
        cli.init_schema(db)
        db.executemany("INSERT INTO conversations(id,source,cwd,metadata) VALUES (?,'codex',?,'{}')",[(f'bad-{i:04}',str(bad)) for i in range(500)]+[('good',str(good))])
        db.execute("INSERT INTO provenance.pending SELECT 'conversations',id,1 FROM conversations")
    ownership_failure(monkeypatch,bad)
    cli.capture_provenance(edit_ids=set(),conversation_ids=set(),strict=False)
    with cli.open_db(read_only=True,purpose='test.retry.progress') as db:
        assert db.execute("SELECT repository IS NOT NULL FROM provenance.conversation_scopes WHERE conversation='good'").fetchone()==(True,)
        assert db.execute('SELECT count(*) FROM provenance.pending').fetchone()==(500,)
