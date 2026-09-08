import http.client, json, socket, threading, time
from contextlib import contextmanager

import pytest
import ai_convos_remote_server as relay


@pytest.fixture
def running_relay(tmp_path,monkeypatch):
    path=tmp_path/"server.db"; relay.connect(path).close(); monkeypatch.setattr(relay,"DB",path)
    @contextmanager
    def run(workers=2,idle=1,deadline=2):
        monkeypatch.setenv("CONVOS_SERVER_WORKERS",str(workers))
        started=threading.Event()
        class Handler(relay.Handler):
            timeout=idle
            def setup(self):
                super().setup()
                started.set()
        with relay.Server(("127.0.0.1",0),Handler) as server:
            server.request_timeout=deadline
            worker=threading.Thread(target=server.serve_forever,kwargs={"poll_interval":.01},daemon=True); worker.start()
            try: yield server.server_address,started
            finally:
                server.shutdown(); worker.join(2)
    return run


def response(address,headers,body=b"",eof=False,path="/v1"):
    with socket.create_connection(address,timeout=2) as conn:
        conn.sendall(f"POST {path} HTTP/1.1\r\nHost: relay\r\n{headers}\r\n".encode()+body)
        if eof: conn.shutdown(socket.SHUT_WR)
        out=http.client.HTTPResponse(conn); out.begin()
        return out.status,json.loads(out.read())


@pytest.mark.parametrize("headers",[
    "", "Content-Length: -1\r\n", "Content-Length: +2\r\n", "Content-Length: 1.0\r\n",
    "Content-Length: 0\r\nContent-Length: 0\r\n", "Content-Length: 0\r\nContent-Length: 4\r\n",
    "Transfer-Encoding: chunked\r\nContent-Length: 0\r\n", f"Content-Length: {64*1024**2+1}\r\n",
])
def test_invalid_http_framing_is_rejected_without_waiting_for_body(running_relay,headers):
    with running_relay() as (address,_): assert response(address,headers)[0]==400


@pytest.mark.parametrize("body",[b"[]",b"null",b'"state"',b'{"op":1}',b'{"op":[]}',b'{"op":"recovery_fetch","user":{}}'])
def test_invalid_http_operation_shapes_are_client_errors(running_relay,body):
    with running_relay() as (address,_): assert response(address,f"Content-Length: {len(body)}\r\n",body)[0]==400


def test_truncated_body_is_rejected_before_dispatch(running_relay,monkeypatch):
    monkeypatch.setattr(relay,"action",lambda *args:pytest.fail("truncated request reached dispatch"))
    with running_relay() as (address,_): assert response(address,"Content-Length: 3\r\n",b"{}",True)==(400,{"error":"incomplete request body"})


def test_internal_failure_does_not_disclose_exception(running_relay,monkeypatch,caplog):
    def fail(*args): raise RuntimeError("private relay filesystem path")
    monkeypatch.setattr(relay,"action",fail)
    with running_relay() as (address,_): assert response(address,"Content-Length: 2\r\n",b"{}")== (500,{"error":"relay request failed"})
    assert "private relay filesystem path" in caplog.text


def test_worker_limit_returns_retryable_overload_then_recovers(running_relay):
    with running_relay(workers=1,idle=.1) as (address,started),socket.create_connection(address,timeout=2) as stalled:
        assert started.wait(2)
        with socket.create_connection(address,timeout=2) as conn:
            out=http.client.HTTPResponse(conn); out.begin()
            assert out.status==503 and out.getheader("Retry-After")=="1" and json.loads(out.read())=={"error":"relay busy"}
        assert stalled.recv(1)==b""
        limit=time.monotonic()+2
        while time.monotonic()<limit:
            status,value=response(address,"Content-Length: 14\r\n",b'{"op":"state"}')
            if status!=503: break
        assert (status,value)==(403,{"error":"invalid device token"})


def test_absolute_deadline_closes_trickling_request(running_relay):
    with running_relay(workers=1,idle=.2,deadline=.15) as (address,started),socket.create_connection(address,timeout=2) as conn:
        assert started.wait(2)
        conn.sendall(b"POST /v1 HTTP/1.1\r\nHost: relay\r\nContent-Length: 1000\r\n\r\n")
        began=time.monotonic()
        for _ in range(15):
            try: conn.sendall(b" ")
            except OSError: break
            time.sleep(.03)
        try: tail=conn.recv(4096)
        except ConnectionResetError: tail=b""
        assert tail==b"" and time.monotonic()-began<.6
        assert response(address,"Content-Length: 14\r\n",b'{"op":"state"}')[0]==403
