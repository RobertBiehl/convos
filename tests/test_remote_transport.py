"""Exercise transport boundaries without sending credentials outside loopback."""
import io, threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import ai_convos_remote as remote


@contextmanager
def endpoint(status,body,headers=None):
    calls=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args): pass
        def do_POST(self):
            calls.append((self.path,self.headers.get("Authorization")))
            self.rfile.read(int(self.headers.get("Content-Length",0)))
            self.send_response(status)
            for key,value in (headers or {}).items(): self.send_header(key,value)
            self.send_header("Content-Length",str(len(body)))
            self.end_headers()
            self.wfile.write(body)
    server=ThreadingHTTPServer(("127.0.0.1",0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True)
    thread.start()
    try: yield f"http://127.0.0.1:{server.server_port}",calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join(2)


@pytest.mark.parametrize("status",[301,302,303,307,308])
def test_redirect_never_forwards_bearer_or_replays_request(status):
    with endpoint(200,b'{}') as (target,received),endpoint(status,b'',{"Location":target+"/capture"}) as (source,sent):
        with pytest.raises(ValueError,match="redirects are not allowed"):
            remote.request({"url":source,"token":"synthetic-secret"},{"op":"state"})
        assert sent==[("/v1","Bearer synthetic-secret")] and received==[]


@pytest.mark.parametrize("status,error",[(403,ValueError),(429,ConnectionError),(503,ConnectionError)])
@pytest.mark.parametrize("body",[b'{"error":"synthetic rejection"}',b'<html>upstream unavailable</html>'])
def test_http_errors_preserve_status_and_retryability(status,error,body):
    with endpoint(status,body) as (url,calls):
        with pytest.raises(error,match=f"Remote state HTTP {status}"):
            remote.request({"url":url,"token":"synthetic-secret"},{"op":"state"})
        assert len(calls)==1


@pytest.mark.parametrize("raw,limit,match",[(b'x'*17,16,"byte limit"),(b'not json',16,"invalid JSON"),(b'[]',16,"JSON object")])
def test_response_reader_bounds_bytes_and_closes_on_failure(raw,limit,match):
    response=io.BytesIO(raw)
    with pytest.raises(ValueError,match=match): remote._response_json(response,limit)
    assert response.closed


@pytest.mark.parametrize("url",["ftp://localhost/file","file://localhost/file","https://user:password@example.test","https://"])
def test_relay_url_rejects_non_http_and_embedded_credentials(url,monkeypatch):
    monkeypatch.setenv("CONVOS_REMOTE_INSECURE","1")
    with pytest.raises(ValueError,match="Remote URL"): remote.safe_url(url)


def test_response_reader_accepts_object_and_closes():
    response=io.BytesIO(b'{"ok":true}')
    assert remote._response_json(response)=={"ok":True} and response.closed
