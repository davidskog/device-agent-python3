"""No live services: connection reuse, failure and shutdown boundaries."""
import threading
import types
import unittest
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from test_agent_reliability import AgentFixture

try:
    import requests as real_requests
except ImportError:
    real_requests = None


class FakeSession:
    def __init__(self, call=None):
        self.calls = []
        self.closes = 0
        self.clears = 0
        self.call = call
        self.cookies = types.SimpleNamespace(clear=self.clear)

    def clear(self):
        self.clears += 1

    def request(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if self.call:
            return self.call(*args, **kwargs)
        return 200

    def close(self):
        self.closes += 1


class HttpTransportTests(AgentFixture):
    def test_one_thread_reuses_connection_without_cookie_or_header_state(self):
        sessions = []
        def factory():
            session = FakeSession();sessions.append(session);return session
        transport = self.lib.AgentHttpTransport(factory)
        for _ in range(3):
            self.assertEqual(200, transport.request('PUT', 'https://example.invalid', headers={'Authorization':'test-only'}))
        self.assertEqual(1, len(sessions))
        self.assertEqual(3, len(sessions[0].calls));self.assertEqual(6, sessions[0].clears)
        self.assertEqual(2, transport.snapshot()['reused'])
        self.assertTrue(transport.close()['clean']);self.assertEqual(1, sessions[0].closes)

    def test_parallel_threads_never_share_session_or_hold_registry_lock_during_request(self):
        barrier=threading.Barrier(2);sessions=[];errors=[]
        def factory():
            session=FakeSession(lambda *a,**k:barrier.wait(2));sessions.append(session);return session
        transport=self.lib.AgentHttpTransport(factory,max_cached=1)
        def call():
            try:transport.request('GET','https://example.invalid')
            except BaseException as error:errors.append(error)
        workers=[threading.Thread(target=call)for _ in range(2)]
        for worker in workers:worker.start()
        for worker in workers:worker.join(3)
        self.assertFalse(errors);self.assertTrue(all(not t.is_alive()for t in workers))
        self.assertEqual(2,len(sessions));self.assertEqual(1,transport.snapshot()['cached'])
        self.assertEqual(1,sum(s.closes for s in sessions),'overflow closes after its only request')
        transport.close();self.assertEqual(2,sum(s.closes for s in sessions))

    def test_dead_thread_session_is_reaped_before_later_admission(self):
        sessions=[]
        def factory():
            s=FakeSession();sessions.append(s);return s
        transport=self.lib.AgentHttpTransport(factory)
        worker=threading.Thread(target=lambda:transport.request('GET','https://example.invalid'))
        worker.start();worker.join(2)
        transport.request('GET','https://example.invalid')
        self.assertEqual(1,sessions[0].closes);self.assertEqual(1,transport.snapshot()['cached'])
        transport.close()

    def test_exception_is_one_attempt_and_next_operation_gets_a_new_session(self):
        sessions=[]
        def failed(*args,**kwargs):raise TimeoutError('private error')
        def factory():
            s=FakeSession(failed if not sessions else None);sessions.append(s);return s
        transport=self.lib.AgentHttpTransport(factory)
        with self.assertRaises(TimeoutError):transport.request('PUT','https://example.invalid')
        self.assertEqual(1,len(sessions[0].calls));self.assertEqual(1,sessions[0].closes)
        self.assertEqual(0,transport.snapshot()['cached'])
        transport.request('GET','https://example.invalid');self.assertEqual(2,len(sessions))
        transport.close()

    def test_shutdown_never_closes_an_inflight_request_or_admits_a_late_one(self):
        entered=threading.Event();release=threading.Event()
        def call(*args,**kwargs):entered.set();release.wait(2);return 200
        session=FakeSession(call);transport=self.lib.AgentHttpTransport(lambda:session)
        worker=threading.Thread(target=lambda:transport.request('PUT','https://example.invalid'))
        worker.start();self.assertTrue(entered.wait(1))
        self.assertEqual({'clean':False,'active':1},transport.close());self.assertEqual(0,session.closes)
        with self.assertRaises(RuntimeError):transport.request('GET','https://example.invalid')
        release.set();worker.join(2);self.assertEqual(1,session.closes)
        self.assertEqual({'clean':True,'active':0},transport.close())

    def test_close_failure_is_counted_and_cannot_change_request_outcome(self):
        session=FakeSession()
        def fail_close():raise OSError('private close error')
        session.close=fail_close;transport=self.lib.AgentHttpTransport(lambda:session)
        self.assertEqual(200,transport.request('GET','https://example.invalid'))
        self.assertFalse(transport.close()['clean']);self.assertEqual(1,transport.snapshot()['closeFailures'])

    @unittest.skipIf(real_requests is None,'requests unavailable on this development host')
    def test_real_requests_reuse_tcp_and_never_resend_failed_http_or_retain_cookies(self):
        calls=[]
        class Handler(BaseHTTPRequestHandler):
            protocol_version='HTTP/1.1'
            def log_message(self,*args):pass
            def do_GET(self):self.reply(200)
            def do_PUT(self):
                self.rfile.read(int(self.headers.get('Content-Length','0')));self.reply(503)
            def do_POST(self):
                self.rfile.read(int(self.headers.get('Content-Length','0')))
                calls.append((self.command,self.client_address[1],self.headers.get('Cookie')))
                self.close_connection=True
                self.connection.shutdown(socket.SHUT_RDWR)
            def reply(self,status):
                calls.append((self.command,self.client_address[1],self.headers.get('Cookie')))
                self.send_response(status);self.send_header('Content-Length','2')
                self.send_header('Set-Cookie','do_not_retain=yes');self.end_headers();self.wfile.write(b'{}')
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.daemon_threads=True
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        self.lib.requests=real_requests
        transport=self.lib.AgentHttpTransport()
        try:
            url='http://127.0.0.1:'+str(server.server_port)+'/'
            for _ in range(2):self.assertEqual(200,transport.request('GET',url,timeout=2).status_code)
            self.assertEqual(503,transport.request('PUT',url,data='{}',timeout=2).status_code)
            self.assertEqual(3,len(calls));self.assertEqual(1,len(set(p for _,p,_ in calls)))
            self.assertTrue(all(cookie is None for _,_,cookie in calls))
            with self.assertRaises(real_requests.RequestException):transport.request('POST',url,data='{}',timeout=2)
            self.assertEqual(4,len(calls),'connection loss never replays the attempted POST')
            self.assertEqual(0,transport.snapshot()['cached'])
            self.assertEqual(200,transport.request('GET',url,timeout=2).status_code)
            self.assertEqual(5,len(calls));self.assertEqual(2,transport.snapshot()['created'])
        finally:
            transport.close();server.shutdown();server.server_close();thread.join(2)

    def test_default_adapter_has_no_automatic_retry_and_one_connection_per_origin(self):
        mounts=[];kwargs=[]
        session=FakeSession();session.mount=lambda scheme,adapter:mounts.append((scheme,adapter))
        self.requests.Session=lambda:session
        self.requests.adapters.HTTPAdapter=lambda **options:kwargs.append(options)or options
        self.assertIs(session,self.lib._newHttpSession())
        self.assertEqual(['http://','https://'],[scheme for scheme,_ in mounts])
        self.assertTrue(all(x=={'max_retries':0,'pool_connections':1,'pool_maxsize':1,'pool_block':True}for x in kwargs))

    def test_partial_session_creation_closes_and_preserves_original_exception(self):
        session=FakeSession();self.requests.Session=lambda:session
        def fail(*args):raise RuntimeError('mount failed')
        session.mount=fail
        with self.assertRaisesRegex(RuntimeError,'mount failed'):self.lib._newHttpSession()
        self.assertEqual(1,session.closes)

    def test_factory_failure_still_closes_dead_owner_sessions(self):
        session=FakeSession();transport=self.lib.AgentHttpTransport(lambda:session)
        worker=threading.Thread(target=lambda:transport.request('GET','https://example.invalid'))
        worker.start();worker.join(2)
        def fail():raise RuntimeError('factory failed')
        transport.factory=fail
        with self.assertRaisesRegex(RuntimeError,'factory failed'):transport.request('GET','https://example.invalid')
        self.assertEqual(1,session.closes);self.assertEqual(0,transport.snapshot()['active'])
        transport.close()

    def test_interruption_retires_connection_and_never_replays(self):
        def interrupt(*a,**k):raise KeyboardInterrupt()
        session=FakeSession(interrupt);transport=self.lib.AgentHttpTransport(lambda:session)
        with self.assertRaises(KeyboardInterrupt):transport.request('PUT','https://example.invalid')
        self.assertEqual(1,len(session.calls));self.assertEqual(1,session.closes)
        self.assertEqual(0,transport.snapshot()['cached']);self.assertTrue(transport.close()['clean'])
