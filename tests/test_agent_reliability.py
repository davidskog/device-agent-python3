"""Offline regressions for TLS_1_0. No device, network or real config access."""
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import types
import unittest
import uuid
from unittest import mock

SOURCE = Path(__file__).resolve().parents[1] / 'davra-agent'


class Response:
    status_code = 200
    content = b'[]'


class RequestsStub(types.ModuleType):
    def __init__(self):
        super().__init__('requests')
        self.calls = []
        self.status = 200
        self.failure = None

    def call(self, method, destination, **kwargs):
        self.calls.append((method, destination, kwargs))
        if self.failure:
            raise self.failure
        response = Response()
        response.status_code = self.status
        return response

    def get(self, destination, **kwargs):
        return self.call('GET', destination, **kwargs)

    def put(self, destination, **kwargs):
        return self.call('PUT', destination, **kwargs)

    def post(self, destination, **kwargs):
        return self.call('POST', destination, **kwargs)


class Worker:
    def __init__(self):
        self.alive = True
        self.stuck = False
        self.joins = []

    def is_alive(self):
        return self.alive

    def join(self, timeout=None):
        self.joins.append(timeout)
        if not self.stuck:
            self.alive = False


class Socket:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class Client:
    instances = []

    def __init__(self):
        self._thread = None
        self._sock = Socket()
        self._sockpairR = Socket()
        self._sockpairW = Socket()
        self.published = []
        self.subscriptions = []
        self.publish_rc = 0
        self.loop_result = 0
        self.starts = 0
        self.stops = 0
        self.on_connect = self.on_message = self.on_disconnect = None
        self.auth = None
        self.connection = None
        self.disconnected = False
        self.instances.append(self)

    def username_pw_set(self, **kwargs):
        self.auth = kwargs

    def connect_async(self, host, port=1883, keepalive=60):
        self.connection = (host, port)
        self.keepalive = keepalive

    def reconnect_delay_set(self, **kwargs):
        self.delays = kwargs

    def loop_start(self):
        self.starts += 1
        self._thread = Worker()
        return self.loop_result

    def loop_stop(self):
        self.stops += 1
        if self._thread:
            self._thread.alive = False

    def disconnect(self):
        self.disconnected = True
        if self._thread and not self._thread.stuck:
            self._thread.alive = False

    def publish(self, topic, payload):
        self.published.append((topic, json.loads(payload)))
        return types.SimpleNamespace(rc=self.publish_rc)

    def subscribe(self, topic):
        self.subscriptions.append(topic)
        return 0, len(self.subscriptions)


class AgentFixture(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='tls-agent-unit-')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.requests = RequestsStub()
        auth = types.ModuleType('requests.auth')
        auth.HTTPBasicAuth = object
        paho = types.ModuleType('paho')
        mqtt = types.ModuleType('paho.mqtt')
        client = types.ModuleType('paho.mqtt.client')
        client.Client = Client
        mqtt.client = client
        paho.mqtt = mqtt
        self.modules = mock.patch.dict(sys.modules, {'requests': self.requests, 'requests.auth': auth,
                                                     'paho': paho, 'paho.mqtt': mqtt,
                                                     'paho.mqtt.client': client})
        self.modules.start()
        self.addCleanup(self.modules.stop)
        spec = importlib.util.spec_from_file_location('davra_lib', str(SOURCE / 'davra_lib.py'))
        self.lib = importlib.util.module_from_spec(spec)
        sys.modules['davra_lib'] = self.lib
        self.addCleanup(lambda: sys.modules.pop('davra_lib', None))
        # Never consult a host's installed Agent configuration during a unit test.
        with mock.patch('os.path.isfile', return_value=False):
            spec.loader.exec_module(self.lib)
        self.lib.installationDir = str(self.root)
        self.lib.logDir = str(self.root)
        self.lib.agentConfigFile = str(self.root / 'config.json')
        self.lib.conf = {'server': 'https://example.invalid', 'UUID': str(uuid.uuid4()),
                         'apiToken': 'TOKEN_CANARY_NOT_REAL', 'heartbeatInterval': 600,
                         'scriptMaxTime': 600, 'mqttBrokerAgentHost': 'localhost',
                         'mqttBrokerServerHost': 'mqtt.example.invalid',
                         'capabilities': {name: {} for name in ('drink', 'tech', 'led', 'internal')}}
        self.original_config = json.dumps(self.lib.conf).encode()
        (self.root / 'config.json').write_bytes(self.original_config)
        self.output = io.StringIO()
        self.console = mock.patch('sys.stdout', self.output)
        self.console.start()
        self.addCleanup(self.console.stop)
        self.addCleanup(lambda: self.lib.closeLogging(timeout=3))
        self.agent = self.load_agent()
        self.agent.clientOfDevice = Client()
        self.reports = []
        self.agent.reportFunctionFinishedAsEventToServer = lambda row: self.reports.append(row) or 200

    def log_output(self):
        self.lib.flushLogging(timeout=3)
        return self.output.getvalue()

    def load_agent(self):
        spec = importlib.util.spec_from_file_location('agent_under_test_' + uuid.uuid4().hex,
                                                      str(SOURCE / 'davra_agent.py'))
        agent = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(agent)
        return agent

    def start(self, name='drink', identifier=None, **extra):
        identifier = identifier or str(uuid.uuid4())
        self.agent.runFunction(name, dict({'functionUuid': identifier, 'value': 1}, **extra))
        return identifier

    def ack(self, identifier, name='drink', **extra):
        message = dict({'functionUuid': identifier, 'finishedFunctionOnApp': name,
                        'status': 'completed', 'response': 'ok'}, **extra)
        self.agent.updateFunctionStatusAsReportedByDeviceApp(message)

    def records(self):
        return {row['functionParameterValues']['functionUuid']: row
                for _, row in self.agent._functionRecords()}


class FunctionTests(AgentFixture):
    def test_overlapping_functions_keep_separate_state_and_out_of_order_acks(self):
        ids = [self.start(name) for name in ('drink', 'tech', 'led')]
        self.assertEqual(set(ids), set(self.records()))
        for index in (2, 0, 1):
            self.ack(ids[index], ('drink', 'tech', 'led')[index])
        self.assertEqual([ids[2], ids[0], ids[1]],
                         [r['functionParameterValues']['functionUuid'] for r in self.reports])
        self.assertEqual({}, self.records())

    def test_unknown_ack_leaves_real_operation_unchanged(self):
        identifier = self.start()
        before = self.records()
        self.ack(str(uuid.uuid4()))
        self.assertEqual(before, self.records())
        self.assertEqual([], self.reports)
        self.assertIn(identifier, self.records())

    def test_wrong_function_name_does_not_finish_another_function(self):
        identifier = self.start('tech')
        self.ack(identifier, 'drink')
        self.assertEqual('running', self.records()[identifier]['status'])
        self.assertEqual([], self.reports)

    def test_duplicate_command_and_ack_never_repeat_publish_or_report(self):
        identifier = self.start()
        self.start(identifier=identifier)
        self.ack(identifier)
        self.ack(identifier)
        self.start(identifier=identifier)
        self.assertEqual(1, len(self.agent.clientOfDevice.published))
        self.assertEqual(1, len(self.reports))

    def test_completed_ids_survive_reload_without_command_replay(self):
        identifier = self.start()
        self.ack(identifier)
        fresh = self.load_agent()
        fresh.clientOfDevice = Client()
        fresh.runFunction('drink', {'functionUuid': identifier, 'value': 1})
        self.assertEqual([], fresh.clientOfDevice.published)

    def test_running_state_survives_reload_without_command_replay(self):
        identifier = self.start()
        fresh = self.load_agent()
        fresh.clientOfDevice = Client()
        fresh.checkFunctionFinished()
        self.assertIn(identifier, {r['functionParameterValues']['functionUuid']
                                   for _, r in fresh._functionRecords()})
        self.assertEqual([], fresh.clientOfDevice.published)

    def test_existing_legacy_record_is_not_deleted_by_a_new_function(self):
        identifier = str(uuid.uuid4())
        self.lib.writeJsonAtomic(self.agent.currentFunctionJson,
                                 {'functionName': 'internal', 'functionParameterValues': {'functionUuid': identifier},
                                  'status': 'running', 'startTime': self.lib.getMilliSecondsSinceEpoch()})
        other = self.start('led')
        self.assertEqual({identifier, other}, set(self.records()))

    def test_timeout_is_failed_once_without_replaying_command(self):
        identifier = self.start()
        self.lib.upsertJsonEntries(self.agent._functionPath(identifier), {'startTime': 0})
        self.agent.checkFunctionFinished()
        self.agent.checkFunctionFinished()
        self.assertEqual('failed', self.reports[0]['status'])
        self.assertEqual(1, len(self.reports))
        self.assertEqual(1, len(self.agent.clientOfDevice.published))

    def test_failed_event_send_is_not_retried(self):
        identifier = self.start()
        report = mock.Mock(return_value=500)
        self.agent.reportFunctionFinishedAsEventToServer = report
        self.ack(identifier)
        for _ in range(3):
            self.agent.checkFunctionFinished()
        self.assertEqual(1, report.call_count)
        self.assertEqual(500, self.agent._recentFunctions()[identifier]['eventHttpStatus'])

    def test_cleanup_write_failure_does_not_repeat_http_report(self):
        identifier = self.start()
        original = self.lib.writeJsonAtomic
        failed = [False]
        def write(path, value):
            if str(path).endswith('.reported-functions.json') and not failed[0]:
                failed[0] = True
                raise OSError('synthetic disk failure')
            return original(path, value)
        with mock.patch.object(self.lib, 'writeJsonAtomic', side_effect=write):
            with self.assertRaises(OSError):
                self.ack(identifier)
            self.agent.checkFunctionFinished()
        self.assertEqual(1, len(self.reports))
        self.assertEqual({}, self.records())

    def test_slow_report_does_not_block_new_function_state(self):
        first = self.start('drink')
        entered, release = threading.Event(), threading.Event()
        def report(row):
            entered.set()
            if not release.wait(3):
                raise RuntimeError('test release missing')
            self.reports.append(row)
            return 200
        self.agent.reportFunctionFinishedAsEventToServer = report
        errors = []
        def finish():
            try:self.ack(first)
            except Exception as error:errors.append(error)
        worker = threading.Thread(target=finish)
        worker.start()
        try:
            self.assertTrue(entered.wait(2))
            second = self.start('tech')
            self.assertIn(second, self.records())
            self.ack(first)  # duplicate while original reporting is in progress
        finally:
            release.set()
            worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertEqual([], errors)
        self.assertEqual(1, len(self.reports))
        self.assertIn(second, self.records())

    def test_unrelated_job_is_not_finished_by_direct_function(self):
        job_id = str(uuid.uuid4())
        self.lib.writeJsonAtomic(self.agent.currentJobJson,
                                 {'UUID': job_id, 'devices': [{'status': 'running'}]})
        identifier = self.start()
        self.ack(identifier)
        self.assertEqual('running', self.lib.readJsonFile(self.agent.currentJobJson)['devices'][0]['status'])

    def test_version_and_config_remain_separate(self):
        self.assertEqual('TLS_1_2', self.lib.davraAgentVersion)
        identifier = self.start()
        self.ack(identifier)
        self.assertEqual(self.original_config, (self.root / 'config.json').read_bytes())
        self.assertNotIn('davraAgentVersion', self.lib.conf)

    def test_wire_message_has_no_agent_private_state(self):
        self.start('tech', info="O'Connor")
        message = self.agent.clientOfDevice.published[-1][1]
        self.assertFalse(any(k.startswith('_') for k in message))
        self.assertEqual("O'Connor", message['functionParameterValues']['info'])
        self.assertEqual('TLS_1_2', message['fromAgent'])

    def test_atomic_json_is_never_seen_truncated_by_a_reader(self):
        filename = str(self.root / 'shared.json')
        self.lib.writeJsonAtomic(filename, {'n': 0, 'payload': 'x' * 1000})
        errors, stop = [], threading.Event()
        def read():
            while not stop.is_set():
                try:
                    v = json.loads(Path(filename).read_text())
                    if len(v['payload']) != 1000:errors.append('truncated')
                except Exception as error:errors.append(error)
        worker = threading.Thread(target=read)
        worker.start()
        try:
            for number in range(50):
                self.lib.writeJsonAtomic(filename, {'n': number, 'payload': 'x' * 1000})
        finally:
            stop.set();worker.join(3)
        self.assertEqual([], errors)
        self.assertFalse(worker.is_alive())


class HttpAndLogTests(AgentFixture):
    def test_each_http_method_is_called_once_on_failure(self):
        for method, action in [('GET', self.lib.httpGet), ('PUT', self.lib.httpPut), ('POST', self.lib.httpPost)]:
            with self.subTest(method=method):
                self.requests.calls.clear()
                self.requests.failure = OSError('SECRET_FROM_EXCEPTION')
                response = action('https://example.invalid/api/test?token=QUERY_CANARY') if method == 'GET' else action('https://example.invalid/api/test?token=QUERY_CANARY', {'password': 'PAYLOAD_CANARY'})
                self.assertEqual(500, response.status_code)
                self.lib.flushLogging(timeout=3)
                data_calls = [c for c in self.requests.calls if '/api/test?' in c[1]]
                self.assertEqual(1, len(data_calls))
                self.assertEqual(method, data_calls[0][0])
        text = self.log_output()
        for secret in ('SECRET_FROM_EXCEPTION', 'QUERY_CANARY', 'PAYLOAD_CANARY', 'TOKEN_CANARY_NOT_REAL'):
            self.assertNotIn(secret, text)

    def test_non_2xx_log_has_status_without_resending(self):
        self.requests.status = 503
        response = self.lib.httpPut('https://example.invalid/api/test', [{'name': 'test.metric'}])
        self.assertEqual(503, response.status_code)
        self.lib.flushLogging(timeout=3)
        self.assertEqual(1, len([c for c in self.requests.calls if c[1].endswith('/api/test')]))
        self.assertIn('http_status=503', self.log_output())
        self.assertIn('test.metric', self.log_output())

    def test_remote_log_failure_does_not_recurse(self):
        self.requests.failure = OSError('broken network')
        self.lib.logEvent('agent_started', forward=True, version='TLS_1_2')
        self.lib.flushLogging(timeout=3)
        self.assertEqual(1, len(self.requests.calls))
        self.assertIn('agent_started', self.log_output())
        self.assertIn('log_remote_failed', self.log_output())

    def test_missing_logfile_is_created(self):
        self.lib.logEvent('agent_started', version='TLS_1_2')
        self.lib.flushLogging(timeout=3)
        self.assertIn('agent_started', (self.root / 'davra_agent_TLS_1_2.log').read_text())

    def test_file_log_failure_does_not_prevent_console_or_return(self):
        with mock.patch('os.open', side_effect=PermissionError('synthetic')):
            self.lib.logEvent('agent_action_failed', 'ERROR', reason='visible_diagnostic')
            self.lib.flushLogging(timeout=3)
        self.assertIn('visible_diagnostic', self.log_output())
        self.assertIn('log_local_failed', self.log_output())


if __name__ == '__main__':
    unittest.main()
