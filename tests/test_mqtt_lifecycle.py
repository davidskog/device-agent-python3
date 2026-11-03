"""No-network MQTT callback and lifecycle regressions."""
import json
import socket
import threading
import time
import types
import unittest
import uuid
from unittest import mock
from test_agent_reliability import AgentFixture, Client

try:
    import paho.mqtt.client as native_paho
except ImportError:
    native_paho = None


class MqttTests(AgentFixture):
    def test_server_keepalive_is_below_observed_idle_disconnect(self):
        self.agent.manageMqttClients()
        self.addCleanup(self.agent.closeMqttClients)
        self.assertEqual(30, self.agent.clientOfServer.keepalive)
        self.assertEqual(60, self.agent.clientOfDevice.keepalive)

    def test_connected_callback_does_not_block_on_http_log_forwarding(self):
        self.agent.manageMqttClients()
        self.addCleanup(self.agent.closeMqttClients)
        self.requests.calls.clear()
        self.agent.mqttOnConnectServer(self.agent.clientOfServer, None, {}, 0)
        self.assertEqual([], self.requests.calls)

    def test_unrelated_subscription_ack_cannot_mark_connection_ready(self):
        self.agent.manageMqttClients()
        self.addCleanup(self.agent.closeMqttClients)
        client = self.agent.clientOfServer
        self.agent.mqttOnConnectServer(client, None, {}, 0)
        self.agent.mqttOnSubscribe(client, None, 99, [0])
        self.assertFalse(self.agent.mqttHealth()['server']['subscribed'])
        self.agent.mqttOnSubscribe(client, None, 1, [0])
        self.assertTrue(self.agent.mqttHealth()['server']['subscribed'])

    def test_disconnect_keeps_reason_and_reconnect_requires_fresh_suback(self):
        self.agent.manageMqttClients()
        self.addCleanup(self.agent.closeMqttClients)
        client = self.agent.clientOfServer
        self.agent.mqttOnConnectServer(client, None, {}, 0)
        self.agent.mqttOnSubscribe(client, None, 1, [0])
        self.agent.mqttOnDisconnect(client, None, 7)
        self.assertFalse(self.agent.mqttHealth()['server']['connected'])
        self.assertFalse(self.agent.mqttHealth()['server']['subscribed'])
        self.assertIn('reason_code=7', self.output.getvalue())
        self.agent.mqttOnConnectServer(client, None, {}, 0)
        self.agent.mqttOnSubscribe(client, None, 1, [0])
        self.assertFalse(self.agent.mqttHealth()['server']['subscribed'])
        self.agent.mqttOnSubscribe(client, None, 2, [0])
        self.assertTrue(self.agent.mqttHealth()['server']['subscribed'])

    def test_rejected_or_failed_subscription_does_not_report_ready(self):
        self.agent.manageMqttClients()
        self.addCleanup(self.agent.closeMqttClients)
        client = self.agent.clientOfServer
        self.agent.mqttOnConnectServer(client, None, {}, 0)
        self.agent.mqttOnSubscribe(client, None, 1, [128])
        self.assertFalse(self.agent.mqttHealth()['server']['subscribed'])
        self.assertTrue(client.disconnected)

    def test_subscribe_exception_is_contained_and_client_can_be_replaced(self):
        self.agent.manageMqttClients()
        self.addCleanup(self.agent.closeMqttClients)
        client = self.agent.clientOfServer
        with mock.patch.object(client, 'subscribe', side_effect=OSError('synthetic')):
            self.agent.mqttOnConnectServer(client, None, {}, 0)
        self.assertFalse(self.agent.mqttHealth()['server']['connected'])
        self.assertFalse(self.agent.mqttHealth()['server']['subscribed'])
        self.assertTrue(client.disconnected)
        self.agent._mqttStates['server']['nextStart'] = 0
        self.agent.manageMqttClients()
        self.assertIsNot(client, self.agent.clientOfServer)

    def test_two_distinct_owned_clients_and_no_duplicate_loops(self):
        self.agent.manageMqttClients()
        first = [s['client'] for s in self.agent._mqttStates.values()]
        self.assertIsNot(first[0], first[1])
        self.assertIs(self.agent.clientOfDevice, first[0])
        self.assertIs(self.agent.clientOfServer, first[1])
        self.agent.manageMqttClients()
        self.assertEqual([1, 1], [c.starts for c in first])
        self.agent.closeMqttClients()
        self.assertTrue(all(c.disconnected for c in first))
        self.assertTrue(all(s['client'] is None for s in self.agent._mqttStates.values()))

    def test_live_disconnected_loop_is_left_to_pahos_reconnect(self):
        self.agent.manageMqttClients()
        state = self.agent._mqttStates['device']
        client = state['client']
        self.agent.mqttOnDisconnect(client, None, 1)
        self.agent.manageMqttClients()
        self.assertIs(client, state['client'])
        self.assertEqual(1, client.starts)
        self.assertFalse(state['connected'])
        self.agent.closeMqttClients()

    def test_dead_loop_is_closed_before_replacement(self):
        self.agent.manageMqttClients()
        state = self.agent._mqttStates['device']
        old = state['client']
        sockets = [old._sock, old._sockpairR, old._sockpairW]
        state['thread'].alive = False
        state['nextStart'] = 0
        self.agent.manageMqttClients()
        self.assertIsNot(old, state['client'])
        self.assertTrue(old.disconnected)
        self.assertEqual(1, old.stops)
        self.assertTrue(all(sock.closed for sock in sockets))
        self.assertEqual(2, state['starts'])
        self.agent.closeMqttClients()

    def test_stuck_worker_prevents_second_receiver(self):
        self.agent.manageMqttClients()
        state = self.agent._mqttStates['device']
        old = state['client']
        old._thread.stuck = True
        with self.assertRaises(self.agent.MqttCleanupError):
            self.agent._closeMqttClient('device')
        self.assertIs(old, state['client'])
        self.assertEqual([45], old._thread.joins)
        old._thread.stuck = False
        self.agent.closeMqttClients()

    def test_start_failure_is_bounded_and_can_recover(self):
        original = self.agent.mqttConnectToDevice
        with mock.patch.object(self.agent, 'mqttConnectToDevice', side_effect=OSError('synthetic')):
            self.agent.manageMqttClients()
        state = self.agent._mqttStates['device']
        self.assertIsNone(state['client'])
        self.assertGreater(state['nextStart'], 0)
        state['nextStart'] = 0
        self.agent.manageMqttClients()
        self.assertIsNotNone(state['client'])
        self.assertEqual(('localhost', 1883), state['client'].connection)
        self.agent.closeMqttClients()

    def test_configuration_failure_closes_constructor_resources(self):
        before = len(Client.instances)
        with mock.patch.object(Client, 'connect_async', side_effect=OSError('synthetic')):
            self.agent.manageMqttClients()
        created = Client.instances[before:]
        self.assertEqual(2, len(created))
        for client in created:
            self.assertTrue(client.disconnected)
            self.assertIsNone(client._sockpairR)
            self.assertIsNone(client._sockpairW)
        self.assertTrue(all(s['client'] is None for s in self.agent._mqttStates.values()))

    def test_callback_exception_does_not_escape_and_next_message_works(self):
        with mock.patch.object(self.agent, 'processMessageFromAppToAgent', side_effect=FileNotFoundError('synthetic')):
            self.agent.mqttOnMessageDevice(None, None, types.SimpleNamespace(payload=b'{}'))
        identifier = self.start()
        payload = json.dumps({'functionUuid': identifier, 'finishedFunctionOnApp': 'drink',
                              'status': 'completed', 'response': 'ok'}).encode()
        self.agent.mqttOnMessageDevice(None, None, types.SimpleNamespace(payload=payload))
        self.assertEqual(1, len(self.reports))

    def test_invalid_payloads_cannot_kill_either_callback(self):
        for callback in (self.agent.mqttOnMessageDevice, self.agent.mqttOnMessageServer):
            for payload in (b'\xff', b'{broken', b'[]', b'null', b'"string"'):
                with self.subTest(callback=callback.__name__, payload=payload):
                    callback(None, None, types.SimpleNamespace(payload=payload))

    def test_apostrophe_is_preserved_in_valid_json(self):
        identifier = str(uuid.uuid4())
        payload = json.dumps({'davra-function': 'drink', 'functionParameterValues':
                              {'functionUuid': identifier, 'info': "O'Connor"}}).encode()
        self.agent.mqttOnMessageServer(None, None, types.SimpleNamespace(payload=payload))
        self.assertEqual("O'Connor", self.records()[identifier]['functionParameterValues']['info'])

    def test_legacy_job_announcement_remains_supported(self):
        with mock.patch.object(self.agent, 'checkForPendingJob') as check:
            self.agent.mqttOnMessageServer(None, None, types.SimpleNamespace(payload=b'davra.announcement:check-for-jobs'))
        check.assert_called_once_with()

    def test_connected_health_requires_connection_and_subscribe_callbacks(self):
        self.agent.manageMqttClients()
        state = self.agent._mqttStates['device']
        client = state['client']
        self.assertFalse(self.agent.mqttHealth()['device']['connected'])
        self.agent.mqttOnConnectDevice(client, None, {}, 0)
        self.assertTrue(self.agent.mqttHealth()['device']['connected'])
        self.assertFalse(self.agent.mqttHealth()['device']['subscribed'])
        self.agent.mqttOnSubscribe(client, None, 1, [0])
        self.assertTrue(self.agent.mqttHealth()['device']['subscribed'])
        self.agent.closeMqttClients()

    def test_stale_old_client_callback_does_not_mark_new_client_connected(self):
        self.agent.manageMqttClients()
        state = self.agent._mqttStates['device']
        old = state['client']
        state['thread'].alive = False
        state['nextStart'] = 0
        self.agent.manageMqttClients()
        self.agent.mqttOnConnectDevice(old, None, {}, 0)
        self.assertFalse(state['connected'])
        self.agent.closeMqttClients()

    def test_shutdown_does_not_replay_pending_water_command(self):
        identifier = self.start()
        self.agent.manageMqttClients()
        clients = [s['client'] for s in self.agent._mqttStates.values()]
        self.agent.closeMqttClients()
        self.assertIn(identifier, self.records())
        self.assertTrue(all(not c.published for c in clients))

    def test_main_action_failure_isolated_but_cleanup_failure_is_fatal(self):
        self.agent._mainAction('synthetic', lambda: (_ for _ in ()).throw(ValueError('synthetic')))
        called = []
        self.agent._mainAction('next', lambda: called.append(True))
        self.assertEqual([True], called)
        with self.assertRaises(self.agent.MqttCleanupError):
            self.agent._mainAction('cleanup', lambda: (_ for _ in ()).throw(self.agent.MqttCleanupError('synthetic')))


class LocalBroker:
    """Minimal MQTT 3.1.1 peer, only on a fresh loopback port; no product broker."""
    def __init__(self, idle_timeout=2):
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.port = self.listener.getsockname()[1]
        self.stopped = threading.Event()
        self.connections = []
        self.workers = []
        self.lock = threading.Lock()
        self.idle_timeout = idle_timeout
        self.pings = 0
        self.listener.settimeout(0.1)

    def start(self):
        self.listener.listen(8)
        self.thread = threading.Thread(target=self.accept)
        self.thread.start()

    def accept(self):
        while not self.stopped.is_set():
            try:
                connection, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            connection.settimeout(self.idle_timeout)
            with self.lock:
                self.connections.append(connection)
            worker = threading.Thread(target=self.serve, args=(connection,))
            self.workers.append(worker)
            worker.start()

    def read_exact(self, connection, size):
        result = b''
        while len(result) < size:
            value = connection.recv(size - len(result))
            if not value:
                raise EOFError()
            result += value
        return result

    def serve(self, connection):
        try:
            while not self.stopped.is_set():
                kind = self.read_exact(connection, 1)[0] >> 4
                length, shift = 0, 0
                while True:
                    value = self.read_exact(connection, 1)[0]
                    length += (value & 127) << shift
                    if not value & 128:
                        break
                    shift += 7
                body = self.read_exact(connection, length)
                if kind == 1:
                    connection.sendall(b'\x20\x02\x00\x00')
                elif kind == 8:
                    connection.sendall(b'\x90\x03' + body[:2] + b'\x00')
                elif kind == 12:
                    with self.lock:
                        self.pings += 1
                    connection.sendall(b'\xd0\x00')
                elif kind == 14:
                    break
        except (OSError, EOFError):
            pass
        finally:
            with self.lock:
                if connection in self.connections:
                    self.connections.remove(connection)
            connection.close()

    def publish(self, payload):
        topic = b'/agent'
        body = len(topic).to_bytes(2, 'big') + topic + payload
        self.assert_small(body)
        with self.lock:
            for connection in list(self.connections):
                connection.sendall(b'\x30' + bytes([len(body)]) + body)

    @staticmethod
    def assert_small(body):
        if len(body) >= 128:
            raise ValueError('test_packet_too_large')

    def drop_connections(self):
        with self.lock:
            for connection in list(self.connections):
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def close(self):
        self.stopped.set()
        self.listener.close()
        self.drop_connections()
        if hasattr(self, 'thread'):
            self.thread.join(3)
        for worker in self.workers:
            worker.join(3)
        if any(worker.is_alive() for worker in self.workers):
            raise RuntimeError('test_broker_worker_left_running')


@unittest.skipIf(native_paho is None, 'native Paho is checked on the deployment targets')
class NativePahoTests(AgentFixture):
    def until(self, predicate, timeout=12):
        deadline = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() > deadline:
                self.fail('native MQTT condition timed out')
            time.sleep(0.02)

    def test_installed_paho_reconnect_callback_and_dead_loop_cleanup(self):
        broker = LocalBroker()
        self.addCleanup(broker.close)
        self.agent.mqtt.Client = native_paho.Client
        self.lib.conf['mqttBrokerServerHost'] = '127.0.0.1'
        self.lib.conf['mqttBrokerServerPort'] = broker.port
        real_connect = native_paho.Client.connect_async
        # Route both factories to this test's peer, never the installed broker.
        def connect(client, host, port=1883, *args, **kwargs):
            return real_connect(client, '127.0.0.1', broker.port, *args, **kwargs)
        with mock.patch.object(native_paho.Client, 'connect_async', connect):
            self.agent.manageMqttClients()
            self.addCleanup(self.agent.closeMqttClients)
            first = [state['client'] for state in self.agent._mqttStates.values()]
            time.sleep(0.15)  # First connection is deliberately unavailable.
            broker.start()
            healthy = lambda: all(row['connected'] and row['subscribed'] for row in self.agent.mqttHealth().values())
            self.until(healthy)
            seen = []
            def processor(value):
                if value.get('raise'):
                    raise FileNotFoundError('synthetic_callback_failure')
                seen.append(value)
            with mock.patch.object(self.agent, 'processMessageFromAppToAgent', processor), \
                    mock.patch.object(self.agent, 'processMessageFromServerToAgent', processor):
                broker.publish(b'{invalid')
                broker.publish(b'{"raise":true}')
                broker.publish(b'{"observed":true}')
                self.until(lambda: len(seen) == 2)
            broker.drop_connections()
            self.until(lambda: all(row['disconnections'] >= 1 for row in self.agent.mqttHealth().values()))
            self.until(healthy)
            self.agent.manageMqttClients()
            self.assertEqual([1, 1], [row['loopStarts'] for row in self.agent.mqttHealth().values()])
            old = first[0]
            old.disconnect()
            old.loop_stop()
            state = self.agent._mqttStates['device']
            state['nextStart'] = 0
            self.agent.manageMqttClients()
            self.until(healthy)
            self.assertIsNot(old, state['client'])
            self.assertEqual(2, state['starts'])
            self.assertIsNone(old._sock)
            self.assertIsNone(old._sockpairR)
            self.agent.closeMqttClients()
            self.assertTrue(all(not client._thread or not client._thread.is_alive() for client in first))

    def test_keepalive_prevents_idle_peer_closure_with_real_packets(self):
        broker = LocalBroker(idle_timeout=2)
        self.addCleanup(broker.close)
        broker.start()
        self.agent.mqtt.Client = native_paho.Client
        self.lib.conf['mqttBrokerServerHost'] = '127.0.0.1'
        self.lib.conf['mqttBrokerServerPort'] = broker.port
        original_connect = native_paho.Client.connect_async
        disconnected = []
        control = native_paho.Client()
        control.on_connect = lambda c, u, f, r: c.subscribe('/agent')
        control.on_disconnect = lambda c, u, r: disconnected.append(r)
        self.addCleanup(control.loop_stop)
        self.addCleanup(control.disconnect)
        # Scale the same keepalive/idle-boundary relation to seconds. The
        # production value is independently asserted above and observed live.
        with mock.patch.object(self.agent, 'SERVER_MQTT_KEEPALIVE_SECONDS', 1), \
                mock.patch.object(native_paho.Client, 'connect_async',
                                  lambda c, h, port=1883, **kw: original_connect(c, '127.0.0.1', broker.port, **kw)):
            self.agent.manageMqttClients()
            self.addCleanup(self.agent.closeMqttClients)
            self.until(lambda: self.agent.mqttHealth()['server']['subscribed'])
            control.connect_async('127.0.0.1', broker.port, keepalive=60)
            control.loop_start()
            self.until(lambda: bool(disconnected))
            self.until(lambda: broker.pings >= 4, timeout=7)
            self.assertEqual(0, self.agent.mqttHealth()['server']['disconnections'])
            self.assertTrue(self.agent.mqttHealth()['server']['subscribed'])
            self.assertEqual(1, self.agent.mqttHealth()['server']['loopStarts'])


if __name__ == '__main__':
    unittest.main()
