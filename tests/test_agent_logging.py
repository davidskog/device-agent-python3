"""Fault and load tests for diagnostics. No real config, broker or network."""
import json
import os
import threading
import time
import types
from unittest import mock
from test_agent_reliability import AgentFixture


class LoggingTests(AgentFixture):
    def logger(self, remote=None, **kw):
        self.now = [0.0]
        self.records = []
        logger = self.lib.AgentLogger(local_sink=self.records.append, remote_sink=remote or (lambda r: None),
                                      clock=lambda: self.now[0], **kw)
        self.addCleanup(lambda: logger.stop(timeout=2))
        return logger

    def test_debug_is_filtered_before_queue_and_expires(self):
        log = self.logger()
        for _ in range(10000):log.emit('diagnostic', 'DEBUG', reason='routine')
        self.assertEqual([], self.records)
        self.assertEqual({}, log.workers)
        self.assertEqual(10000, log.totals['suppressed_debug'])
        log.enable_debug(10)
        log.emit('diagnostic', 'DEBUG', reason='temporary')
        self.assertTrue(log.flush())
        self.assertEqual(1, sum(r['level']=='DEBUG' for r in self.records))
        self.now[0] = 11
        log.poll();log.emit('diagnostic', 'DEBUG', reason='expired');log.flush()
        self.assertEqual(1, sum(r['level']=='DEBUG' for r in self.records))
        self.assertEqual(1, sum(r['event']=='debug_expired' for r in self.records))
        for value in (-1, 0, 601, '10'):
            with self.assertRaises(ValueError):log.enable_debug(value)

    def test_first_failure_repeat_interval_and_actual_recovery(self):
        log = self.logger()
        for _ in range(1000):log.problem('http', 'http_failed', method='PUT', endpoint='/api/v1/iotdata', http_status=503)
        log.flush();self.assertEqual(1, len(self.records))
        self.now[0] = 59;log.poll();log.flush();self.assertEqual(1, len(self.records))
        self.now[0] = 60;log.poll();log.flush()
        repeated = self.records[-1]
        self.assertEqual('problem_repeated', repeated['event']);self.assertEqual(1000, repeated['fields']['count'])
        log.problem('http', 'http_failed', method='PUT', endpoint='/api/v1/iotdata', http_status=401)
        log.flush();self.assertEqual(3, len(self.records))
        self.now[0] = 61;log.recovered('http');log.flush()
        self.assertEqual('problem_recovered', self.records[-1]['event'])
        self.assertEqual(1001, self.records[-1]['fields']['count'])
        self.assertEqual({}, log.episodes)

    def test_independent_error_groups_do_not_erase_each_other(self):
        log = self.logger()
        log.problem('device', 'mqtt_problem', role='device', reason='disconnected')
        log.problem('server', 'mqtt_problem', role='server', reason='disconnected')
        log.recovered('device');log.flush()
        self.assertEqual(1, len(log.episodes));self.assertEqual('server', next(iter(log.episodes))[0])

    def test_error_cardinality_and_queues_are_bounded(self):
        log = self.logger(episode_limit=8)
        for n in range(1000):log.problem('group'+str(n), 'http_failed', http_status=500+n)
        log.flush();self.assertEqual(8, len(log.episodes))
        self.assertEqual(992, log.totals['episode_overflow'])

    def test_slow_remote_does_not_block_local_or_caller_and_never_retries(self):
        entered, release = threading.Event(), threading.Event(); calls=[]
        def remote(record):
            calls.append(record);entered.set();release.wait(2)
            return types.SimpleNamespace(status_code=503)
        log = self.logger(remote)
        started = time.monotonic();log.emit('agent_started', forward=True, version='TLS_1_3')
        self.assertLess(time.monotonic()-started, .2);self.assertTrue(entered.wait(1))
        log.emit('command_received', function_uuid='11111111-1111-4111-8111-111111111111', function='drink')
        deadline=time.monotonic()+1
        while len(self.records)<2 and time.monotonic()<deadline:time.sleep(.005)
        self.assertEqual(2,len(self.records));self.assertEqual(1,len(calls))
        release.set();self.assertTrue(log.flush())
        self.assertEqual(1,len(calls));self.assertIn('log_remote_failed',[r['event']for r in self.records])

    def test_remote_exception_is_local_only_and_recovery_is_real(self):
        calls=[]
        def remote(record):
            calls.append(record)
            if len(calls)==1:raise OSError('DO_NOT_LOG_EXCEPTION_CONTENT')
            return types.SimpleNamespace(status_code=200)
        log=self.logger(remote)
        log.emit('agent_started',forward=True);log.flush()
        self.assertEqual(1,len(calls));self.assertEqual(1,len(log.episodes))
        log.emit('agent_stopped',forward=True);log.flush()
        self.assertEqual(2,len(calls));self.assertEqual({},log.episodes)
        self.assertNotIn('DO_NOT_LOG_EXCEPTION_CONTENT',json.dumps(self.records))

    def test_local_sink_failure_does_not_escape_and_is_recoverable(self):
        records=[];failed=[True]
        def sink(record):
            if failed[0]:raise PermissionError('private')
            records.append(record)
        log=self.logger();log.local_sink=sink
        log.emit('agent_started');self.assertTrue(log.flush());self.assertEqual(1,len(log.episodes))
        failed[0]=False;log.emit('command_received',function='drink');log.flush()
        self.assertEqual({},log.episodes)
        self.assertIn('problem_recovered',[r['event']for r in records])

    def test_overflow_prioritizes_error_without_blocking_or_unbounded_growth(self):
        entered,release=threading.Event(),threading.Event();records=[]
        def sink(record):entered.set();release.wait(2);records.append(record)
        log=self.logger(local_limit=2);log.local_sink=sink
        log.enable_debug(600);self.assertTrue(entered.wait(1))
        for _ in range(20):log.emit('diagnostic','DEBUG',reason='routine')
        log.emit('http_failed','ERROR',http_status=503)
        self.assertEqual(2,len(log.queues['local']));self.assertGreater(log.totals['local_dropped'],0)
        release.set();log.flush()
        self.assertTrue(any(r['event']=='http_failed'for r in records))

    def test_stop_is_bounded_and_no_second_live_worker_can_replace_blocked_one(self):
        entered,release=threading.Event(),threading.Event()
        def sink(record):entered.set();release.wait(2)
        log=self.logger();log.local_sink=sink;log.emit('agent_started');self.assertTrue(entered.wait(1))
        worker=log.workers['local'];started=time.monotonic();closed=log.stop(timeout=.05)
        self.assertLess(time.monotonic()-started,.3);self.assertFalse(closed['clean'])
        self.assertFalse(log.start());self.assertIs(worker,log.workers['local'])
        release.set();worker.join(1);self.assertTrue(log.stop()['clean'])
        self.assertTrue(log.start(restart=True));self.assertTrue(log.stop()['clean'])

    def test_counters_are_exact_with_concurrent_producers(self):
        log=self.logger()
        threads=[threading.Thread(target=lambda:[log.count('http_calls')for _ in range(2000)])for _ in range(8)]
        for t in threads:t.start()
        for t in threads:t.join()
        self.assertEqual(16000,log.totals['http_calls'])
        self.now[0]=300;log.poll();log.flush()
        self.assertEqual(16000,self.records[-1]['fields']['http_calls'])
        self.assertEqual(300,self.records[-1]['fields']['uptime_s'])
        self.now[0]=599;log.poll();log.flush();self.assertEqual(1,len(self.records))

    def test_fields_are_allowlisted_single_line_and_no_payload_is_retained(self):
        log=self.logger()
        secret='SECRET_CANARY_123'
        log.emit('http_failed','ERROR',password=secret,headers={'Authorization':secret},
                 script=secret,config={'apiToken':secret},reason='bad\n'+secret,
                 endpoint='/api/v1/iotdata?token='+secret,function_uuid=secret,http_status=503)
        log.flush();raw=json.dumps(self.records)
        self.assertNotIn(secret,raw);self.assertEqual('other_endpoint',self.records[0]['fields']['endpoint'])
        self.assertEqual(503,self.records[0]['fields']['http_status'])

    def test_journal_priorities_and_version_owned_file(self):
        for level,priority in [('DEBUG',7),('INFO',6),('WARN',4),('ERROR',3)]:
            row={'event':'diagnostic','level':level,'fields':{},'at':1}
            self.lib._write_local_log(row)
            self.assertIn('<'+str(priority)+'>',self.output.getvalue())
        path=self.root/'davra_agent_TLS_1_3.log';self.assertEqual(0o600,path.stat().st_mode&0o777)
        self.assertFalse((self.root/'davra_agent.log').exists())

    def test_symlink_log_destination_is_not_followed(self):
        secret=self.root/'protected';secret.write_text('unchanged')
        (self.root/'davra_agent_TLS_1_3.log').symlink_to(secret)
        with self.assertRaises(OSError):self.lib._write_local_log({'event':'diagnostic','level':'INFO','fields':{},'at':1})
        self.assertEqual('unchanged',secret.read_text())

    def test_telemetry_batch_is_one_request_without_per_metric_messages(self):
        self.agent.sendIotDataToServer({'sendIotData':json.dumps([{'name':'test.metric','value':i,'msg_type':'datum'}for i in range(1000)])})
        self.assertEqual(1,len(self.requests.calls))
        self.assertEqual(1000,self.lib._logger.totals['telemetry_datapoints_ok'])
        self.assertEqual('',self.log_output())

    def test_http_timeout_is_transport_error_and_never_a_logged_http_500(self):
        self.requests.failure=TimeoutError('PRIVATE_EXCEPTION_DETAIL')
        self.lib.httpPut('https://example.invalid/api/v1/iotdata?token=PRIVATE_QUERY',{'password':'PRIVATE_PAYLOAD'})
        text=self.log_output();self.assertEqual(2,len(self.requests.calls)) # data + one selected diagnostic
        self.assertIn('transport_error',text);self.assertNotIn('http_status=500',text)
        for value in ['PRIVATE_EXCEPTION_DETAIL','PRIVATE_QUERY','PRIVATE_PAYLOAD']:self.assertNotIn(value,text)

    def test_native_command_exception_preserves_return_without_unassigned_variable(self):
        with mock.patch('subprocess.run',side_effect=OSError('DO_NOT_LOG')):
            self.assertEqual('unknown',self.lib.runNativeCommand(['unavailable','SECRET_ARG']))
        text=self.log_output();self.assertIn('native_command_failed',text)
        self.assertNotIn('SECRET_ARG',text);self.assertNotIn('DO_NOT_LOG',text)

    def test_worker_start_failure_is_bounded_without_propagating(self):
        log=self.logger()
        with mock.patch('threading.Thread.start', side_effect=RuntimeError('NO_THREADS')):
            for _ in range(20):log.emit('agent_started')
        self.assertEqual(1,log.totals['local_worker_failures'])
        self.assertEqual(1,log.totals['remote_worker_failures'])
        self.now[0]=2
        log.start();self.assertTrue(log.flush())
        self.assertEqual(20,len(self.records))
        self.assertEqual(1,log.totals['local_worker_starts'])

    def test_closed_logger_cannot_be_restarted_by_late_emit(self):
        log=self.logger();log.emit('agent_started');log.flush();log.stop()
        workers=dict(log.workers)
        self.assertFalse(log.emit('command_received'))
        self.assertEqual(workers,log.workers)
        self.assertTrue(all(not w.is_alive() for w in log.workers.values()))

    def test_logging_exception_cannot_escape_to_mqtt_command_handling(self):
        with mock.patch.object(self.lib._logger,'emit',side_effect=OSError('SINK_FAILURE')):
            self.assertFalse(self.lib.logEvent('command_received'))
            identifier=self.start()
        self.assertIn(identifier,super().records())

    def test_invalid_counter_name_and_huge_integer_are_safe(self):
        log=self.logger();log.count('version',100)
        self.assertNotIn('version',log.totals)
        log.emit('diagnostic',count=10**1000);log.flush()
        self.assertEqual(10**15,self.records[0]['fields']['count'])

    def test_overflow_is_reported_and_old_log_files_are_untouched(self):
        log=self.logger(episode_limit=1)
        log.problem('one','http_failed',http_status=500)
        for n in range(5):log.problem('other'+str(n),'http_failed',http_status=503)
        log.poll();log.flush()
        overflow=[r for r in self.records if r['event']=='log_overflow']
        self.assertEqual(1,len(overflow));self.assertEqual(5,overflow[0]['fields']['count'])
        log.poll();log.flush();self.assertEqual(1,len([r for r in self.records if r['event']=='log_overflow']))
        old=self.root/'davra_agent.log';old.write_text('original')
        self.lib._write_local_log({'event':'diagnostic','level':'INFO','fields':{},'at':1})
        self.assertEqual('original',old.read_text())

    def test_poll_recovers_failed_worker_start_without_new_message(self):
        log=self.logger()
        with mock.patch('threading.Thread.start',side_effect=RuntimeError('temporary')):
            log.emit('agent_started')
        self.now[0]=2;log.poll();log.flush()
        self.assertEqual(1,len(self.records))
        self.assertEqual('agent_started',self.records[0]['event'])

    def test_log_descriptor_closes_on_file_validation_error(self):
        real_open=self.lib.os.open;opened=[]
        def track(*args,**kwargs):
            descriptor=real_open(*args,**kwargs);opened.append(descriptor);return descriptor
        with mock.patch.object(self.lib.os,'open',side_effect=track),mock.patch.object(self.lib.os,'fstat',side_effect=OSError('test')):
            with self.assertRaises(OSError):self.lib._write_local_log({'event':'diagnostic','level':'INFO','fields':{},'at':1})
        for descriptor in opened:
            with self.assertRaises(OSError):os.fstat(descriptor)

    def test_emit_inflight_at_shutdown_cannot_resurrect_workers(self):
        log=self.logger();entered,release=threading.Event(),threading.Event();original=log.start;results=[]
        def delayed_start(*args,**kwargs):entered.set();release.wait(1);return original(*args,**kwargs)
        with mock.patch.object(log,'start',side_effect=delayed_start):
            thread=threading.Thread(target=lambda:results.append(log.emit('agent_started')))
            thread.start();self.assertTrue(entered.wait(1));self.assertTrue(log.stop()['clean'])
            release.set();thread.join(1)
        self.assertEqual([False],results);self.assertEqual({},log.workers)

    def test_legacy_payloads_never_enter_any_sink_even_in_debug(self):
        self.lib.enableDebugLogging(600)
        value='LOG_PAYLOAD_SECRET_CANARY'
        for level in ['DEBUG','INFO','WARN','ERROR']:
            self.lib.log({'password':value},level)
            self.lib.log(json.dumps({'Authorization':value}),level)
        self.lib.flushLogging(timeout=3)
        self.assertNotIn(value,self.log_output());self.assertEqual([],self.requests.calls)
        path=self.root/'davra_agent_TLS_1_3.log'
        self.assertNotIn(value,path.read_text())

    def test_summary_reports_zero_errors_and_does_not_send_http(self):
        calls=[];log=self.logger(remote=lambda r:calls.append(r))
        self.now[0]=300;log.poll();log.flush()
        self.assertEqual([],calls);summary=self.records[-1]
        self.assertEqual('driftstatus',summary['event'])
        self.assertEqual(0,summary['fields']['http_failed'])
        self.assertEqual(0,summary['fields']['telemetry_batches_failed'])
        self.assertEqual(0,summary['fields']['local_dropped'])

    def test_argument_parser_accepts_exact_legacy_unit_without_mutating_argv(self):
        legacy=['>>','/var/log/davra_agent.log','2>&1'];original=list(legacy)
        self.assertEqual(0,self.agent.parseAgentArguments([]))
        self.assertEqual(0,self.agent.parseAgentArguments(legacy));self.assertEqual(original,legacy)
        self.assertEqual(10,self.agent.parseAgentArguments(['--debug-seconds','10']+legacy))
        self.assertEqual(600,self.agent.parseAgentArguments(['--debug-seconds','600']))
        for args in [['--unknown'],['>>','/other/log','2>&1'],['--debug-seconds','601'],
                     ['--debug-seconds','-1'],['--debug-seconds','SECRET_ARG']]:
            with self.assertRaises(ValueError):self.agent.parseAgentArguments(args)

    def test_actual_entrypoint_with_committed_service_arguments(self):
        import runpy,signal,shlex,sys
        from pathlib import Path
        source=Path(__file__).resolve().parents[1]/'davra-agent'
        unit=(source/'davra_agent.service').read_text()
        command=next(line.split('=',1)[1]for line in unit.splitlines()if line.startswith('ExecStart='))
        argv=shlex.split(command)[2:]
        self.assertEqual(['>>','/var/log/davra_agent.log','2>&1'],argv)
        def install_handler(number,handler):
            if number==signal.SIGTERM:handler(number,None)
        with mock.patch.object(sys,'argv',[str(source/'davra_agent.py')]+argv),\
             mock.patch('signal.signal',side_effect=install_handler),\
             mock.patch.object(self.lib,'updateDeviceLabelOnServer'),\
             mock.patch.object(self.lib,'registerDeviceCapability'):
            with self.assertRaises(SystemExit)as closed:
                runpy.run_path(str(source/'davra_agent.py'),run_name='__main__')
        self.assertEqual(0,closed.exception.code)
        self.assertIn('event=agent_started',self.log_output())
        self.assertIn('event=agent_stopped',self.log_output())
        self.assertTrue(all(not worker.is_alive()for worker in self.lib._logger.workers.values()))
