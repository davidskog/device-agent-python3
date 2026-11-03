# Utility functions for other programs
#
import subprocess
import os, string
import time, requests, os.path
from requests.auth import HTTPBasicAuth
import json 
from pprint import pprint
import sys
import uuid
import threading
import tempfile
import collections
import math
import re
import stat
from urllib.parse import urlsplit
from datetime import datetime

# Update this when anything changes in the agent
davraAgentVersion = "TLS_1_4"

# Short, process-local critical sections for Agent state and local log files.
# Never hold either lock while doing HTTP, MQTT or running an application.
stateLock = threading.RLock()
_logLock = threading.RLock()

installationDir = "/usr/bin/davra"
# Config file for the agent running on this device
agentConfigFile = installationDir + "/config.json"
# Where logs are saved by default
logDir = "/var/log"
# Flags to indicate cache entries
flagNewCapabilityReadyToReport = False


# Load configuration if it exists
conf = {}
def loadConfiguration():
    global conf
    try:
        if(os.path.isfile(agentConfigFile) is True):
            with open(agentConfigFile) as data_file:
                conf = json.load(data_file)
    except:
        print('<3>ERROR: Agentkonfiguration kunde inte läsas', flush=True)
loadConfiguration()


def getConfiguration():
    return conf


# Update (or insert) a configuration item with a value
def upsertConfigurationItem(itemKey, itemValue):
    global conf
    loadConfiguration()
    if(os.path.isfile(agentConfigFile) is True):
        # If this a new key or an alteration of the current config
        if((itemKey in conf) == False or conf[itemKey] != itemValue):
            # Update the item
            conf[itemKey] = itemValue
            # Write the full config file to disk
            with open(agentConfigFile, 'w') as outfile:
                json.dump(conf, outfile, indent=4)
            time.sleep(0.2)
            # Update local cached version of configs
            loadConfiguration()
            # Report the new configuration to the server as an event
            reportDeviceConfigurationToServer()
    return


# Send all of the configuration file to the server as an event
def reportDeviceConfigurationToServer():
    dataToSend = { 
        "UUID": conf['UUID'],
        "name": "davra.agent.configured",
        "value": {
            'deviceConfig': conf
        },
        "msg_type": "event"
    }
    # Inform user of the overall data being sent for a single metric
    logEvent('config_reported', 'DEBUG', state='sending')
    sendDataToServer(dataToSend)
    logEvent("config_reported", "DEBUG", state="returned")
    return

# All runtime diagnostics enter this bounded, structured path. Network and
# file work happens only in the two owned workers, never in MQTT callbacks.
_LOG_LEVELS = {'DEBUG': 10, 'INFO': 20, 'WARN': 30, 'WARNING': 30, 'ERROR': 40}
_LOG_PRIORITIES = {'DEBUG': 7, 'INFO': 6, 'WARN': 4, 'ERROR': 3}
_LOG_LABELS = {
    'agent_started': 'Agent startad', 'agent_stopped': 'Agent stoppad',
    'agent_action_failed': 'Agentarbete misslyckades', 'agent_config_invalid': 'Agentkonfiguration ofullständig',
    'driftstatus': 'Driftstatus', 'mqtt_loop_started': 'MQTT-nätloop startad',
    'mqtt_connected': 'MQTT ansluten; prenumeration inväntas', 'mqtt_ready': 'MQTT redo',
    'mqtt_problem': 'MQTT-fel', 'command_received': 'Kommando mottaget',
    'command_finished': 'Kommando avslutat', 'command_rejected': 'Kommando avvisat',
    'command_duplicate': 'Dubblett ignorerad', 'command_ack_ignored': 'Kvittens ignorerad',
    'command_report': 'Kommandoresultat rapporterat', 'job_received': 'Jobb mottaget',
    'job_running': 'Jobb startat', 'job_finished': 'Jobb avslutat',
    'http_failed': 'HTTP-anrop misslyckades', 'problem_repeated': 'Upprepat fel',
    'problem_recovered': 'Återställd', 'log_remote_failed': 'Fjärrloggning misslyckades',
    'log_local_failed': 'Lokal loggning misslyckades', 'log_overflow': 'Loggposter kunde inte köas',
    'debug_started': 'Tidsbegränsad DEBUG aktiverad', 'debug_expired': 'DEBUG avslutad',
    'legacy_log': 'Äldre diagnostik; fritt innehåll utelämnat',
    'capabilities_reported': 'Förmågor rapporterade', 'capability_registered': 'Förmåga registrerad',
    'capability_removed': 'Förmåga borttagen', 'native_command_failed': 'Lokalt program misslyckades',
    'native_command_finished': 'Lokalt program avslutat', 'config_reported': 'Konfiguration rapporterad',
    'script_received': 'Skriptjobb mottaget', 'script_finished': 'Skriptjobb avslutat',
    'package_received': 'Installationsjobb mottaget', 'package_finished': 'Installationsjobb avslutat',
    'agent_function_started': 'Agentfunktion startad', 'metric_invalid': 'Ogiltig datapost',
    'function_state_invalid': 'Ogiltigt kommandotillstånd', 'diagnostic': 'Diagnostik',
}
_LOG_FIELDS = frozenset((
    'version', 'role', 'state', 'reason', 'error_class', 'reason_code', 'http_status',
    'method', 'endpoint', 'elapsed_ms', 'function', 'function_uuid', 'job_uuid',
    'result', 'reporting', 'count', 'new_count', 'period_s', 'duration_ms',
    'uptime_s', 'loop_starts', 'keepalive_s', 'reconnect_ms', 'sink', 'exit_code',
    'interval_s', 'mqtt_local', 'mqtt_davra', 'local_queue', 'remote_queue',
    'local_busy', 'remote_busy', 'local_workers', 'remote_workers', 'message_omitted',
    'cause_event', 'pending', 'fields_omitted', 'http_calls', 'http_ok', 'http_failed',
    'http_elapsed_ms', 'http_max_ms', 'http_mean_ms', 'http_cached_sessions', 'http_reused_connections',
    'http_pool_close_failures', 'telemetry_batches_ok', 'telemetry_batches_failed',
    'telemetry_datapoints_ok', 'telemetry_datapoints_failed', 'invalid_datapoints',
    'local_dropped', 'remote_dropped', 'suppressed_debug', 'episode_overflow',
    'local_write_failures', 'remote_write_failures', 'local_worker_starts', 'remote_worker_starts',
    'metric_names', 'datapoints', 'device_uuid',
    'local_worker_failures', 'remote_worker_failures',
))
_LOG_COUNTERS = frozenset(name for name in _LOG_FIELDS if name.startswith(('http_', 'telemetry_')))
_LOG_COUNTERS |= frozenset(('invalid_datapoints', 'local_dropped', 'remote_dropped', 'suppressed_debug',
                           'episode_overflow', 'local_write_failures', 'remote_write_failures'))


def _safe_log_fields(fields):
    result = {}
    for key, value in fields.items():
        if key not in _LOG_FIELDS or value is None:
            continue
        if isinstance(value, bool):
            result[key] = value
        elif isinstance(value, int) or isinstance(value, float) and math.isfinite(value):
            result[key] = max(-10**15, min(10**15, value))
        elif isinstance(value, str):
            if key in ('function_uuid', 'job_uuid', 'device_uuid'):
                if not re.fullmatch(r'[a-fA-F0-9]{8}(?:-[a-fA-F0-9]{4}){3}-[a-fA-F0-9]{12}', value):
                    continue
            elif key == 'endpoint':
                if not re.fullmatch(r'/api/v1/(?:iotdata(?:/meta-data)?|logs|jobs(?:/[a-fA-F0-9-]+){0,2}|devices(?:/[a-fA-F0-9-]+)?)', value):
                    value = 'other_endpoint'
            elif not re.fullmatch(r'[A-Za-z0-9_.:/-]{1,120}', value):
                continue
            result[key] = value
        elif key == 'metric_names' and isinstance(value, (list, tuple)):
            result[key] = ','.join(name for name in value[:8]
                                  if isinstance(name, str) and re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', name))
    return result


class AgentLogger:
    """Bounded best-effort diagnostics, with no data/command resend semantics."""
    def __init__(self, local_sink=None, remote_sink=None, clock=None,
                 local_limit=512, remote_limit=64, episode_limit=128):
        self.clock = clock or time.monotonic
        self.started_at = self.clock()
        self.last_summary = self.started_at
        self.local_sink = local_sink or _write_local_log
        self.remote_sink = remote_sink or _send_remote_log
        self.limits = {'local': local_limit, 'remote': remote_limit}
        self.episode_limit = episode_limit
        self.condition = threading.Condition(threading.RLock())
        self.queues = {'local': collections.deque(), 'remote': collections.deque()}
        self.workers = {}
        self.busy = {'local': 0, 'remote': 0}
        self.totals = collections.Counter()
        self.last_totals = collections.Counter()
        self.episodes = {}
        self.mqtt = {'device': 'unknown', 'server': 'unknown'}
        self.debug_until = 0
        self.stopping = False
        self.stop_deadline = None
        self.start_after = {'local': 0, 'remote': 0}
        self.start_delay = {'local': 2, 'remote': 2}
        self.overflow_announced = {'local_dropped': None, 'remote_dropped': None, 'episode_overflow': None}
        self.overflow_reported = collections.Counter()

    def start(self, restart=False):
        with self.condition:
            if self.stopping:
                if not restart or any(t.is_alive() for t in self.workers.values()):
                    return False
                self.stopping = False
                self.stop_deadline = None
            for sink in ('local', 'remote'):
                current = self.workers.get(sink)
                if current is None or not current.is_alive():
                    if self.clock() < self.start_after[sink]:
                        continue
                    worker = threading.Thread(target=self._work, args=(sink,), name='davra-log-' + sink, daemon=True)
                    self.workers[sink] = worker
                    try:
                        worker.start()
                        self.totals[sink + '_worker_starts'] += 1
                        self.start_delay[sink] = 2
                    except Exception:
                        self.totals[sink + '_worker_failures'] += 1
                        self.start_after[sink] = self.clock() + self.start_delay[sink]
                        self.start_delay[sink] = min(60, self.start_delay[sink] * 2)
            return True

    def _put(self, sink, record):
        rows = self.queues[sink]
        if len(rows) >= self.limits[sink]:
            incoming = _LOG_LEVELS[record['level']]
            victim = next((i for i, row in enumerate(rows) if _LOG_LEVELS[row['level']] < incoming), None)
            if victim is None:
                self.totals[sink + '_dropped'] += 1
                return False
            del rows[victim]
            self.totals[sink + '_dropped'] += 1
        rows.append(record)
        self.condition.notify_all()
        return True

    def emit(self, event, level='INFO', forward=False, **fields):
        level = 'WARN' if level == 'WARNING' else level
        if event not in _LOG_LABELS or level not in _LOG_PRIORITIES:
            event, level, fields, forward = 'legacy_log', 'WARN', {'message_omitted': True}, False
        with self.condition:
            if level == 'DEBUG' and self.clock() >= self.debug_until:
                self.totals['suppressed_debug'] += 1
                return False
            if self.stopping:
                self.totals['local_dropped'] += 1
                return False
        if not self.start():
            return False
        record = {'event': event, 'level': level, 'fields': _safe_log_fields(fields), 'at': time.time()}
        with self.condition:
            accepted = self._put('local', record)
            # Only explicitly selected diagnostics use this sink. No recursive
            # remote error, automatic retry, or telemetry batch enters it.
            if forward and event not in ('log_remote_failed', 'log_local_failed', 'legacy_log'):
                self._put('remote', record)
            return accepted

    def count(self, name, amount=1):
        if name not in _LOG_COUNTERS or not isinstance(amount, int) or amount < 0:
            return
        with self.condition:
            self.totals[name] += amount

    def enable_debug(self, seconds):
        if not isinstance(seconds, int) or not 1 <= seconds <= 600:
            raise ValueError('debug_seconds_must_be_1_to_600')
        with self.condition:
            self.debug_until = self.clock() + seconds
        self.emit('debug_started', period_s=seconds)

    def health(self, health):
        with self.condition:
            for role in self.mqtt:
                h = health.get(role, {})
                self.mqtt[role] = 'ready' if h.get('connected') and h.get('subscribed') and h.get('loopAlive') else 'not_ready'

    def problem(self, group, event, level='ERROR', forward=False, **fields):
        fields = _safe_log_fields(fields)
        signature = tuple((key, fields.get(key)) for key in ('role', 'reason', 'reason_code', 'error_class', 'method', 'endpoint', 'http_status'))
        key = (group, event, signature)
        now = self.clock()
        with self.condition:
            row = self.episodes.get(key)
            if row:
                row['count'] += 1
                row['last'] = now
                row['fields'] = fields
                return
            if len(self.episodes) >= self.episode_limit:
                self.totals['episode_overflow'] += 1
                return
            self.episodes[key] = {'group': group, 'event': event, 'level': level, 'forward': forward,
                                  'fields': fields, 'count': 1, 'reported': 1, 'first': now, 'last': now, 'announced': now}
        self.emit(event, level, forward, **fields)

    def recovered(self, group):
        now = self.clock()
        with self.condition:
            rows = [self.episodes.pop(key) for key in list(self.episodes) if self.episodes[key]['group'] == group]
        if rows:
            self.emit('problem_recovered', forward=any(r['forward'] for r in rows),
                      cause_event=rows[0]['event'], count=sum(r['count'] for r in rows),
                      duration_ms=int((now-min(r['first'] for r in rows))*1000),
                      **{k: rows[0]['fields'][k] for k in ('role', 'method', 'endpoint') if k in rows[0]['fields']})

    def poll(self):
        now = self.clock()
        pending = []
        with self.condition:
            if self.stopping:
                return
        self.start()
        with self.condition:
            if self.debug_until and now >= self.debug_until:
                self.debug_until = 0
                pending.append(('debug_expired', 'INFO', False, {}))
            for row in self.episodes.values():
                if row['count'] > row['reported'] and now-row['announced'] >= 60:
                    fields = dict(row['fields'], cause_event=row['event'], count=row['count'],
                                  new_count=row['count']-row['reported'], duration_ms=int((now-row['first'])*1000))
                    row['reported'], row['announced'] = row['count'], now
                    pending.append(('problem_repeated', row['level'], row['forward'], fields))
            for key, announced in self.overflow_announced.items():
                count = self.totals[key]-self.overflow_reported[key]
                if count and (announced is None or now-announced >= 60):
                    self.overflow_announced[key], self.overflow_reported[key] = now, self.totals[key]
                    pending.append(('log_overflow', 'ERROR', False, {'sink': key, 'count': count}))
            if now-self.last_summary >= 300:
                totals = self.totals.copy()
                names = ('http_calls', 'http_ok', 'http_failed', 'telemetry_batches_ok',
                         'telemetry_batches_failed', 'telemetry_datapoints_ok', 'telemetry_datapoints_failed',
                         'invalid_datapoints', 'local_dropped', 'remote_dropped', 'episode_overflow',
                         'local_write_failures', 'remote_write_failures', 'suppressed_debug',
                         'local_worker_failures', 'remote_worker_failures')
                fields = {name: totals[name]-self.last_totals[name] for name in names}
                transport = _httpTransport.snapshot()
                fields.update(version=davraAgentVersion, period_s=int(now-self.last_summary),
                              uptime_s=int(now-self.started_at), mqtt_local=self.mqtt['device'], mqtt_davra=self.mqtt['server'],
                              local_queue=len(self.queues['local']), remote_queue=len(self.queues['remote']),
                              local_workers=int(bool(self.workers.get('local') and self.workers['local'].is_alive())),
                              remote_workers=int(bool(self.workers.get('remote') and self.workers['remote'].is_alive())),
                              http_max_ms=totals['http_max_ms'],
                              http_mean_ms=int((totals['http_elapsed_ms']-self.last_totals['http_elapsed_ms']) /
                                               max(1, fields['http_calls'])),
                              http_cached_sessions=transport['cached'],
                              http_reused_connections=transport['reused'],
                              http_pool_close_failures=transport['closeFailures'])
                pending.append(('driftstatus', 'INFO', False, fields))
                self.last_summary, self.last_totals = now, totals
        for event, level, forward, fields in pending:
            self.emit(event, level, forward, **fields)

    def _work(self, sink):
        while True:
            with self.condition:
                while not self.queues[sink]:
                    if self.stopping:
                        return
                    self.condition.wait(0.5)
                if self.stopping and time.monotonic() >= self.stop_deadline:
                    self.totals[sink+'_dropped'] += len(self.queues[sink])
                    self.queues[sink].clear()
                    self.condition.notify_all()
                    return
                record = self.queues[sink].popleft()
                self.busy[sink] += 1
            try:
                if sink == 'local':
                    self.local_sink(record)
                    self.recovered('log_local')
                else:
                    response = self.remote_sink(record)
                    if response is not None and not 200 <= response.status_code < 300:
                        fields = _http_log_fields(response)
                        self.count('remote_write_failures')
                        self.problem('log_remote', 'log_remote_failed', sink='remote', **fields)
                    else:
                        self.recovered('log_remote')
            except Exception as error:
                self.count(sink+'_write_failures')
                self.problem('log_' + sink, 'log_' + sink + '_failed', sink=sink, error_class=type(error).__name__)
            finally:
                with self.condition:
                    self.busy[sink] -= 1
                    self.condition.notify_all()

    def flush(self, timeout=2):
        deadline = time.monotonic() + timeout
        with self.condition:
            while any(self.queues.values()) or any(self.busy.values()):
                remaining = deadline-time.monotonic()
                if remaining <= 0:
                    return False
                self.condition.wait(remaining)
        return True

    def stop(self, timeout=3):
        deadline = time.monotonic() + timeout
        with self.condition:
            self.stopping, self.stop_deadline = True, deadline
            self.condition.notify_all()
            workers = list(self.workers.values())
        for worker in workers:
            if worker is not threading.current_thread() and worker.ident is not None:
                worker.join(max(0, deadline-time.monotonic()))
        with self.condition:
            return {'clean': not any(t.is_alive() for t in workers) and not any(self.queues.values()),
                    'remaining': [t.name for t in workers if t.is_alive()],
                    'pending': sum(len(q) for q in self.queues.values()), 'totals': dict(self.totals)}


def _log_line(record):
    fields = ' '.join(key+'='+str(value).lower() if isinstance(value, bool) else key+'='+str(value)
                      for key, value in sorted(record['fields'].items()))
    return record['level']+': '+_LOG_LABELS[record['event']]+' event='+record['event']+(' '+fields if fields else '')


def _write_local_log(record):
    line = _log_line(record)
    failures = []
    try:
        print('<'+str(_LOG_PRIORITIES[record['level']])+'>'+line, flush=True)
    except (OSError, ValueError) as error:
        failures.append(type(error).__name__)
    try:
        with _logLock:
            # The old file remains untouched, including potentially sensitive
            # historic content. Only this version-owned mirror rotates.
            filename = os.path.join(logDir, 'davra_agent_'+davraAgentVersion+'.log')
            for item in (filename, filename+'.1', filename+'.2'):
                if os.path.lexists(item):
                    info = os.lstat(item)
                    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                        raise OSError('unsafe_log_file')
            if os.path.exists(filename) and os.path.getsize(filename) >= 10000000:
                if os.path.exists(filename+'.1'):
                    os.replace(filename+'.1', filename+'.2')
                os.replace(filename, filename+'.1')
            descriptor = os.open(filename, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600)
            try:
                info = os.fstat(descriptor)
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise OSError('unsafe_log_file')
                stream = os.fdopen(descriptor, 'a')
                descriptor = None
                with stream:
                    stamp = datetime.fromtimestamp(record['at']).isoformat(timespec='seconds')
                    stream.write(stamp+' '+line+'\n')
            finally:
                if descriptor is not None:
                    os.close(descriptor)
    except (OSError, ValueError) as error:
        failures.append(type(error).__name__)
    if failures:
        raise OSError('local_log_sink_failed')


def _send_remote_log(record):
    return sendLogToServer({'UUID': conf['UUID'], 'name': 'davra.log',
                           'value': {'severity': record['level'], 'message': _log_line(record)}})


def _http_log_fields(response):
    if getattr(response, 'transport_error', None):
        return {'state': 'transport_error', 'error_class': response.transport_error}
    return {'http_status': response.status_code}


_logger = AgentLogger()

def _log_safely(action, *args, **fields):
    try:
        return action(*args, **fields)
    except Exception:
        # Logging is best effort. Never let its failure escape to the MQTT
        # receiver or start a recursive diagnostic through the failed sink.
        try:
            with _logger.condition:
                _logger.totals['local_dropped'] += 1
        except Exception:
            pass
        return False

def logEvent(event, severity='INFO', forward=False, **fields):
    return _log_safely(_logger.emit, event, severity, forward, **fields)

def logProblem(group, event, severity='ERROR', forward=False, **fields):
    return _log_safely(_logger.problem, group, event, severity, forward, **fields)

def logRecovered(group):
    _log_safely(_logger.recovered, group)

def logHttpFields(response):
    return _http_log_fields(response)

def countDiagnostic(name, amount=1):
    _log_safely(_logger.count, name, amount)

def pollLogging(health=None):
    if health is not None:
        _log_safely(_logger.health, health)
    _log_safely(_logger.poll)

def flushLogging(timeout=2):
    return _logger.flush(timeout)

def closeLogging(timeout=3):
    return _logger.stop(timeout)

def enableDebugLogging(seconds):
    _logger.enable_debug(seconds)

# Compatibility entry points never serialize arbitrary legacy strings/objects.
# Runtime call sites below use structured events instead.
def log(log_msg, severity='DEBUG', forward=True):
    return logEvent('legacy_log', severity, message_omitted=True)

def logToServer(severity, message):
    return logEvent('legacy_log', severity, message_omitted=True)

def logDebug(log_msg):
    return log(log_msg, 'DEBUG')

def logInfo(log_msg):
    return log(log_msg, 'INFO')

def logWarning(log_msg):
    return log(log_msg, 'WARN')

def logError(log_msg):
    return log(log_msg, 'ERROR')


def getHeadersForRequests():
    headersTmp = {'Accept' : 'application/json', \
        'Content-Type' : 'application/json', \
        'Authorization': 'Bearer ' + conf['apiToken']}
    return headersTmp


def createMetricOnServer(metricName, metricUnits, metricDescription):
    #contents = '[{ "name": "' + metricName + '", '\
    #    + '"label": "' + metricName + '", '\
    #    + '"description": "' + metricDescription + '", '\
    #    + '"semantics": "metric" }]'
    contents = [ { "name": metricName, "label": metricName, "description": metricDescription, "semantics": "metric" } ]
    headers = getHeadersForRequests()
    r = httpPost(conf['server'] + '/api/v1/iotdata/meta-data', contents)
    if(r.status_code == 200):
        pass  # The shared HTTP path records the outcome.
    else:
        pass  # The shared HTTP path records the outcome.
    return


# When sending iot data, this is a shorter function to use then regular put
def sendDataToServer(dataToSend):
    responseFromServer = httpPut(conf['server'] + '/api/v1/iotdata', dataToSend)
    return(responseFromServer)

def sendLogToServer(dataToSend):
    responseFromServer = _httpRequest('PUT', conf['server'] + '/api/v1/logs', dataToSend, diagnostic=False, timeout=3)
    return(responseFromServer)


# For when a http request fails, use this to return a similar object
class emptyRequestsObject(object):
    status_code = 500
    content = ""


def _newHttpSession():
    session = requests.Session()
    try:
        for scheme in ('http://', 'https://'):
            session.mount(scheme, requests.adapters.HTTPAdapter(
                max_retries=0, pool_connections=1, pool_maxsize=1, pool_block=True))
    except BaseException:
        try:
            session.close()
        except Exception:
            pass
        raise
    return session


class AgentHttpTransport(object):
    """Reuse connections within one caller thread, never replay a request.

    No shared Session, sender queue or network call under the registry lock.
    Dead-thread sessions are closed on the next admission. Overflow callers
    retain the former one-call lifetime instead of growing the idle cache.
    """
    def __init__(self, factory=None, max_cached=8):
        if type(max_cached) is not int or not 1 <= max_cached <= 32:
            raise ValueError('invalid_http_cache_bound')
        self.factory = factory or _newHttpSession
        self.max_cached = max_cached
        self.lock = threading.Lock()
        self.entries = {}
        self.closed = False
        self.active = 0
        self.created = 0
        self.reused = 0
        self.close_failures = 0

    def _close(self, sessions):
        for session in sessions:
            try:
                session.close()
            except Exception:
                with self.lock:
                    self.close_failures += 1

    def request(self, method, destination, **kwargs):
        owner = threading.current_thread()
        expired = []
        try:
            with self.lock:
                if self.closed:
                    raise RuntimeError('http_transport_closed')
                for thread, entry in list(self.entries.items()):
                    if not thread.is_alive() and not entry['busy']:
                        expired.append(entry['session'])
                        del self.entries[thread]
                entry = self.entries.get(owner)
                if entry is not None:
                    if entry['busy']:
                        raise RuntimeError('http_transport_reentrant')
                    self.reused += 1
                else:
                    entry = {'session': self.factory(), 'busy': False,
                             'cached': len(self.entries) < self.max_cached}
                    self.created += 1
                    if entry['cached']:
                        self.entries[owner] = entry
                entry['busy'] = True
                self.active += 1
        except BaseException:
            self._close(expired)
            raise
        self._close(expired)
        session = entry['session']
        failed = False
        try:
            # Independent requests never retained response cookies. Pooling
            # sockets must not introduce cookie-based state or shared auth.
            session.cookies.clear()
            return session.request(method, destination, **kwargs)
        except BaseException:
            failed = True
            raise
        finally:
            try:
                session.cookies.clear()
            except Exception:
                failed = True
            with self.lock:
                entry['busy'] = False
                self.active -= 1
                retire = failed or self.closed or not entry['cached']
                if retire and self.entries.get(owner) is entry:
                    del self.entries[owner]
            if retire:
                self._close([session])

    def close(self):
        with self.lock:
            self.closed = True
            idle = [entry['session'] for entry in self.entries.values() if not entry['busy']]
            self.entries = {thread: entry for thread, entry in self.entries.items() if entry['busy']}
            active = self.active
        self._close(idle)
        with self.lock:
            clean = active == 0 and self.close_failures == 0
        return {'clean': clean, 'active': active}

    def snapshot(self):
        with self.lock:
            return {'cached': len(self.entries), 'active': self.active,
                    'created': self.created, 'reused': self.reused,
                    'closeFailures': self.close_failures}


_httpTransport = AgentHttpTransport()


def closeHttpTransport():
    return _httpTransport.close()


def _httpRequest(method, destination, data=None, diagnostic=True, timeout=20):
    """One request only. Log delivery never recursively reports to itself."""
    started = time.monotonic()
    endpoint = 'other_endpoint'
    try:
        endpoint = _safe_log_fields({'endpoint': urlsplit(destination).path}).get('endpoint', 'other_endpoint')
        headers = getHeadersForRequests()
        options = {'headers': headers, 'timeout': timeout}
        if method != "GET":
            options['data'] = json.dumps(data)
        response = _httpTransport.request(method, destination, **options)
    except Exception as error:
        response = emptyRequestsObject()
        response.transport_error = type(error).__name__
    if diagnostic:
        group = 'http:' + method + ':' + endpoint
        elapsed = int((time.monotonic()-started)*1000)
        failed = bool(getattr(response, 'transport_error', None)) or not 200 <= response.status_code < 300
        countDiagnostic('http_calls')
        countDiagnostic('http_failed' if failed else 'http_ok')
        countDiagnostic('http_elapsed_ms', elapsed)
        with _logger.condition:
            _logger.totals['http_max_ms'] = max(_logger.totals['http_max_ms'], elapsed)
        if failed:
            rows = data if isinstance(data, list) else [data]
            references = {'datapoints': len(rows)}
            references['metric_names'] = [row.get('name') for row in rows[:8] if isinstance(row, dict)]
            for row in rows[:8]:
                if isinstance(row, dict) and isinstance(row.get('tags'), dict):
                    references['function_uuid'] = row['tags'].get('functionUuid')
                    if references['function_uuid']:
                        break
            logProblem(group, 'http_failed', forward=True, method=method, endpoint=endpoint,
                       elapsed_ms=elapsed, **references, **_http_log_fields(response))
        else:
            logRecovered(group)
    return response

# Make a http request of type PUT
# Supply the destination API endpoint as string and the dataToSend as JSON object
def httpPut(destination, dataToSend):
    return _httpRequest("PUT", destination, dataToSend)



# Make a http request of type POST
# Supply the destination API endpoint as string and the dataToSend as JSON object
def httpPost(destination, dataToSend):
    return _httpRequest("POST", destination, dataToSend)


# Make a http request of type GET
# Supply the destination API endpoint as string
def httpGet(destination):
    return _httpRequest("GET", destination)


# Send the device capabilities from config file up to server at /api/v1/devices
def reportDeviceCapabilities():
    headers = getHeadersForRequests()
    dataToSend = { "capabilities": conf["capabilities"] }
    r = httpPut(conf['server'] + '/api/v1/devices/' + conf["UUID"], dataToSend)
    if (r.status_code == 200):
        return(r.status_code)
    else:
        return(r.status_code) 


# Update (or insert) a configuration item with a capability for this device
def registerDeviceCapability(itemKey, itemValue):
    logEvent('capability_registered', 'DEBUG', function=itemKey);
    listCapabilities = conf["capabilities"] if "capabilities" in conf else {}
    # If the capability was already known (and in the config.info)
    if((itemKey in listCapabilities) == False or listCapabilities[itemKey] != itemValue):
        logEvent('capability_registered', 'DEBUG', function=itemKey)
        listCapabilities[itemKey] = itemValue
        upsertConfigurationItem("capabilities", listCapabilities)
        reportDeviceCapabilities()
    return;


# Delete a configuration item of a capability for this device
def unregisterDeviceCapability(itemKey):
    logEvent('capability_removed', 'DEBUG', function=itemKey);
    listCapabilities = conf["capabilities"] 
    # If the capability was already known (and in the config.info)
    if((itemKey in listCapabilities) == True):
        listCapabilities.pop(itemKey)
        upsertConfigurationItem("capabilities", listCapabilities)
        reportDeviceCapabilities()
    return;


def getLanIpAddress():
    # Returns the current LAN IP address.
    arg = "ip route list"
    p = subprocess.Popen(arg,shell=True,text=True,stdout=subprocess.PIPE)
    data = p.communicate()
    split_data = data[0].split()
    ipaddr = split_data[split_data.index("src")+1]
    return ipaddr




###########################   MQTT


def checkIsAgentMqttBrokerInstalled():
    checkProcess = runCommandWithTimeout(' systemctl status mosquitto ', 10)[1]
    if(len(checkProcess) > 5):
        return True
    else:
        return False



###########################   RUN OS COMMANDS


# Runs command on operating system 
def runNativeCommand(arrayCommandLineParts = []):
    try:
        s = subprocess.run(arrayCommandLineParts, capture_output=True, text=True).stdout
        return s
    except Exception as e:
        logProblem('native_command', 'native_command_failed', error_class=type(e).__name__)
        return 'unknown'


def runNativeCommandLine(strCommandLine):
    return runNativeCommand(strCommandLine.split())


# Execute command line and return the exit code and stdout
# scriptResponse is a tuple of (exitStatusCode, stdout)
def runCommandWithTimeout(command, timeout):
    timeout = int(timeout)
    p = subprocess.Popen(command, shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while timeout > 0:
        exitStatusCode = p.poll()
        if exitStatusCode is not None:
            # Command finished running (exitStatusCode is 0 when ok)
            logEvent('native_command_finished', 'DEBUG', exit_code=exitStatusCode)
            return (exitStatusCode, p.communicate()[0])
        time.sleep(0.1)
        timeout -= 0.1
    else:
        # Script has timed out so kill it
        try:
            p.kill()
        except OSError as e:
            if e.errno != 3:
                raise
    return (-1, None)


# Returns operating system detail
def getOperatingSystem():
    retVal = runNativeCommandLine("grep PRETTY_NAME /etc/os-release")
    if(retVal != None and len(retVal) > 1 and '=' in retVal):
        return retVal.split('=')[1]
    else:
        return runNativeCommandLine("uname -a")



def getRam():
    #Returns a tuple (total ram, available ram) in megabytes.
    try:
        s = subprocess.run(["free", "-m"], capture_output=True, text=True).stdout
        lines = s.split("\n")
        return ( int(lines[1].split()[1]), int(lines[2].split()[3]) )
    except:
        return (0, 0)

def getProcessCount():
    #Returns the number of processes.
    try:
        s = subprocess.run(["ps","-e"], capture_output=True, text=True).stdout
        return len(s.split("\n"))
    except:
        return 0

def getUptime():
    #Returns a tuple (uptime, 1 min load average).
    try:
        s = subprocess.run(["uptime"], capture_output=True, text=True).stdout
        load_split = s.split("load average: ")
        load_one = float(load_split[1].split(",")[0])
        up = load_split[0]
        up_pos = up.rfind(",", 0, len(up)-4)
        up = up[:up_pos].split("up ")[1]
        return (up, load_one)
    except:
        return ("", 0)

def getUptimeProcess():
    # Returns uptime in seconds
    try:
        s = subprocess.run(["cat", "/proc/uptime"], capture_output=True, text=True).stdout
        return int(float(s.split(" ")[0]))
    except:
        return 0


###########################   Utilities


def updateDeviceLabelOnServer(labelKey, labelValue):
    headers = getHeadersForRequests()
    r = httpGet(conf['server'] + '/api/v1/devices/' + conf['UUID'])
    if (r.status_code == 200 and json.loads(r.content)['totalRecords'] == 1):
        dataToSend = json.loads(r.content)['records'][0]
        if (dataToSend['labels'] is None):
            dataToSend['labels'] = {}
        dataToSend['labels'][labelKey] = labelValue
        r = httpPut(conf['server'] + '/api/v1/devices/' + conf['UUID'], dataToSend)
        if (r.status_code == 200):
            return r
        else:
            return r
    else:
        return r
    return


def getMilliSecondsSinceEpoch():
    return int((datetime.utcnow() - datetime(1970,1,1)).total_seconds() * 1000)


# Is a string valid json
def isJson(myjson):
    try:
        json_object = json.loads(myjson)
    except ValueError as e:
        return False
    return True


# For a file in the current dir, get the absolute location
def getAbsoluteFileLocation(inputFilename):
    return os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), inputFilename)


# Remove a directory if it already exists and make it again, ready for writing to
def provideFreshDirectory(dirToClean):
    if(len(dirToClean) > 3):
        os.system("sudo rm -rf " + dirToClean)
        os.system("mkdir -p " + dirToClean)
        os.system("sudo chmod 777 " + dirToClean)
    return


# Ensure a directory exists for writing to
def ensureDirectoryExists(dir):
    if(len(dir) > 3):
        os.system("mkdir -p " + dir)
        os.system("sudo chmod 777 " + dir)
    return


# Edit a json file to upsert a parameter
def readJsonFile(filename):
    with stateLock:
        with open(filename) as data_file:
            return json.load(data_file)


def writeJsonAtomic(filename, value):
    """Publish a complete Agent state file; never a truncated intermediate JSON."""
    with stateLock:
        directory = os.path.dirname(os.path.abspath(filename))
        os.makedirs(directory, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=".agent-state-", dir=directory)
        try:
            with os.fdopen(fd, "w") as outfile:
                json.dump(value, outfile, indent=4)
                outfile.flush()
                os.fsync(outfile.fileno())
            os.replace(temporary, filename)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


def upsertJsonEntries(filename, updates, expectedFunctionUuid=None):
    with stateLock:
        content = readJsonFile(filename)
        if expectedFunctionUuid is not None and content.get("functionParameterValues", {}).get("functionUuid") != expectedFunctionUuid:
            raise ValueError("function_identity_mismatch")
        content.update(updates)
        writeJsonAtomic(filename, content)
        return content


def upsertJsonEntry(jsonFileToEdit, jsonKey, jsonValue):
    return upsertJsonEntries(jsonFileToEdit, {jsonKey: jsonValue})


# Reduce a string to only safe characters
def safeChars(inputStr):
    return(''.join(ch for ch in inputStr if ch.isalnum()))


def generateUuid():
    return str(uuid.uuid4())

