# TLS_1_2 — bounded diagnostic logging

This release changes diagnostic logging. It does not change telemetry payloads,
heartbeat intervals, command IDs, validity, MQTT keepalive/reconnect, API retries,
configuration reporting, dependencies, systemd or Node-RED.

## Normal operation

DEBUG is disabled. Each 300-second interval emits one local `driftstatus` with
monotonic process uptime, most recently observed MQTT health, HTTP/batch counts,
queue depths, live logging workers and drop/failure counters. Its HTTP maximum
is since process start. This diagnostic makes no telemetry request. INFO/WARN/
ERROR map to actual journal priorities 6/4/3 with the stream priority prefix.

`agent_started`, `agent_stopped`, first MQTT-ready/subscription and meaningful
command transitions remain immediate queued diagnostics. They are not generic
per-message echoes. A device result and its HTTP reporting result are separate.
The existing data/command APIs still make one attempt per invocation.

Each first fault is queued immediately. Identical repetitions are counted and
reported no more often than once per 60 seconds. A real successful recovery
closes the matching fault group; reconnect readiness requires the actual
matching successful SUBACK. Different failure types remain separate. At most
128 active fault signatures are retained; overflow is explicitly counted and
reported instead of allocating unbounded memory.

## Sinks and failure behavior

Only the two owned daemon workers perform local/remote log I/O. The local queue
holds at most 512 records; the remote queue holds at most 64. Important incoming
records can replace lower-priority queued records; all drops are counted. A
living blocked worker is never replaced. Failed worker creation has bounded
2-to-60-second backoff. The regular logging poll can restart a dead worker;
late emitters cannot resurrect a deliberately closed logger.

Remote forwarding is opt-in at each structured call site. Normal summaries,
telemetry chatter and command trace rows are local. Selected operational errors
and recovery/start events may use Davra's existing log endpoint once. A remote
log failure is local-only, never recursively forwarded or retried. Logging HTTP
uses a three-second request timeout. Data HTTP retains its previous timeout.
The application does not wait for log delivery.

Shutdown drains only within a bounded deadline and reports unclosed workers or
pending records truthfully. A clean thread shutdown does not claim that every
log was delivered: drop/write-failure counts remain separate evidence.

The local mirror uses the existing log directory but a version-owned filename,
`davra_agent_TLS_1_2.log`, maximum 10 MB plus two rotated backups. Old-version
logs and journal history are untouched. New mirror files are private and
symlink/hardlink destinations are refused.

## Safe diagnostic contract

Only known event names and allowlisted bounded scalar fields are serialized.
The first eight valid metric names may identify a failed batch; values, headers,
queries, configuration objects, scripts and arbitrary exception bodies are
never logged. Function/job UUIDs retain exact identity. Free-form legacy log
entry points emit only an omitted-message marker, including when DEBUG is on.

Optional `--debug-seconds N` enables filtered structured DEBUG for 1–600 seconds
in that process only. The default is zero. It writes no configuration and adds
no token/body logging. Do not modify the service configuration to enable it as
part of an ordinary upgrade.

## Validation and upgrade

Run the full Agent tests with the installed target Python/Paho. Tests inject
slow/failing sinks, overload, thread-start failure, shutdown races, callbacks,
reconnection, wrong/duplicate command identities and log secret canaries.
Live journal-priority checks use an explicitly labelled isolated diagnostic.

The VK2 upgrade replaces only `davra_agent.py` and `davra_lib.py` from the
immutable release package after the preceding original and commands close.
Do not use `install.sh` as an upgrade. Preserve config byte-for-byte, Python/
Paho, service configuration and the original rollback copy. Revalidate version,
source hashes, real MQTT readiness, fresh heartbeat, logs and actual commands.

Only the exact final complete Desktop142 original can establish matrix PASS.
Isolated logger tests or a clean partial run cannot substitute for that result.
