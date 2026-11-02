# TLS_1_0 — narrow Agent reliability release

This release belongs to `davidskog/device-agent-python3`. `TLS_1_0` is our
Agent version string; it is unrelated to a TLS encryption protocol version.
The same string is used by startup, heartbeat, device label and the release
manifest. It is defined only in `davra-agent/davra_lib.py`, never in config.

## Changes

- Independent function UUIDs have independent state files. A new DV, TV or LED
  command cannot delete another command's state. State updates are atomic and
  protected within the Agent process. Existing legacy state remains readable.
- Completion requires the matching UUID and function name. Late, duplicated,
  unrelated and malformed acknowledgements cannot complete another operation.
  Timed-out functions are failed, not reported as executed successfully.
- Neither startup nor MQTT recovery republishes pending water commands.
  The last 256 completed UUIDs are retained as bounded duplicate protection.
  This is not an unlimited exactly-once delivery guarantee.
- Both MQTT message callbacks isolate malformed input and application errors.
  Paho owns normal reconnection, including an unavailable broker at startup.
  Supervision replaces an actually dead receiver only after closing its own
  thread and sockets. A thread that cannot close causes a process restart
  rather than a second simultaneous receiver.
- Local logs identify client role, version, failure/recovery and loop counts.
  Repeated identical MQTT/main-loop failures are limited to once a minute.
  Heartbeats include current MQTT connection/subscription/receiver health.
- HTTP failures record method, endpoint path, status/error class, elapsed time
  and bounded metric/function references. Authentication, headers, full data
  payloads and HTTP error bodies are not added to failure diagnostics.
  Log failures cannot recurse into another remote log request or kill a
  callback. A new remote log request is never made to report an HTTP failure.

Each HTTP call still makes exactly one request. No API outbox, resend queue,
automatic data/event resend or command replay is introduced. An event attempt
is recorded before sending so a local cleanup failure cannot resend it.
Existing configuration/registration capabilities are retained, not redesigned.

## Verification

Run `python3 -I -B -m unittest discover -s tests -v` from the repository root.
Tests use temporary state, synthetic credentials and mocked HTTP. They never
read an installed Agent config or command an installed device. When Paho is
installed, an additional native integration test uses an ephemeral loopback
MQTT peer to exercise first-connect failure, reconnect, malformed/callback
errors, dead-loop replacement and cleanup with the actual installed library.

Deployment targets keep their existing environments:

| Target | Python | Paho |
| --- | --- | --- |
| VK2 simulator | 3.13.5 | 2.1.0 |
| VK1 | 3.7.3 | 1.4.0 |
| VK4 | 3.7.3 | 1.4.0 |

Passing isolated tests is distinct from runtime upgrade and Davra readback.
The deployment evidence records those checks separately for each kiosk.

## Package and new installations

After committing, run `python3 -I -B tools/package_release.py --output /absolute/path`
in a clean worktree. The archive is generated from the committed Git blobs,
with full source commit, version and SHA-256 for every included file. Caches,
logs, `.DS_Store`, local state and config/credentials are excluded.

Use the `TLS_1_0` release assets from **our fork** for future installations;
do not download the upstream `main` artifact. The package contains the same
four Python files as this source commit, including the two repaired runtime
files. SDK, setup, dependency declarations and installer are unchanged.

The inherited installer is for separately planned new-host provisioning. It
installs dependencies and runs setup; its old dependency and platform
assumptions remain. Do not run it on VK2/VK1/VK4 as an upgrade. Dependency
modernization and broader installer work are deferred to another sprint.

## Existing-host upgrade / rollback

Upgrade VK2, then VK1, then VK4, only after the preceding target is verified.
Confirm identity, idle command state, current code and unchanged config hash.
Back up **code only**, stage and hash-check the two replacement Python files,
stop `davra_agent.service` and confirm its old process exited, replace both
files together while stopped, retain file ownership/modes, then start it.
Allow for the existing 50-second systemd startup delay. Do not alter service,
config, UUID, credentials, endpoints, dependencies, Node-RED or onboarding.

Verify installed hashes, actual new process, both MQTT paths, fresh telemetry,
fresh Davra heartbeat and `davraAgentVersion=TLS_1_0` device label, unchanged
config and no own upgrade processes/locks. VK2-only targeted simulator commands
verify UUID-bound completions; physical water commands are excluded on VK1/VK4.
If upgrade verification fails, stop the new Agent, restore both backed-up code
files while stopped and restart the old code with the same untouched config.
Retain pending command evidence; never clean or replay it blindly.

## Deferred work

API delivery guarantees/outbox, asynchronous HTTP isolation, broader SDK/job
hardening, dependency/platform upgrades and installer modernization require
their own planned scope. None is silently included in TLS_1_0.
