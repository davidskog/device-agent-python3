# TLS_1_1 — server MQTT idle-connection recovery

TLS_1_1 retains the TLS_1_0 per-operation state and duplicate suppression.
It changes the server MQTT keepalive from Paho's default 60 seconds to
30 seconds. Two independent read-only clients on VK2 demonstrated that
the server path closed the 60-second idle connection before its first PING,
while the 30-second client received three PING responses and stayed connected
through the 95-second observation. This identifies an idle boundary on the
connection path; it does not identify which remote hop imposes it.

The receiver's connection callback now writes its connection/reconnection
diagnostic locally rather than waiting for an HTTP log forward. Health only
accepts the SUBACK matching that connection's subscription request. Disconnect
diagnostics include the numeric Paho reason code. Existing heartbeat reporting
and ordinary automatic Paho reconnect remain in place.

No broker, endpoint, identity, credential, certificate, Python or package change
is required. Configuration is not changed, and QoS is not increased or claimed
to create an offline command-delivery guarantee. No telemetry resend, command
replay or timestamp refresh is added. The original operation IDs, duplicate
bookkeeping and Node-RED command validity rules are retained.

## Existing-host upgrade

Do **not** run `install.sh` or setup as an upgrade. After confirming no pending
operation and an idle target, back up the existing `davra_agent.py` and
`davra_lib.py`, record their hashes and the configuration hash, stop the Agent,
and replace only those two Python files from the verified TLS_1_1 release.
Preserve owner/mode and config bytes; start the existing service and verify its
new process, code hashes, version heartbeat, both MQTT paths and actual command
results. Keep the previous two files for a controlled rollback.

The authorized first target is VK2 only. Publication of this release does not
authorize upgrading VK1/VK4 or other existing installations.
