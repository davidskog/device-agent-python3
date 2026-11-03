# TLS_1_4 — bounded HTTP connection reuse

Retains TLS_1_3 logging, MQTT lifecycle and command semantics. Uses one non-shared HTTP session per calling thread, at most eight cached sessions, zero automatic request retries and no data queue. Dead-thread sessions are closed; overflow uses a one-call session. Response cookies are not carried to the next request. A failed request retires its connection without resending.

Shutdown rejects new requests and closes idle sessions; active requests close their own session when they finish. No session is closed underneath another live caller.

Only VK2 is upgraded in this pass. Configuration, Python, Paho, systemd, MQTT keepalive and Node-RED controller timings remain unchanged. Install only the two Python runtime files from the checksummed archive, never install.sh. Preserve TLS_1_3 as rollback.

HTTP connection reuse reduces handshake overhead; it is not proof that the complete Desktop142 matrix passes. That requires a fresh complete original after installation.
