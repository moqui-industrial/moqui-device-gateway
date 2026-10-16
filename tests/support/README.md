# Optional protocol integration fixtures

The tests in `tests/transport_integration_test.rs` are selected explicitly with
`--ignored`. Use a disposable Moqui-loaded database in `DATABASE_URL`.

For OPC UA, install `asyncua` in a Python virtual environment, then start:

```bash
python tests/support/opcua_server.py
```

For SFTP, provide an OpenSSH/SFTP test server with a writable destination and
set `SFTP_TEST_URI` (for example `sftp:transfer@127.0.0.1:12222/home/transfer/uploads`),
`SFTP_KNOWN_HOSTS_FILE` and either `SFTP_PASSWORD` or `SFTP_PRIVATE_KEY_FILE`.
Verify the test server host key before adding it to the isolated known_hosts
file. The SFTP test overwrites `regression.bin` in that directory and verifies
rejection of untrusted hosts and path traversal.

The Modbus test starts its own TCP fixture; no external Modbus server is needed.

```bash
cargo test --locked --test transport_integration_test -- --ignored --nocapture
```

For encrypted OPC UA tests, install `asyncua` and `cryptography`, then start the
separate secure fixture (loopback port 12687). Its argument must be an isolated
scratch directory: certificates/private keys are generated there for this test.
They are short-lived test identities, not deployment credentials.

```bash
python tests/support/opcua_secure_server.py /tmp/moqui-opcua-test-pki
```

After it starts, run only OPC UA cases, sequentially because they share parameters:

```bash
OPCUA_TEST_PKI=/tmp/moqui-opcua-test-pki \
  cargo test --locked --test transport_integration_test opcua_ -- --ignored --nocapture --test-threads=1
```

Keep `DATABASE_URL` pointing at a disposable Moqui-loaded database and start the
plain fixture too. The secure test covers Basic256Sha256 Sign/SignAndEncrypt,
both AES policies with SignAndEncrypt, password/X509 identities, monitored
updates and cancellation. Wrong passwords, untrusted servers and untrusted
client certificates must fail. Both sides validate their peer's certificate.
These tests do not verify plant hardware or site PKI commissioning.
