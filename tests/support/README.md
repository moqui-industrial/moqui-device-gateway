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

These fixtures do not verify plant hardware or production OPC UA security modes.
