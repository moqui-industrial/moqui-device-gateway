# Validation

The checked-in tests cover decimal conversion, integer widths and JSON precision,
PLC event identity/type handling, ownership/cancellation of polling, actual TCP
Modbus request/response frames and MQTT 5 broker handshake/PUBACK/SUBACK frames,
and binary file delivery with path rejection. The TCP peers are local protocol
fixtures, not industrial devices or a certified interoperability suite.

An MQTT test checks that a write without items publishes its payload as one
message on the request topic and waits for the PUBACK (the trajectory export).

The native metadata test boots the actual mr-moqui core, links the component's
native registry entries, loads every service file of the component, checks that
each protocol service implements the moqui-device interface, starts the
ToolFactory twice and shuts it down twice. Its database driver deliberately
rejects all queries: this validates integration metadata/lifecycle, not a real
database persistence round trip.

The OPC UA backend is compiled with the real async-opcua library. Conversion and
configuration tests are separate from server interoperability. Likewise, the
SFTP feature compiles against russh/russh-sftp; compilation alone does not prove
that a target CNC/SFTP server will accept a file or private key.

Before switching a real installation, exercise all configured requests with its
actual database and protocol peers: positive/negative quality, authentication,
certificate rejection, network loss during writes, queue saturation, duplicate
log dates, failed persistence, cancellation and shutdown under load. A success
from the native read service includes synchronous persistence; continuous
acquisition can acknowledge startup before later persistence failures, which are
reported in logs. The persistence queue is bounded at 1024 batches.

The component does not establish worst-case latency bounds, hard real-time
behavior, safety certification or actuator-level acknowledgement from MQTT.

Local verification (2026-10-21), mr-moqui master 9fe4b939 and the CI revision
ed07b11 (`cargo test --locked`, `cargo clippy --locked -- -D warnings`), Rust
1.99.1, edition 2021: 35 Rust tests, among them the OPC UA backend against an
in-process async-opcua server (read, write, live subscription) and the
persistence failure notice rules of the former InboundErrorNotifier.

Component specs (`./moq test moqui-device moqui-device-gateway`): 38 of 38 active
features with a local Artemis broker, the OPC UA test server of
`examples/opcua_test_server.rs`, an SFTP server and the inference HTTP fixture of
moqui-device; 32 of 32 with `device_gateway_device_id=GW_EDGE_01`; the 3 native
ONNX features with the `onnx` build feature. They port the scenarios of the
former gateway tests: recipe text and trajectory recipe export, the redundant
second destination, MQTT write, subscription ingestion and a burst of ten logged
messages, the PLC log cases (numeric, text, no source, DT# date, mixed batch,
malformed batch), OPC UA write, one-shot read, polling and monitored item into
the Parameter records, the gateway scope and startup restoration with inactive
memberships, the refusal of an out-of-scope request, the persistence failure and
recovery notifications, an SFTP recipe export, and the HVAC demo end to end.
