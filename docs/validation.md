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

Local verification (2026-10-21): 27 tests passed with all default features and
Clippy passed for all targets with warnings denied, against mr-moqui master
9fe4b939 and Rust 1.99.1. The framework APIs the component uses (service calls,
message facade error marks, JSON conversion) are unchanged at the CI revision
ed07b11.

Component specs (2026-10-21): `./moq test moqui-device moqui-device-gateway` with
`gateway_mqtt_test=1` passed 18 of 18 active features against a local Artemis
broker, among them the HVAC demo end to end: 20 live parameters published by
`HVAC_DEMO_LiveParametersWrite`, the PLC log subscription started, and a
ParameterLogger event stored as a ParameterLog row.
