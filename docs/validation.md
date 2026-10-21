# Validation

The checked-in tests cover decimal conversion, integer widths and JSON precision,
PLC event identity/type handling, ownership/cancellation of polling, actual TCP
Modbus request/response frames and MQTT 5 broker handshake/PUBACK/SUBACK frames,
and binary file delivery with path rejection. The TCP peers are local protocol
fixtures, not industrial devices or a certified interoperability suite.

The native metadata test boots the actual mr-moqui core, links the component's
native registry entry, checks the inherited service interface, starts the
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

Initial local verification: 26 tests passed with all default features; Clippy
passed for all targets with warnings denied. Rust 1.99.1 was used. The container
needed `RUSTFLAGS="-C codegen-units=1"` and one Cargo build job to avoid empty
object archives from its compiler/filesystem combination; no backend feature
was excluded from that full run.
