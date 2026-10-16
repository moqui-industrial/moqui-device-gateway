# Rust industrial integration validation — 2026-10-16

Framework: MR-Moqui master `ab88f80`. Industrial components: local working
changes on `mr-moqui`; no commit or push is implied by this report.

The framework loaded 10,210 records (framework, mantle-udm, moqui-math,
moqui-device) into the separate disposable database `moqui_rust_model` on the
existing test PostgreSQL container. Application data in the existing `moqui`
database was not used for these tests. Artemis was reused; it was not restarted.

## Passing checks

| Check | Result |
|---|---|
| moqui-plc script suite | 41 passed |
| moqui-plc skill suite, including Rust/Quarkus guide separation | 28 passed |
| Gateway unit/regression suite | 8 passed |
| PostgreSQL + Artemis integration suite | 7 passed |
| OPC UA / SFTP / Modbus protocol suite | 3 passed |
| MR-Moqui device service specs, including HTTP fixtures | 8 passed |
| Rust Clippy, all targets, warnings denied | Passed |
| Service XSD validation and component.xml validation | Passed |
| DeviceTestData, HVACDemoData, TrajectoryPlannerData static model checks | No errors or warnings |
| Gateway and industrial Rust Compose configuration validation | Passed |
| Updated minimal SQL fixture schema and both seed files on an empty DB | Passed |
| Gateway process startup, stop and restart against the real model | Both starts restored 2 subscriptions and reached UP |

Total executed tests in the six suites: 95. Container/protocol tests are ignored
by the normal unit command and were selected explicitly with `--ignored`.
They fail rather than silently passing when required infrastructure is absent.

The live tests cover MQTT v5 publication, scalar/log ingestion, concurrent log
sequences, authenticated REST and scope rejection, binary content transfer,
recipe export and failure reporting, scoped trajectory publication, OPC UA
read/write/monitored updates/cancellation, verified-host SFTP upload/overwrite,
and Modbus TCP read/write/polling. SFTP bytes were also read back independently
from the temporary OpenSSH container.

Framework specs cover model loading, dry-run configuration, REST headers/body,
HTTP failure propagation, binary OpenVLA image encoding, grounding vector
persistence, explicit demo interpolation and remote-provider waypoint persistence.
The HTTP fixture is a local test provider, not a neural inference benchmark.

## Boundaries

These are integration checks on local test services, not CODESYS commissioning,
field I/O, FAT/SAT, production OPC UA security certification, Modbus TLS device
validation, broker HA failover or a load test. Compose files were parsed and
validated; the new industrial Rust image was not deployed. RuSTy/IronPLC are
experimental openings for simple programs, not supported complete moqui-plc
platforms. No PLC compiler or physical controller was required for these checks.

The Java comparison baseline is the gateway's current `main` branch (previously
called `master`). Rust shares the model and supported REST/protocol contracts;
Java-specific Camel options, SEDA routes and JVM integrations are not Rust
execution engines. See the README's explicit supported settings.
