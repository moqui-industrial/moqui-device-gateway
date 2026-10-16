# Moqui Device Gateway — Rust

The `mr-moqui` branch implements the standalone industrial edge gateway in
Rust (Tokio/Axum). It executes the `moqui-device` model stored in the Moqui-owned
PostgreSQL database. It is independent of the framework process and PLC compiler.
Use [MR-Moqui master](https://github.com/moqui/mr-moqui) with the `mr-moqui`
branches of [moqui-device](https://github.com/moqui/moqui-device/tree/mr-moqui)
and [moqui-math](https://github.com/moqui/moqui-math/tree/mr-moqui).

The Java implementation uses Quarkus/Apache Camel on
[`main`](https://github.com/moqui/moqui-device-gateway/tree/main) (formerly
`master`). Follow the README of the selected branch: Gradle, GraalVM, Camel
routes and SEDA endpoints belong to the Java gateway. This Rust gateway uses
supervised Tokio tasks and async protocol clients; it accepts `paho-mqtt5:` URI
syntax for compatibility, without running Camel.

## Model and responsibility

The persistent declaration is the Moqui model, not local route files:

```text
Device + PhysicalDevice (gateway identity)
  -> DeviceGroupMember / DgmpEdgeGateway
  -> recursive group members (PLCs, controllers, remote IO)
  -> active DeviceRequest / DeviceRequestItem / DeviceConnection
  -> Rust protocol tasks -> MQTT v5 / OPC UA / field device
```

Each process has one `GATEWAY_DEVICE_ID`. It must exist as both `Device` and
`PhysicalDevice`, with an active `DgmpEdgeGateway` membership. Use one active
replica per logical gateway ID. Different gateways need different identities;
there is no distributed leader election for replicas sharing an ID.

REST execution, content transfer, recipe export and trajectory export enforce
the gateway's recursive device scope. The request's target is the field device;
its `brokerUri` identifies the transport. Startup restores effective-dated
subscribe/event/state-change/cyclic requests routed through
`DrrMoquiDeviceGateway`, in modeled priority/sequence order. Unsubscribe stops
runtime tasks; update the persistent request dates/routing/membership to prevent
restoration after restart.

Use the schema and seed managed by MR-Moqui, with its default Java-compatible
SQL naming (`DeviceRequest` -> `device_request`). Legacy quoted MR-Moqui names
are not supported by the gateway SQL. This is a shared domain model, not a
separate gateway-owned production schema.

## Build and start

Requires Rust/Cargo 1.99 or newer and PostgreSQL. Build from this branch:

```bash
cargo build --locked
export DATABASE_URL=postgres://moqui:moqui@127.0.0.1:5432/moqui
export LOG_DATABASE_URL="$DATABASE_URL"       # local test only; production may split logs
export GATEWAY_DEVICE_ID=GW_EDGE_01
export GATEWAY_API_AUTH_TOKEN='replace-with-a-real-secret'
export MQTT_BROKER_URL=tcp://127.0.0.1:1883
export MQTT_USERNAME=artemis                   # local Artemis example
export MQTT_PASSWORD=artemis
cargo run --locked
```

The process reads `.env` (or `runtime/.env`), then configuration from environment
variables and an optional `.properties` file/directory selected by
`SMALLRYE_CONFIG_LOCATIONS`. Dotted gateway property names are supported for
migration; Camel route expressions and SmallRye interpolation are not evaluated.
Native Rust environment variables are recommended.

| Variable | Default / purpose |
|---|---|
| `DATABASE_URL` | Transactional PostgreSQL URL; default `postgres://moqui:moqui@localhost:5432/moqui` |
| `LOG_DATABASE_URL` | Telemetry PostgreSQL URL; defaults to `DATABASE_URL` |
| `GATEWAY_DEVICE_ID` | Required logical identity |
| `HTTP_HOST`, `HTTP_PORT` | `0.0.0.0`, `8081` |
| `APP_PROFILE` | `prod`; `QUARKUS_PROFILE` is a legacy alias |
| `GATEWAY_API_AUTH_ENABLED` | `true` |
| `GATEWAY_API_AUTH_HEADER` | `X-API-Key` |
| `GATEWAY_API_AUTH_TOKEN` | Must be a non-default token in production |
| `GATEWAY_STARTUP_DISCOVERY_ENABLED` | `true` |
| `MQTT_BROKER_URL` | Default broker for bulk publishing and compatible URI fallback |
| `MQTT_USERNAME`, `MQTT_PASSWORD` | Deployment-side broker credentials; explicit URI credentials take precedence |
| `MQTT_READ_ROUTE_AUTO_STARTUP`, `MQTT_READ_CONSUME_URI` | Optional static parameter consumer |
| `PLC_LOG_ROUTE_AUTO_STARTUP`, `PLC_LOG_CONSUME_URI` | Optional static PLC log consumer; disabled by default |
| `MQTT_WRITE_AFTER_PUBLISH_ENABLED`, `MQTT_WRITE_AFTER_PUBLISH_URI` | Optional HTTP success callback |
| `OPCUA_WRITE_AFTER_PUBLISH_ENABLED`, `OPCUA_WRITE_AFTER_PUBLISH_URI` | Optional HTTP success callback |
| `MQTT_READ_AFTER_STORE_ENABLED`, `MQTT_READ_AFTER_STORE_URI` | Optional HTTP storage callback |
| `FILE_TRANSFER_URI` | Recipe output destination; default `target/test-recipes` |
| `FILE_TRANSFER_URI_2_ENABLED`, `FILE_TRANSFER_URI_2` | Optional second recipe destination |
| `SFTP_KNOWN_HOSTS_FILE` | OpenSSH known_hosts file; required for SFTP host verification |
| `SFTP_PASSWORD` / `SFTP_PRIVATE_KEY_FILE` | SFTP credentials; optional key passphrase in `SFTP_PRIVATE_KEY_PASSPHRASE` |
| `GATEWAY_INBOUND_ERROR_NOTIFICATION_ENABLED` | `true` |
| `GATEWAY_INBOUND_ERROR_NOTIFICATION_THRESHOLD_SECONDS` | `60` |
| `GATEWAY_INBOUND_ERROR_NOTIFICATION_URI` | Optional HTTP notification endpoint |

`QUARKUS_DATASOURCE_JDBC_URL` and `QUARKUS_DATASOURCE_LOG_JDBC_URL` remain aliases
for database URLs and accept the `jdbc:postgresql://` prefix. Put credentials in
the URL when using those aliases; separate Java username/password settings are
not automatically combined. The legacy `MQTT_WRITE_AFTERPUBLISH_ENABLED` is
accepted; prefer the native spelling with `_AFTER_PUBLISH_`.

Profiles `dev`, `test`, `local`, `integration` bypass API authentication for local
testing. Production rejects the shipped placeholder tokens. API keys use the
configured header or `Authorization: Bearer <token>`; query-string API keys are
not accepted. Keep credentials in deployment configuration rather than seed data.

## Protocols and data contracts

### MQTT v5

Examples of compatible `DeviceRequest.brokerUri` values:

```text
paho-mqtt5:?brokerUrl=tcp://broker:1883&qos=1&cleanStart=false&sessionExpiryInterval=86400
mqtt://broker:1883
mqtts://broker:8883
```

Topics come from `DeviceRequestItem.query`, then `DeviceRequest.query`, then the
`paho-mqtt5:` topic. Options include `clientId`, `userName`, `password`, `qos`
(0/1/2), `cleanStart`, `sessionExpiryInterval`, `retain`/`retained`. TLS uses the
client's default trusted roots. Unsupported Camel-specific endpoint behavior is
not an alternate runtime inside this process.

Writes publish the parameter identity/value envelope plus the reviewed
`requestItemName` key, when present. Reserved envelope keys cannot be overridden.
QoS 1/2 writes complete after the broker's PUBACK/PUBCOMP; a connection error,
rejection or timeout returns failure. QoS 0 only confirms that the client sent
the message. `publishUriList` omits credentials. The transport boundary does not
prove that PLC application code applied the value.

Subscriptions use stable gateway/request/topic-derived client IDs and v5 session
expiry. A new broker session re-establishes its topic filter. Canceling a task
sends UNSUBSCRIBE before disconnecting. Broker persistence, QoS and a stable ID
are needed for offline delivery; database-driven restoration alone is not broker
persistence.

Inbound scalar or identity/value JSON is normalized to the model. Non-logging
purposes update existing `Parameter` rows; logging requests append `ParameterLog`.
The gateway never creates missing Parameters. Concurrent log writes allocate
sequence numbers under PostgreSQL transaction locks, respecting the model's
unique `(parameterId, sequenceNum)` index.

PLC `LoggerFacade` batches use exact persistent identities: `loggerName` is
`Device.deviceId`; non-empty `source` is `Parameter.parameterId` and selects
`ParameterLog`, while an empty source selects `DeviceLog`. Numeric/text/enum
payload type does not choose the table. Use `PLC_LOG_CONSUME_URI` or a modeled
`DrpLogging` subscription on the `moqui-plc` topic.

### OPC UA and Modbus

OPC UA read/write/subscription requests use `DcdOpcUa`, a TCP `DeviceConnection`
and standard node IDs such as `ns=2;s=virtual_plc_feedback` or `ns=1;i=1005`.
`RAW(...)` node syntax is accepted. The current OPC UA configuration uses
anonymous sessions with security mode None; configure network exposure
accordingly. Connection waits are bounded and subscription cancellation closes
the session.

The Rust extension also includes Modbus TCP read/write/cyclic requests (`DcdModbusTCP`; legacy `DcdModbus` is also accepted)
with holding/input/coil/discrete targets. Modbus TLS requires a trusted PEM
bundle in `MODBUS_TLS_CA_FILE` and verifies the endpoint hostname; no hard-coded
server name or empty trust store is used. This is separate from Java gateway
parity. Do not infer coverage of vendor fieldbuses or production TLS policies
from the generic protocol clients.

### Files, recipes and trajectories

`DeviceConfig` is atomic; `DeviceRuleSet` and ordered `DeviceRule` rows compose
multi-device recipes. Export produces `{ruleSetName}_pNN.txt`, with assignments
such as `dev.tempSetpoint:=20`. PLC/atomic-device naming adds the IEC `dev.` prefix
at projection time. Trajectory-backed configurations include `Trajectory.*`
assignments. File/export failures return errors; a second-destination failure
may follow a successful first transfer, so inspect the result before retrying.

Local destinations use `file:/path` or a directory path. Filenames must be single
names without path components. Local writes replace the destination through a
temporary file, so they do not follow an existing destination-file symlink.

SFTP supports `sftp:user@host:22/path` and `sftp://user@host:22/path`, with
`privateKeyFile`, `knownHostsFile` or `password` options (deployment environment
variables are preferred). The host key must match known_hosts. Completion means
the remote file was written and closed; there is no local success fallback.
SFTP overwrites existing destination files and does not promise atomic remote
replacement. The remote directory must already exist and be writable.

## REST API

| Endpoint | JSON body / behavior |
|---|---|
| `POST /api/device-request/run/{requestName}` | Execute the modeled request, including `DrtUnsubscribe` with `parentRequestName` |
| `POST /api/device-content/transfer/{requestName}` | `filename`, `contentBase64` (standard padded base64); destination is the request's `brokerUri` |
| `POST /api/device-config/export` | `deviceRuleSetId`, optional `deviceId` / `deviceRuleId`; checks every selected target before transferring |
| `POST /api/trajectory/export` | `requestName`, `approximatedFunctionId`, `waypointCount`, `mqttTopic`, `axes`; topic must match the scoped request's query |
| `GET /q/health`, `/q/health/ready` | Database and gateway identity/membership readiness |
| `GET /q/health/live` | Process liveness |

Trajectory axes are equal-length arrays of finite numbers. The bulk route
publishes through the deployment-configured MQTT broker after scope validation.
The Moqui-side wrapper includes `requestName`; older payloads without it must be
updated. Optional HTTP callbacks are best effort and do not turn a completed
transport write into a failed write.

```bash
curl -X POST http://localhost:8081/api/device-request/run/TestPlcMqttPublishReq01 \
  -H "X-API-Key: $GATEWAY_API_AUTH_TOKEN"
curl http://localhost:8081/q/health/ready
```

Readiness does not require every field PLC or broker to be reachable. Field
failures and reconnect attempts appear in protocol logs/error notifications.

## Local containers and tests

The SQL under `tests/resources` is a minimal test fixture, not a production
schema. Its device identities are `GW_EDGE_01`, `VIRTUAL_PLC_01`, `DRV_EDGE_01`,
`DG_EDGE_01`; request/parameter identities follow `moqui-device/DeviceTestData.xml`.
The suffix template is expanded by `docker/initdb/30-load-gateway-fixtures.sh`.
The Moqui XML fixture is now `type="test"`, excluded from ordinary seed loading.
The base XML tests need no Mantle. WorkEffort/Measurement telemetry is in the
explicitly loaded `moqui-device/test/mantle/MantleTestData.xml` and requires
optional `mantle-udm` only for those scenarios. Follow moqui-device's test README.

```bash
docker compose -f docker/postgres-compose.yml -p moqui-gateway up -d
# Start Artemis from the deployment repository, or reuse an existing test broker:
docker compose -f ../moqui-deploy/industrial/activemq-compose.yml -p moqui-broker up -d
export GATEWAY_API_AUTH_TOKEN='local-development-token'
docker compose -f docker/device-gateway-compose.yml -p moqui-gateway up -d --build
```

The gateway Dockerfile is `Dockerfile` at this branch's root. There is no GraalVM
Dockerfile on this branch. Container requests must use broker addresses reachable
from that container; `localhost` inside a container is not the Docker host.
The Compose seed uses `host.docker.internal` for a host-published broker.
PostgreSQL initializes fixtures only when its data volume is new.

```bash
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
# Explicit live tests: use a disposable database, never an existing plant DB.
DATABASE_URL=postgres://moqui:moqui@127.0.0.1:5432/moqui \
GATEWAY_DEVICE_ID=GW_EDGE_01 \
cargo test --locked --test integration_containers_test -- --ignored --nocapture
```

Container tests are explicitly ignored by default and fail when selected without
the required services. They cover discovery/scope, real broker publish and ingest,
PLC log persistence, authenticated REST, binary transfer and recipe errors.
Unit tests also check MQTT v5 on the wire, timeout without acknowledgement and
filename confinement. These checks do not replace CODESYS or field commissioning.

See [the integration validation record](docs/validation-2026-10-16.md) for the
executed checks and their scope. Additional OPC UA/SFTP/Modbus fixture instructions
are in [tests/support/README.md](tests/support/README.md).

## Java/Rust compatibility and remaining differences

| Contract | Rust branch compared with Java/Quarkus/Camel |
| --- | --- |
| Database and device scope | Same modeled devices, requests, parameters, configurations and content; Rust uses Moqui's Java-compatible SQL names |
| Core REST operations | Same run-request, transfer-content and export-config paths and JSON intent; Rust also enforces request scope |
| MQTT | MQTT **v5** through `rumqttc::v5`; Java uses Camel/Paho MQTT5. QoS acknowledgements, session options, credentials and TLS are implemented |
| File/SFTP | Native local file and SFTP transports, with known-host verification for SFTP |
| Modbus | Native TCP/TLS read, write and cyclic execution; not arbitrary Camel endpoint options |
| OPC UA | Anonymous/security-none read, write and subscription tested; advanced Java/Camel security and connection options are not fully ported |
| Trajectory MQTT export | Rust adds `/api/trajectory/export`; the reviewed Java branch has no matching endpoint yet |
| ONNX trajectory inference | Native optional moqui-device service using ort/ONNX Runtime, or an explicitly remote provider; see the [isolated Rust example](https://github.com/moqui/moqui-device/tree/mr-moqui/examples/onnx-obstacle) |

These are shared application contracts with documented remaining differences,
not a claim that every Camel route, endpoint option or deployment is interchangeable.
