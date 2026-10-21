# moqui-device-gateway

Native field communication for [mr-moqui](https://github.com/moqui/mr-moqui).
On this branch the gateway is a Moqui component that runs inside mr-moqui, not a
separate process. It merges moqui-plc4j and the protocol execution part of the
Quarkus/Camel moqui-device-gateway.

| Branch | Content |
|---|---|
| `main` | The Quarkus/Camel standalone gateway |
| `old-mr-moqui` | The first standalone Rust rewrite, kept for reference until this work is complete |
| `mr-moqui` | This Moqui component |

What the Quarkus gateway did over REST is now a local service call, and its SQL
is Entity Engine queries. Recipe rendering and trajectory messages are general
device services of moqui-device: this component only moves the data.

## Services

The application entry point is `moqui.device.DeviceServices.run#DeviceRequest`
(moqui-device). It calls the service in `DeviceRequest.runServiceName`, which is
one of the protocol services of this component, each an implementation of
`moqui.device.DeviceServices.run#DeviceRequestInternal`:

| `runServiceName` | Endpoints | Request types |
|---|---|---|
| `moqui.device.gateway.MqttServices.run#DeviceRequest` | `mqtt://`, `mqtts://`, legacy `paho-mqtt5:` | read, write (items, or one `payload` message), subscribe, cyclic, unsubscribe |
| `moqui.device.gateway.OpcUaServices.run#DeviceRequest` | `opc.tcp://`, `DcdOpcUa` | read, write, subscribe (monitored items), cyclic, unsubscribe |
| `moqui.device.gateway.ModbusServices.run#DeviceRequest` | `modbus-tcp://`, `DcdModbusTCP` | read, write, cyclic, unsubscribe |
| `moqui.device.gateway.FileServices.run#DeviceRequest` | `file:`, `sftp://` | content transfer, recipe export (`DrtConfigWrite`) |
| `moqui.device.gateway.PlcLogServices.run#DeviceRequest` | MQTT | subscription to the moqui-plc LoggerFacade events, unsubscribe |

A service runs only its own protocol: a request whose endpoint is another
protocol is an error, never a silent switch.

The other services of the component:

| Service | Use |
|---|---|
| `moqui.device.gateway.InboundServices.store#Samples` | Stores the values a device sent: the result of a read and the samples of a subscription or polling. A SECA on it is the after-store callback of the former gateway |
| `moqui.device.gateway.InboundServices.store#PlcLog` | Stores PLC log events: ParameterLog for an event with a source, DeviceLog for the others |
| `moqui.device.gateway.InboundServices.notify#InboundState` | Tells the user (topic `DeviceGatewayInbound`) that a request's values fail to store, and when they store again |
| `moqui.device.gateway.SubscriptionServices.get#GatewayScope` | The identity and scope of a gateway: its DeviceGroups and their alive members |
| `moqui.device.gateway.SubscriptionServices.get#StartupSubscriptions` | The subscriptions a gateway starts: those of the controllers in its scope, in order |
| `moqui.device.gateway.SubscriptionServices.start#Subscriptions` | Restores the subscriptions after a start and restarts those that lost their connection |
| `moqui.device.gateway.SubscriptionServices.get#SubscriptionState` | The subscriptions of this process: running, and ended without an unsubscribe |

In Rust, `DeviceGatewayToolFactory` (tool `DeviceGateway`) is the registry of
connections and subscriptions: it owns the I/O runtime, the protocol clients
with their sessions, the active subscriptions and the persistence queue. Each
protocol is a `DeviceClient`: `MqttDeviceClient`, `OpcUaDeviceClient`,
`ModbusDeviceClient`, and `FileDeviceClient` for file and SFTP.

Device, DeviceConnection, DeviceRequest, DeviceRequestItem, Parameter and
ParameterDef remain the existing Moqui entities. No additional database, REST
process or route interpreter is introduced.

## Installation

Install this repository at `runtime/component/moqui-device-gateway` in mr-moqui,
with `moqui-device` and `moqui-math` on their `mr-moqui` branches. The component
uses the framework's sibling `moqui-core` and `moqui-lib` crates. It is not a
standalone checkout with respect to Cargo path dependency resolution.

Build using the framework's `./moq build`, which links native components, and
load the seed data (the `start_DeviceGatewaySubscriptions` service job and the
`DeviceGatewayInbound` notification topic).

### Properties

| Property | Default | Use |
|---|---|---|
| `device_gateway_device_id` | empty | The gateway Device this instance is: a `PhysicalDevice` and a `DgmpEdgeGateway` member of its DeviceGroups. Set: the subscriptions of the controllers in its scope are restored after a start, and a request for a device outside its scope is refused. Empty: nothing is restored and no scope is checked |
| `device_gateway_restore_subscriptions` | `true` | `false` on an instance that must not start the subscriptions |
| `device_gateway_error_notification` | `true` | `false` turns off the persistence failure notifications |
| `device_gateway_error_notification_seconds` | `60` | How long the values of a request fail to store before the user is told | Rust
1.99 or later is required by this initial lockfile/toolchain baseline.

```sh
cd runtime/component/moqui-device-gateway
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
```

The component specs (`test/DeviceGateway.spec.js`) run inside MR-Moqui with the
data of moqui-device: `./moq test moqui-device-gateway` from the framework root.
The MQTT features also need an MQTT 5 broker on 127.0.0.1:1883 with user
`artemis`/`artemis` (the RMQTT or Artemis compose of moqui-deploy):

```sh
MQTT_USERNAME=artemis MQTT_PASSWORD=artemis gateway_mqtt_test=1 ./moq test moqui-device-gateway
```

More optional features: `device_gateway_error_notification_seconds=0` (the
persistence failure notification), `device_gateway_device_id=GW_EDGE_01` (scope
refusal and startup restoration), `gateway_opcua_test=1` with the OPC UA test
server of `cargo run --example opcua_test_server` (127.0.0.1:12686/milo, the
nodes of DeviceTestData.xml), and `gateway_sftp_test=1` with
`gateway_sftp_fingerprint=SHA256:...` for an SFTP server on 127.0.0.1:2223 (user
`foo`/`pass`, directory `/upload`, for example the `atmoz/sftp` image). The Rust
tests start their own OPC UA server (`tests/opcua_server.rs`).

For a smaller transport-only build, use
`cargo test --locked --no-default-features --features modbus,mqtt`.
Cargo still resolves optional local framework manifests, so keep the component
in its installation directory. See [validation](docs/validation.md) for what
these checks prove and which deployment checks remain necessary.

## Included capabilities

| Backend | Read/write | Continuous acquisition | Files |
| --- | --- | --- | --- |
| Modbus TCP | coils and typed integer/float registers | explicit `DrtCyclic` polling | no |
| MQTT 5 | JSON values; gateway-compatible item publication envelope; one JSON message for a write without items; QoS 1 PUBACK | broker subscriptions with SUBACK | no |
| OPC UA | typed values, per-node status and source timestamps | monitored-item subscriptions or explicit polling | no |
| Local directory | no | no | binary transfer, temporary file followed by rename |
| SFTP | no | no | binary transfer, pinned SSH host key, password or private key |

Browse/discovery, structured OPC UA values and Modbus TLS are not implemented.
An unsupported request returns an error; there is no fallback protocol. The
complete migration checklist is in [compatibility](docs/compatibility.md).

### PROFINET, EtherNet/IP and EtherCAT

These fieldbuses are deliberately not part of the component. On them the PLC owns
the drives and the cyclic I/O, and the parameters of an inverter or servo drive
are reached through acyclic services (PROFINET record data and PROFIdrive
parameter access, CIP explicit messages); EtherCAT has a single master, the PLC.
The Rust libraries reviewed in October 2026 do not fit this use: the PROFINET
controller stacks are GPL-3.0 (they cannot be linked into the CC0 binary) or
device-side only, and the maintained EtherNet/IP crate targets Logix tags, with
generic CIP access only as raw requests.

Read and write drive parameters through the PLC instead: the CODESYS or TIA
Portal parameter-access blocks of the drive, exposed to Moqui through OPC UA or
MQTT, which this component supports. Commissioning stays with the engineering
tools (TIA Portal/Startdrive, CODESYS, the drive vendor's software).

## One API, two callers

`DeviceGatewayFacade::execute` takes a prepared, owned request, input values, an
absolute monotonic deadline, output storage and an optional sample sink. The
`DeviceClient` trait is the backend contract. Neither requires an
ExecutionContext. See [direct Rust access](examples/direct_modbus.rs).

The Moqui protocol services resolve cached configuration using the Entity
Engine, prepare this same request, and store finite reads through
`store#Samples` before returning. A bounded worker queue sends subscription and
polling samples to `store#Samples` or `store#PlcLog`. Factory shutdown cancels
subscriptions, closes sessions and drains queued persistence work.

This implementation allocates, uses asynchronous networking, locks and queues.
It is not a hard real-time PLC runtime. A future control loop would need its own
verified scheduling, memory and I/O constraints; it must not treat this entire
service path as deterministic merely because it is written in Rust.

## Connection configuration

* Modbus: `DeviceConnection.driverEnumId=DcdModbusTCP`,
  `transportConfig=host:502`, options `unit-identifier=1` and optionally
  `byte-order=big-endian&word-order=big-endian`. Addresses use PLC4J-style
  zero-based offsets, e.g. `holding-register:2:UINT` or
  `holding-register:10:REAL`. No 40001 adjustment is inferred.
* MQTT: `brokerUri=mqtt://host:1883` or `mqtts://host:8883`; the topic is the
  item query or request query. Supported legacy `paho-mqtt5:?brokerUrl=...`
  URIs are normalized without running Camel. Only QoS 1 is supported.
* OPC UA: `brokerUri=opc.tcp://host:4840/path` or `DcdOpcUa` transportConfig;
  addresses are explicit `ns=2;s=Temperature` NodeIds. Options are
  `securityPolicy`, `securityMode`, `pkiDir`, `certificateFile`,
  `privateKeyFile`, `applicationUri`. Secure modes require client credentials
  and pretrusted server certificates. Username authentication requires
  `SignAndEncrypt`.
* Files: `brokerUri=file:/absolute/directory` or `sftp://host:22/directory`.
  SFTP requires `host-key-fingerprint=SHA256:...` plus credentials in the
  `DeviceConnection` options (driver `DcdSftp`), for example
  `usernameEnv=PLC_SFTP_USER&passwordEnv=PLC_SFTP_PASSWORD&host-key-fingerprint=SHA256:...`;
  optional `private-key-file` names an unencrypted private key file. The remote
  directory must exist. Each file is written as `.<name>.part` and renamed, so
  the PLC never reads a partial file (an interrupted transfer can leave only the
  `.part` file).

### Recipes to a CODESYS PLC over SFTP

A CODESYS Control runtime on Linux (or any PLC with an SSH server) receives the
recipe files of moqui-device's `export#DeviceConfig` in its recipe directory:

```xml
<moqui.device.DeviceConnection connectionName="PLC1_SFTP" deviceId="PLC1" driverEnumId="DcdSftp"
        connectionTypeEnumId="DctDirectConnection" purposeEnumId="DcpMaintenance"
        options="usernameEnv=PLC1_SFTP_USER&amp;passwordEnv=PLC1_SFTP_PASSWORD&amp;host-key-fingerprint=SHA256:..."/>
<moqui.device.DeviceRequest requestName="PLC1_ExportProcess" deviceId="PLC1" requestTypeEnumId="DrtConfigWrite"
        routerEnumId="DrrDirect" runServiceName="moqui.device.gateway.FileServices.run#DeviceRequest"
        connectionName="PLC1_SFTP" brokerUri="sftp://plc1.factory.local:22/var/opt/codesys/PlcLogic/recipes"
        query="PLC1_RuleSet" timeout="30000"/>
```

Read the fingerprint with `ssh-keyscan -t ed25519 plc1.factory.local | ssh-keygen -lf -`.
A `+` of the fingerprint may be written as it is or as `%2B`.

### Redundant PLCs

The second PLC of a redundant pair gets its own `DeviceRequest`, with its
endpoint and `parentRequestName` = the request of the first PLC. After a
completed recipe export (`DrtConfigWrite`) or content transfer
(`DrtContentTransfer`), moqui-device's `run#DeviceRequest` sends the same files
there; a failure on the second PLC does not change the first one's result and is
in `secondaryResults` and in its own notification. The second request may send
the DeviceContent of its parent's device.

An item without `itemTypeEnumId` gets its physical type from the kind of its
parameter: a decimal number as a 64-bit float, an indicator as a boolean, a bit
set as 64 bits, text as text. That fits JSON over MQTT; set `itemTypeEnumId` for
an exact register or PLC type.

Options accept query-string or JSON string-map syntax. `usernameEnv` and
`passwordEnv` resolve credentials during Moqui preparation. Legacy literal
`username`/`userName`/`password` are accepted but omitted from diagnostics.
`DeviceConnection.userId` requires a credential provider and is explicitly
rejected: a Moqui password hash cannot authenticate an industrial device.
Unknown protocol options are rejected rather than silently ignored.

All request timeout and pollingInterval values are **milliseconds**. One deadline
covers session acquisition and all items. A timed-out write has an `uncertain`
outcome and is never automatically resubmitted. MQTT PUBACK confirms broker
acceptance, not application of a setpoint by the target device.

## Inputs and results

Besides the request and its items, `run#DeviceRequest` passes:

* `deviceContentId`: for `DrtContentTransfer`, the DeviceContent to send. It must
  belong to the device of the request.
* `transferFiles`: files to send, maps with `name` and `bytes` (binary) or
  `content` (text). moqui-device's recipe export
  (`moqui.device.DeviceConfigServices.export#DeviceConfig`) sends its files this
  way; a `DrtConfigWrite` to a file or SFTP endpoint is a transfer.
* `payload`: a map sent as one JSON message by an MQTT write without items, on
  the topic in `DeviceRequest.query`. moqui-device's
  `moqui.device.TrajectoryPlannerServices.export#Trajectory` publishes trajectories
  this way.

Finite calls return `status`, `itemResults`, `subscriptionId`,
`transferredFiles` and `errorMessage`. Status is `completed`, `partial`, `failed`,
`uncertain`, `active` or `unchanged`. `run#DeviceRequest` also records
failed/partial/uncertain outcomes in the Moqui message facade and notifies the
user of writes, exports and transfers. The Rust API retains completed item
results if a later operation fails.

## Subscriptions

Use `DrtUnsubscribe` with `parentRequestName` to stop the owning request. An active
request cannot be replaced without explicitly stopping it. Subscription ownership
is local to the mr-moqui process: run one assigned owner for a field device;
sharing a database does not coordinate sessions across hosts.

mr-moqui has no startup hook, so the service job `start_DeviceGatewaySubscriptions`
calls `start#Subscriptions` every minute. Its first run in a process starts the
subscriptions of the gateway `device_gateway_device_id`: the active
`DrtSubscribe`, `DrtEvent`, `DrtStateChange` and `DrtCyclic` requests, run by a
service of this component, of the controllers in its scope (members with purpose
`DgmpProcessPLC`, `DgmpSafetyPLC`, `DgmpController`, `DgmpMotionController` or a
remote I/O purpose, in the DeviceGroups the gateway is a `DgmpEdgeGateway`
member of, recursively, with membership status empty or `DgmsAlive`), ordered by
priority and sequenceNum. Later runs restart only the subscriptions that ended
without an unsubscribe, such as an MQTT subscription whose connection was lost.
An unsubscribed request stays stopped. No write is ever replayed.

When the values of a subscription or polling request fail to store for
`device_gateway_error_notification_seconds`, the user gets one notification on the
topic `DeviceGatewayInbound`, and another when they store again.

`DrpLogging` writes logs; a missing purpose updates current Parameter values.
`PlcLogServices.run#DeviceRequest` subscribes to PLC LoggerFacade batches: it
keeps `loggerName` as Device.deviceId and `source` as the exact
Parameter.parameterId; an empty source writes the complete event to DeviceLog.
CODESYS `DT#` timestamps are interpreted as UTC; RFC3339 timestamps retain their
offset. Existing Parameter and Device identities are required, and no unknown
parameters are created.
