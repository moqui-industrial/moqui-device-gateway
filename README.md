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

The rest of the Quarkus/Camel gateway moves here step by step: its SQL becomes
entity engine queries and its REST calls become local service calls.

The public application entry point remains
`moqui.device.DeviceServices.run#DeviceRequest`. Set the request's
`runServiceName` to `moqui.device.gateway.DeviceGatewayServices.run#DeviceRequest`.
Device, DeviceConnection, DeviceRequest, DeviceRequestItem, Parameter and
ParameterDef remain the existing Moqui entities. No additional database,
REST process, route interpreter or scheduler is introduced.

## Installation

Install this repository at `runtime/component/moqui-device-gateway` in mr-moqui,
with `moqui-device` and `moqui-math` on their `mr-moqui` branches. The component
uses the framework's sibling `moqui-core` and `moqui-lib` crates. It is not a
standalone checkout with respect to Cargo path dependency resolution.

Apply `patches/moqui-device-mr-moqui.patch` to moqui-device unless those changes
are already present. The patch exposes structured outcomes through the general
entry point, forwards content-transfer inputs and allows itemless unsubscribe.
It preserves the existing dispatch to GenICam and other implementations.

Build using the framework's `./moq build`, which links native components.
The component registers `moqui_device_gateway::DeviceGatewayToolFactory` as tool `DeviceGateway`
and its native service through the framework's native registry. Rust 1.99 or
later is required by this initial lockfile/toolchain baseline.

```sh
cd runtime/component/moqui-device-gateway
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
```

For a smaller transport-only build, use
`cargo test --locked --no-default-features --features modbus,mqtt`.
Cargo still resolves optional local framework manifests, so keep the component
in its installation directory. See [validation](docs/validation.md) for what
these checks prove and which deployment checks remain necessary.

## Included capabilities

| Backend | Read/write | Continuous acquisition | Files |
| --- | --- | --- | --- |
| Modbus TCP | coils and typed integer/float registers | explicit `DrtCyclic` polling | no |
| MQTT 5 | JSON values; gateway-compatible item publication envelope; QoS 1 PUBACK | broker subscriptions with SUBACK | no |
| OPC UA | typed values, per-node status and source timestamps | monitored-item subscriptions or explicit polling | no |
| Local directory | no | no | binary transfer, temporary file followed by rename |
| SFTP | no | no | binary transfer, pinned SSH host key, password or private key |

PROFINET, EtherNet/IP, Modbus TLS, browse/discovery, structured OPC UA values,
recipe/trajectory export and legacy gateway startup restoration are not yet
implemented. They return unsupported errors where applicable; there is no
fallback protocol. The existing gateway must remain available for workloads
not covered by this release. The complete migration checklist is in
[compatibility](docs/compatibility.md).

## One API, two callers

`DeviceGatewayFacade::execute` takes a prepared, owned request, input values, an
absolute monotonic deadline, output storage and an optional sample sink. The
`DeviceGatewayClient` trait is the backend contract. Neither requires an
ExecutionContext. See [direct Rust access](examples/direct_modbus.rs).

The Moqui native service resolves cached configuration using the Entity Engine,
prepares this same request, and persists finite reads before returning. A bounded
worker queue persists subscription/polling samples. Factory shutdown cancels
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
  SFTP requires `host-key-fingerprint=SHA256:...` plus credentials. Optional
  `private-key-file` names an unencrypted private key file. The remote directory
  must exist. SFTP writes can leave a partial file on failure; delivery is not
  atomic as it is for a local rename.

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

Finite calls return `status`, `itemResults`, `subscriptionId`,
`transferredFiles` and `errorMessage`. Status is `completed`, `partial`, `failed`,
`uncertain`, `active` or `unchanged`. The patched general service also records
failed/partial/uncertain outcomes in the Moqui message facade. The Rust API
retains completed item results if a later operation fails.

Use `DrtUnsubscribe` with `parentRequestName` to stop the owning request. An active
request cannot be replaced without explicitly stopping it. A disconnected MQTT
subscription ends and logs the failure; restarting is an explicit operation in
this release. Subscription ownership is local to the mr-moqui process. Run one
assigned owner for a field device; sharing a database does not coordinate
sessions across hosts.

`DrpLogging` writes logs; a missing purpose updates current Parameter values.
PLC LoggerFacade batches preserve `loggerName` as Device.deviceId and `source`
as the exact Parameter.parameterId. Empty source writes the complete event to
DeviceLog. An itemless logging MQTT subscription selects this batch format;
`requestGroup=plc-log` selects it explicitly. CODESYS `DT#` timestamps are
interpreted as UTC; RFC3339 timestamps retain their offset. Existing Parameter
and Device identities are required, and no unknown parameters are created.
