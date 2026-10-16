# Gateway configuration compatibility

The `mr-moqui` branch runs the gateway in Rust with Tokio/Axum and native protocol
clients. This guide covers migration from the Java implementation and the
supported compatibility settings. For Rust setup and operation, see [README.md](../README.md).

## Other implementation

The Java/Quarkus/Apache Camel implementation is documented on
[`main`](https://github.com/moqui/moqui-device-gateway/tree/main).
The Rust gateway accepts compatible configuration and URI formats using its
native clients, including the `paho-mqtt5:` URI syntax.

## Legacy configuration

The process reads `.env` (or `runtime/.env`), then configuration from environment
variables and an optional `.properties` file/directory selected by
`SMALLRYE_CONFIG_LOCATIONS`. Dotted gateway property names are supported for
migration; Camel route expressions and SmallRye interpolation are not evaluated.
Native Rust environment variables are recommended.

`APP_PROFILE` is the native profile setting; `QUARKUS_PROFILE` is accepted as a
legacy alias.

`QUARKUS_DATASOURCE_JDBC_URL` and `QUARKUS_DATASOURCE_LOG_JDBC_URL` remain aliases
for database URLs and accept the `jdbc:postgresql://` prefix. Put credentials in
the URL when using those aliases; separate Java username/password settings are
not automatically combined. The legacy `MQTT_WRITE_AFTERPUBLISH_ENABLED` is
accepted; prefer the native spelling with `_AFTER_PUBLISH_`.

## Java/Rust compatibility and remaining differences

| Contract | Rust branch compared with Java/Quarkus/Camel |
| --- | --- |
| Database and device scope | Same modeled devices, requests, parameters, configurations and content; Rust uses Moqui's Java-compatible SQL names |
| Core REST operations | Same run-request, transfer-content and export-config paths and JSON intent; Rust also enforces request scope |
| MQTT | MQTT **v5** through `rumqttc::v5`; Java uses Camel/Paho MQTT5. QoS acknowledgements, session options, credentials and TLS are implemented |
| File/SFTP | Native local file and SFTP transports, with known-host verification for SFTP |
| Modbus | Native TCP/TLS read, write and cyclic execution; not arbitrary Camel endpoint options |
| OPC UA | Read/write/subscriptions with None, Basic256Sha256 and AES policies; explicit trust, password and X509 identities; only the documented options are accepted |
| Trajectory MQTT export | Rust adds `/api/trajectory/export`; the reviewed Java branch has no matching endpoint yet |
| ONNX trajectory inference | Native optional moqui-device service using ort/ONNX Runtime, or an explicitly remote provider; see the [isolated Rust example](https://github.com/moqui/moqui-device/tree/mr-moqui/examples/onnx-obstacle) |

These are shared application contracts with documented remaining differences,
not a claim that every Camel route, endpoint option or deployment is interchangeable.
