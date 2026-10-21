# Migration boundary and remaining work

Source references used for the initial implementation:

* mr-moqui master `ed07b11228cab9296b4793c4b21f3e0fb036bed2`.
* moqui-plc4j original Groovy FieldbusClient and Plc4jUtil: dispatch, typed
  addresses, bulk indexing, parameter conversion and logging intent.
* moqui-device-gateway original Java GatewayRequestService, GatewayResource
  and static Camel route definitions: MQTT envelopes, read/write/subscription,
  LoggerFacade batches and file delivery.
* The existing gateway Rust port was not imported as the implementation base.

## Route inventory

| Original route/responsibility | Current implementation | Migration state |
| --- | --- | --- |
| dispatch-device-request | one run#DeviceRequest per protocol (Mqtt, OpcUa, Modbus, File, PlcLog Services) over DeviceGatewayFacade | DeviceRequest.runServiceName selects the service; no route interpreter |
| mqtt-read-device-request-consumer, mqtt-read-device-request | MQTT backend and JSON normalization | supported JSON scalar/object/list reads; not arbitrary Camel transforms |
| opcua-read-device-request-consumer, opcua-read-device-request | OPC UA backend | typed reads, monitored items and polling |
| store-device-request-inbound | InboundServices.store#Samples | Entity Engine; scoped existing identities |
| store-parameter-batch | bounded persistence worker calling store#Samples | current values, conversion, duplicate/tolerance policy of the DeviceRequestItem |
| store-log-batch | same worker | ParameterLog, original PLC type distinction |
| plc-log-consumer, plc-log-ingest | PlcLogServices.run#DeviceRequest: MQTT subscription + PLC batch decoder | explicit service per request; restored at startup like the other subscriptions |
| plc-log-ingest-with-source | InboundServices.store#PlcLog, ParameterLog | exact Parameter ID; no inferred identity |
| plc-log-ingest-without-source | InboundServices.store#PlcLog, DeviceLog | full event JSON |
| transfer-file, transfer-device-content | FileServices.run#DeviceRequest with deviceContentId or transferFiles; SFTP writes a .part file and renames it | native bytes, no base64 REST hop |
| file.transfer.uri.2 (second destination) | moqui-device run#DeviceRequest: the requests with parentRequestName = the primary (the redundant PLC) get the same files | done; a failure there does not change the primary result |
| after-store-callback | a SECA on InboundServices.store#Samples or store#PlcLog; moqui-device notify#DeviceRequestResult after writes, exports and transfers | done; no HTTP callback |
| InboundErrorNotifier | notifier.rs in the persistence worker, InboundServices.notify#InboundState: a Moqui notification (topic DeviceGatewayInbound) after device_gateway_error_notification_seconds of failures, and on recovery | done |
| run-device-config-export | moqui-device DeviceConfigServices.get#DeviceConfigExportFiles (the export SQL on the Entity Engine) and export#DeviceConfig, sent with FileServices | done |
| dynamic startup restoration/group recursion | SubscriptionServices.get#GatewayScope and get#StartupSubscriptions (the scope SQL of the former gateway: DgmpEdgeGateway groups, alive members recursively, controller purposes), start#Subscriptions from the service job start_DeviceGatewaySubscriptions; property device_gateway_device_id; a request outside the scope is refused | done: restores after a start, restarts lost subscriptions, never undoes an unsubscribe |
| trajectory service | moqui-device TrajectoryPlannerServices.get#TrajectoryAxes and export#Trajectory, sent as one MQTT payload message | done; the original gateway never exposed its advertised trajectory endpoint |

moqui-device on mr-moqui no longer has DeviceGatewayServices.xml or the
DrrMoquiDeviceGateway router: its requests name these protocol services
directly. Its HTTP helper for inference providers is
TrajectoryPlannerServices.call#InferenceEndpoint. Recipe selectors,
StatusFlow/StatusItem handling, compound types, target-specific formats and
per-device export conventions belong to moqui-device's general services, not to
a protocol backend. Keep these selectors on the Entity Engine.

## Differences requiring review

* A `DrtEvent`/`DrtStateChange` subscription requires an actual event backend;
  it is not silently converted into Modbus polling. Use `DrtCyclic` explicitly.
* Modbus reads support typed bulk arrays with ordered item indices. Bulk writes
  are explicitly rejected until a grouped, prevalidated PDU writer is added;
  use explicit scalar item addresses for writes in this release.
* Numeric conversions use Decimal, zero bounds and zero significant digits are
  meaningful, and zero scaling is rejected. Out-of-range writes and fractional
  writes to integer physical types fail instead of truncating.
* Inbound parameters must already exist and belong to the request Device.
  Recipes still bound only to deviceConfigId must first be materialized into
  device-bound Parameter values through the existing domain services.
* A PLC log request is scoped to one device. A single legacy topic carrying
  multiple devices needs separate scoped requests or a future explicit group
  ownership policy; it cannot impersonate another device through loggerName.
* Existing ParameterLog uniqueness constraints on parameterId/observedDate and
  parameterId/sequenceNum still apply. Duplicate source timestamps or competing
  writers cause observable persistence failures; timestamps are not fabricated.
* Native MQTT currently uses a separate clean session for each finite operation
  and each subscription. clientId becomes a prefix plus process/sequence suffix.
  Do not rely on old persistent client IDs or retained QoS sessions.
* TLS for MQTT uses system roots. Private CA/mTLS configuration and network
  reconnect/backoff need additional deployment-specific work and integration
  tests. OPC UA rejects untrusted certificates and deprecated security policies.
* SFTP requires an explicit host-key pin, not implicit trust on first use.
* The general service keeps its original audit-based onlyChangedParameters
  filtering. This is not a new acknowledgement watermark or cross-host scheduler.

## Next implementation increments

1. Test service/entity round trips on the supported production database with
   existing Device/Parameter seed data; add failure-injection persistence tests.
2. Add grouped Modbus bulk write PDUs. Bulk read items already share one
   response per configured address within a request.
3. Verify the recipe export files against the Quarkus gateway output for real
   rule sets, and the trajectory message against MqttParameterSub.
4. Verify owner assignment across instances (one owner per field device) with
   device_gateway_restore_subscriptions.
5. Add MQTT private CA/mTLS and a reconnect backoff for subscriptions (today
   start#Subscriptions restarts a lost one within a minute); never automatically
   replay a command write.
6. Validate pinned-host SFTP and secured OPC UA against real servers/devices.
7. PROFINET, EtherNet/IP and EtherCAT are not added (decision of 2026-10-21):
   drive parameters are read and written through the PLC, which owns the
   drives, and exposed over OPC UA or MQTT; see the README. Cyclic I/O and
   safety PLC control stay outside this component. Modbus TLS needs its own
   verified transport adapter.
8. Only after parity gates pass, migrate installations and remove obsolete
   gateway-specific service implementations.

GenICam remains a separate camera implementation of DeviceRequestInternal.
This initial device gateway component does not depend on its HTTP streaming changes
and does not replace the camera/Content pipeline.
