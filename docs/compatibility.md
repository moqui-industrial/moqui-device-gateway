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
| dispatch-device-request | DeviceConnectorFacade + Moqui service binding | native request selection; no route interpreter |
| mqtt-read-device-request-consumer, mqtt-read-device-request | MQTT backend and JSON normalization | supported JSON scalar/object/list reads; not arbitrary Camel transforms |
| opcua-read-device-request-consumer, opcua-read-device-request | OPC UA backend | typed reads, monitored items and polling |
| store-device-request-inbound | binding persistence | Entity Engine; scoped existing identities |
| store-parameter-batch | bounded persistence worker | current values, conversion, duplicate/tolerance policy |
| store-log-batch | same worker | ParameterLog, original PLC type distinction |
| plc-log-consumer, plc-log-ingest | MQTT subscription + PLC batch decoder | itemless logging requests; no static startup auto-subscription |
| plc-log-ingest-with-source | ParameterLog persistence | exact Parameter ID; no inferred identity |
| plc-log-ingest-without-source | DeviceLog persistence | full event JSON |
| transfer-file, transfer-device-content | file/SFTP backends and resource preparation | native bytes, no base64 REST hop |
| after-store-callback | existing Moqui entity ECAs remain applicable | legacy HTTP callback parity is pending |
| run-device-config-export | existing device services retained | DeviceRuleSet/DeviceConfig rendering and transfer adapter pending |
| dynamic startup restoration/group recursion | existing scheduler/deployment remains | explicit owner/startup service pending |
| trajectory service | original general service retained | native renderer/transfer integration pending; original gateway did not expose its advertised trajectory endpoint |

Do not delete DeviceGatewayServices.xml or remove the old gateway merely by
installing this component. Its generic HTTP helper is used by other services,
and exports/callbacks/restoration still need explicit migration and tests.
Recipe selectors, StatusFlow/StatusItem handling, compound types, target-specific
formats and per-device export conventions belong to moqui-device's general
services, not to a protocol backend. Keep these selectors on the Entity Engine.

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
3. Port DeviceConfig/DeviceRuleSet and trajectory renderers into general
   moqui-device services, supplying immutable files/values to this API.
4. Replace legacy HTTP callback/bootstrap paths with explicit Moqui services and
   framework schedules; verify owner assignment and restart semantics.
5. Add MQTT private CA/mTLS and tested reconnect policies for subscriptions;
   never automatically replay a command write.
6. Validate pinned-host SFTP and secured OPC UA against real servers/devices.
7. Select proven PROFINET Supervisor and EtherNet/IP explicit-message stacks.
   Keep cyclic I/O controller ownership and safety PLC control outside this
   initial implementation. Modbus TLS needs its own verified transport adapter.
8. Only after parity gates pass, migrate installations and remove obsolete
   gateway-specific service implementations.

GenICam remains a separate camera implementation of DeviceRequestInternal.
This initial device connector component does not depend on its HTTP streaming changes
and does not replace the camera/Content pipeline.
