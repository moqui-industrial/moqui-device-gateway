// run#DeviceGatewayRequest and run#PlcLogRequest on the drivers of this component, with moqui-math and moqui-device
// installed. From the framework root:
//     ./moq test --debug moqui-device-gateway
// or with local Modbus, OPC UA, SFTP and MQTT peers (and EtherNet/IP with CPPPO_PYTHON): scripts/run-driver-tests.py.
// The requests are those of moqui-device's DeviceTestData.xml and HVAC demo data. The services common to every
// driver (recipe export, content transfer, sample store, gateway scope) are tested by moqui-device's
// DeviceRequests.spec.js. Optional features:
//   gateway_mqtt_test=1: the MQTT 5 broker of moqui-device@broker-uri, @broker-username and @broker-password,
//     which every broker request uses;
//   moqui-device@gateway-device-id HVAC_DEMO_GATEWAY (or none): the HVAC demo, whose devices are outside GW_EDGE_01;
//   moqui-device-gateway@error-notification-seconds 0 with gateway_mqtt_test: the persistence failure notification;
//   moqui-device@gateway-device-id GW_EDGE_01: requests outside the gateway scope are refused;
//   gateway_opcua_test=1: the OPC UA server of `cargo run --example opcua_test_server` on 127.0.0.1:12686;
//   gateway_modbus_test=1 and gateway_modbus_endpoint=modbus-tcp://host:port: a Modbus TCP server
//     (scripts/modbus_peer.py);
//   gateway_eip_endpoint=logix://host:port: an EtherNet/IP Logix server with a DINT tag Speed (cpppo).
spec({
    setup() { ec.artifactExecution.disableAuthz(); },
    cleanup() { ec.artifactExecution.enableAuthz(); },
});

// A conf attribute of this instance, empty when not set
const confValue = (selector) => ec.factory.getConfValue(selector) || "";
const gatewayService = "moqui.device.gateway.DeviceGatewayServices.run#DeviceGatewayRequest";
const plcLogService = "moqui.device.gateway.DeviceGatewayServices.run#PlcLogRequest";
const storeRequest = (fields) => call("store#moqui.device.DeviceRequest",
    Object.assign({timeout: 5000, runServiceName: gatewayService}, fields));
const storeItem = (fields) => call("store#moqui.device.DeviceRequestItem", fields);
// a DeviceConnection of VIRTUAL_PLC_01: the endpoint, driver and credentials of a direct request
const storeConnection = (connectionName, transportConfig, driverEnumId, fields) =>
    call("store#moqui.device.DeviceConnection", Object.assign({connectionName, deviceId: "VIRTUAL_PLC_01",
        connectionTypeEnumId: "DctDirectConnection", purposeEnumId: "DcpOperations", transportConfig, driverEnumId}, fields));
const deleteConnections = (...names) => names.forEach(connectionName =>
    ec.entity.find("moqui.device.DeviceConnection").condition("connectionName", connectionName).deleteAll());
const run = (requestName, fields) => call("moqui.device.DeviceServices.run#DeviceRequest", Object.assign({requestName}, fields));
const deleteRequests = (...names) => names.forEach(requestName => {
    ec.entity.find("moqui.device.DeviceRequestItem").condition("requestName", requestName).deleteAll();
    ec.entity.find("moqui.device.DeviceRequest").condition("requestName", requestName).deleteAll();
});
// publish one JSON message on a topic, through an MQTT write request without items
const publish = (topic, payload) => {
    storeRequest({requestName: "GatewayTestPublish", deviceId: "VIRTUAL_PLC_01", requestTypeEnumId: "DrtWrite",
        routerEnumId: "DrrMQTTBroker", query: topic});
    return run("GatewayTestPublish", {payload});
};
// wait for a condition the persistence worker makes true, at most a few seconds
const waitFor = (condition, millis = 8000) => {
    const started = Date.now();
    let value = condition();
    while (!value && Date.now() - started < millis) value = condition();
    return value;
};
const failureOf = (action) => {
    try {
        action();
        return ec.message.hasError() ? ec.message.getErrorsString() : null;
    } catch (e) {
        return String(e.message || e);
    }
};
const parameterNumber = (parameterId) => ec.entity.find("moqui.math.Parameter").condition("parameterId", parameterId)
    .useCache(false).one().numericValue?.toNumber();
const setReference = (value) => call("update#moqui.math.Parameter", {parameterId: "TestPlcReferenceParam", numericValue: value});
const resetReference = () => setReference(300);
const notificationCount = (...topics) => ec.entity.find("moqui.security.user.NotificationMessage")
    .condition("topic", "in", topics).useCache(false).count();

// ---- requests every driver refuses

feature("a PLC log service refuses a request whose endpoint is another protocol", {
    allowMessageErrors: true,
    when() {
        const request = ec.entity.find("moqui.device.DeviceRequest").condition("requestName", "TestPlcOpcUaWriteReq01").one();
        const device = ec.entity.find("moqui.device.Device").condition("deviceId", request.deviceId).one();
        const connection = ec.entity.find("moqui.device.DeviceConnection").condition("connectionName", request.connectionName).one();
        const failure = failureOf(() => call(plcLogService, {request, device, connection}));
        ec.message.clearErrors();
        return {failure};
    },
    then(out) {
        out.failure != null;
        out.failure.includes("MQTT");
    },
});

feature("a payload is refused by a protocol that cannot send it", {
    allowMessageErrors: true,
    when() {
        const failure = failureOf(() => run("TestPlcOpcUaWriteReq01", {payload: {x: 1}}));
        ec.message.clearErrors();
        return {failure};
    },
    then(out) {
        out.failure != null;
        out.failure.includes("payload");
    },
});

feature("a read, a polling and a cyclic subscription of an MQTT topic are refused", {
    allowMessageErrors: true,
    when() {
        const failures = {};
        for (const [requestName, requestTypeEnumId, pollingInterval] of
            [["GatewayMqttRead", "DrtRead", null], ["GatewayMqttPoll", "DrtRead", 100], ["GatewayMqttCyclic", "DrtCyclic", 100]]) {
            storeRequest({requestName, deviceId: "VIRTUAL_PLC_01", requestTypeEnumId, pollingInterval,
                routerEnumId: "DrrMQTTBroker"});
            storeItem({requestName, requestItemName: "value", query: "plc/value", parameterId: "TestPlcFeedbackParam",
                itemTypeEnumId: "DritUInt"});
            let out = null;
            const failure = failureOf(() => { out = run(requestName); });
            failures[requestName] = failure || (out && out.status + ": " + out.errorMessage);
            ec.message.clearErrors();
        }
        return failures;
    },
    then(r) {
        r.GatewayMqttRead.includes("Mqtt cannot read or poll");
        r.GatewayMqttPoll.includes("Mqtt cannot read or poll");
        r.GatewayMqttCyclic.includes("MQTT has no cyclic subscription");
    },
    cleanup() { deleteRequests("GatewayMqttRead", "GatewayMqttPoll", "GatewayMqttCyclic"); },
});

feature("an endpoint of no driver is refused", {
    allowMessageErrors: true,
    when() {
        storeConnection("GatewayHttpConn", "http://127.0.0.1:8081/api", "DcdMtConnect");
        storeRequest({requestName: "GatewayHttpRead", deviceId: "VIRTUAL_PLC_01", requestTypeEnumId: "DrtRead",
            routerEnumId: "DrrDirect", connectionName: "GatewayHttpConn", query: "x"});
        const failure = failureOf(() => run("GatewayHttpRead"));
        ec.message.clearErrors();
        return {failure};
    },
    then(out) {
        out.failure.includes("Unsupported fieldbus protocol");
    },
    cleanup() {
        deleteRequests("GatewayHttpRead");
        deleteConnections("GatewayHttpConn");
    },
});

feature("a direct request needs a DeviceConnection, whose credentials are not in its options", {
    allowMessageErrors: true,
    when() {
        const failures = {};
        const attempt = (name, fields) => {
            storeRequest(Object.assign({requestName: "GatewayCredentialRead", deviceId: "VIRTUAL_PLC_01",
                requestTypeEnumId: "DrtRead", routerEnumId: "DrrDirect"}, fields));
            storeItem({requestName: "GatewayCredentialRead", requestItemName: "value", query: "holding-register:0:UINT",
                parameterId: "TestPlcFeedbackParam", itemTypeEnumId: "DritUInt"});
            failures[name] = failureOf(() => run("GatewayCredentialRead"));
            ec.message.clearErrors();
        };
        attempt("noConnection", {});
        storeConnection("GatewayCredentialConn", "modbus-tcp://127.0.0.1:9", "DcdModbusTCP", {options: "password=secret"});
        attempt("clearPassword", {connectionName: "GatewayCredentialConn"});
        return failures;
    },
    then(out) {
        out.noConnection.includes("needs a DeviceConnection");
        out.clearPassword.includes("DeviceConnection.username and password");
    },
    cleanup() {
        deleteRequests("GatewayCredentialRead");
        deleteConnections("GatewayCredentialConn");
    },
});

feature("with a configured gateway a request outside its scope is refused", {
    ignore: confValue("moqui-device@gateway-device-id") !== "GW_EDGE_01",
    allowMessageErrors: true,
    when() {
        const failure = failureOf(() => run("HVAC_DEMO_PlcLogSubscribe"));
        ec.message.clearErrors();
        return {failure};
    },
    then(out) {
        out.failure != null;
        out.failure.includes("not in the scope of gateway GW_EDGE_01");
    },
});

// ---- MQTT

feature("an MQTT write publishes the request items", {
    ignore: !System.getProperty("gateway_mqtt_test"),
    when() {
        return run("TestPlcMqttPublishReq01");
    },
    then(out) {
        out.status == "completed";
        out.itemResults.length == 2;
    },
});

feature("the parameter sync request publishes the PLC parameters", {
    ignore: !System.getProperty("gateway_mqtt_test"),
    when() {
        call("update#moqui.device.DeviceRequest", {requestName: "VIRTUAL_PLC_01_SyncParameters", onlyChangedParameters: "N"});
        return run("VIRTUAL_PLC_01_SyncParameters");
    },
    then(out) {
        out.status == "completed";
        out.itemResults.length > 0;
    },
    cleanup() {
        call("update#moqui.device.DeviceRequest", {requestName: "VIRTUAL_PLC_01_SyncParameters", onlyChangedParameters: "Y"});
    },
});

feature("a trajectory export publishes one MQTT message", {
    ignore: !System.getProperty("gateway_mqtt_test"),
    when() {
        // a trajectory of the test data: planning one needs the onnx feature of moqui-device
        const sample = ec.entity.find("moqui.math.ApproximatedFunctionSample").list()[0];
        storeRequest({requestName: "GatewayTestTrajectory", deviceId: "VIRTUAL_PLC_01",
            requestTypeEnumId: "DrtWrite", routerEnumId: "DrrMQTTBroker",
            query: "moqui/test/trajectory"});
        return call("moqui.device.DeviceServices.export#Trajectory",
            {approximatedFunctionId: sample.approximatedFunctionId, requestName: "GatewayTestTrajectory"});
    },
    then(out) {
        out.status == "completed";
    },
    cleanup() { deleteRequests("GatewayTestTrajectory"); },
});

feature("an MQTT subscription starts once, answers unchanged while running, and stops on unsubscribe", {
    ignore: !System.getProperty("gateway_mqtt_test"),
    when() {
        const started = run("TestPlcMqttSubscribeReq01");
        const again = run("TestPlcMqttSubscribeReq01");
        const stopped = run("TestPlcMqttUnsubscribeReq01");
        return {started, again, stopped};
    },
    then(out) {
        out.started.status == "active";
        out.again.status == "unchanged";
        out.stopped.status == "completed";
    },
});

feature("an MQTT subscription stores the values it receives", {
    ignore: !System.getProperty("gateway_mqtt_test"),
    when() {
        const started = run("TestPlcMqttSubscribeReq01");
        const published = publish("mqtt-subscribe-device-request/virtual-plc/feedback", {virtualPlcFeedback: 42.25});
        const stored = waitFor(() => parameterNumber("TestPlcFeedbackParam") === 42.25);
        run("TestPlcMqttUnsubscribeReq01");
        return {started, published, stored, value: parameterNumber("TestPlcFeedbackParam")};
    },
    then(out) {
        out.started.status == "active";
        out.published.status == "completed";
        out.stored == true;
        out.value == 42.25;
    },
    cleanup() { deleteRequests("GatewayTestPublish"); },
});

feature("MQTT 5 delivers values with the QoS 0, 1 and 2 of each request", {
    ignore: !System.getProperty("gateway_mqtt_test"),
    when() {
        const delivered = [];
        for (const qos of [0, 1, 2]) {
            const topic = `mqtt-read-device-request/native/qos/${qos}`;
            const common = {deviceId: "VIRTUAL_PLC_01", routerEnumId: "DrrMQTTBroker", query: topic, qos};
            storeRequest({...common, requestName: "GatewayQosSubscribe", requestTypeEnumId: "DrtSubscribe"});
            storeItem({requestName: "GatewayQosSubscribe", requestItemName: "value",
                parameterId: "TestPlcFeedbackParam", query: topic, itemTypeEnumId: "DritReal"});
            storeRequest({...common, requestName: "GatewayQosPublish", requestTypeEnumId: "DrtWrite"});
            storeRequest({...common, requestName: "GatewayQosStop", requestTypeEnumId: "DrtUnsubscribe",
                parentRequestName: "GatewayQosSubscribe"});
            run("GatewayQosSubscribe");
            const expected = 500 + qos;
            try {
                const written = run("GatewayQosPublish", {payload: {value: expected}});
                delivered.push(written.status === "completed"
                    && waitFor(() => parameterNumber("TestPlcFeedbackParam") === expected));
            } finally {
                run("GatewayQosStop");
            }
        }
        return {delivered};
    },
    then(out) {
        out.delivered.length == 3;
        out.delivered.every(value => value === true);
    },
    cleanup() { deleteRequests("GatewayQosStop", "GatewayQosSubscribe", "GatewayQosPublish"); },
});

feature("an unreadable MQTT item does not drop its valid sibling or stop subsequent collecting", {
    ignore: !System.getProperty("gateway_mqtt_test") || confValue("moqui-device-gateway@error-notification-seconds") !== "0",
    when() {
        const topic = "mqtt-read-device-request/native/partial-collection";
        const common = {deviceId: "VIRTUAL_PLC_01", routerEnumId: "DrrMQTTBroker", query: topic};
        storeRequest({...common, requestName: "GatewayPartialRead", requestTypeEnumId: "DrtSubscribe"});
        storeRequest({...common, requestName: "GatewayPartialStop", requestTypeEnumId: "DrtUnsubscribe",
            parentRequestName: "GatewayPartialRead"});
        storeItem({requestName: "GatewayPartialRead", parameterId: "TestPlcReferenceParam", requestItemName: "bad",
            query: topic, sequenceNum: 10, itemTypeEnumId: "DritReal"});
        storeItem({requestName: "GatewayPartialRead", parameterId: "TestPlcFeedbackParam", requestItemName: "good",
            query: topic, sequenceNum: 20, itemTypeEnumId: "DritReal"});
        const notices = () => notificationCount("DeviceInboundFailed", "DeviceInboundRecovered");
        const before = notices();
        run("GatewayPartialRead");
        try {
            publish(topic, {bad: "not numeric", good: 611});
            const first = waitFor(() => parameterNumber("TestPlcFeedbackParam") === 611 && notices() > before);
            publish(topic, {bad: "still unreadable", good: 612});
            const next = waitFor(() => parameterNumber("TestPlcFeedbackParam") === 612);
            return {first, next};
        } finally {
            run("GatewayPartialStop");
        }
    },
    then(out) {
        out.first == true;
        out.next == true;
    },
    cleanup() { deleteRequests("GatewayPartialStop", "GatewayPartialRead", "GatewayTestPublish"); },
});

feature("a burst of ten MQTT messages is logged ten times", {
    ignore: !System.getProperty("gateway_mqtt_test"),
    when() {
        storeRequest({requestName: "GatewayTestBurst", deviceId: "VIRTUAL_PLC_01", requestTypeEnumId: "DrtSubscribe",
            purposeEnumId: "DrpLogging", routerEnumId: "DrrMQTTBroker", query: "moqui/test/burst"});
        storeItem({requestName: "GatewayTestBurst", requestItemName: "value", parameterId: "TestPlcFeedbackParam",
            sequenceNum: 10, query: "moqui/test/burst", itemTypeEnumId: "DritReal"});
        storeRequest({requestName: "GatewayTestBurstStop", parentRequestName: "GatewayTestBurst", deviceId: "VIRTUAL_PLC_01",
            requestTypeEnumId: "DrtUnsubscribe", routerEnumId: "DrrMQTTBroker"});
        const count = () => ec.entity.find("moqui.math.ParameterLog").condition("parameterId", "TestPlcFeedbackParam").useCache(false).count();
        const before = count();
        const started = run("GatewayTestBurst");
        for (let i = 1; i <= 10; i++) publish("moqui/test/burst", {value: 100 + i});
        waitFor(() => count() >= before + 10);
        const logged = count() - before;
        run("GatewayTestBurstStop");
        return {started, logged};
    },
    then(out) {
        out.started.status == "active";
        out.logged == 10;
    },
    cleanup() { deleteRequests("GatewayTestBurstStop", "GatewayTestBurst", "GatewayTestPublish"); },
});

// ---- PLC log over MQTT (the CODESYS LogDispatcher of moqui-plc)

feature("the HVAC demo publishes the live parameters and stores the PLC log across the plant hierarchy", {
    // HVAC_DEMO_PLC is in the scope of HVAC_DEMO_GATEWAY only
    ignore: !System.getProperty("gateway_mqtt_test")
        || !["", "HVAC_DEMO_GATEWAY"].includesconfValue("moqui-device@gateway-device-id"),
    when() {
        call("update#moqui.device.DeviceRequest", {requestName: "HVAC_DEMO_LiveParametersWrite", onlyChangedParameters: "N"});
        const write = run("HVAC_DEMO_LiveParametersWrite");
        const subscription = run("HVAC_DEMO_PlcLogSubscribe");
        // what the CODESYS LogDispatcher publishes, sent here by an MQTT write without items
        storeRequest({requestName: "GatewayTestPlcLogPublish", deviceId: "HVAC_DEMO_PLC",
            requestTypeEnumId: "DrtWrite", routerEnumId: "DrrMQTTBroker", query: "moqui-plc"});
        const paramCount = () => ec.entity.find("moqui.math.ParameterLog").condition("parameterId", "HvacTempSetpoint").useCache(false).count();
        const pumpCount = () => ec.entity.find("moqui.device.DeviceLog").condition("deviceId", "HVAC_COLD_GLYCOL_PUMP").useCache(false).count();
        const beforeParam = paramCount();
        const beforePump = pumpCount();
        const publishParam = run("GatewayTestPlcLogPublish",
            {payload: {loggerName: "HVAC_DEMO_PLC", source: "HvacTempSetpoint", type: 1, numericValue: 21.5,
                sequence: 101, logEventDate: "2026-10-21T10:00:00Z"}});
        const publishPump = run("GatewayTestPlcLogPublish",
            {payload: {loggerName: "HVAC_COLD_GLYCOL_PUMP", source: "", type: 3, message: "Glycol pump started",
                sequence: 102, logEventDate: "2026-10-21T10:00:01Z"}});
        waitFor(() => paramCount() > beforeParam && pumpCount() > beforePump);
        const pumpLog = ec.entity.find("moqui.device.DeviceLog").condition("deviceId", "HVAC_COLD_GLYCOL_PUMP")
            .condition("sequenceNum", 102).useCache(false).one();
        storeRequest({requestName: "GatewayTestPlcLogUnsubscribe", deviceId: "HVAC_DEMO_PLC",
            parentRequestName: "HVAC_DEMO_PlcLogSubscribe", requestTypeEnumId: "DrtUnsubscribe", routerEnumId: "DrrMQTTBroker",
            runServiceName: plcLogService});
        run("GatewayTestPlcLogUnsubscribe");
        return {
            write, subscription, publishParam, publishPump,
            storedParam: paramCount() - beforeParam,
            storedPump: pumpCount() - beforePump,
            pumpSeq: pumpLog ? Number(String(pumpLog.sequenceNum)) : null,
        };
    },
    then(out) {
        out.write.status == "completed";
        out.write.itemResults.length == 20;
        out.subscription.status == "active";
        out.publishParam.status == "completed";
        out.publishPump.status == "completed";
        out.storedParam == 1;
        out.storedPump == 1;
        out.pumpSeq == 102;
    },
    cleanup() {
        ec.entity.find("moqui.device.DeviceLog").condition("deviceId", "HVAC_COLD_GLYCOL_PUMP").condition("sequenceNum", 102).deleteAll();
        ec.entity.find("moqui.math.ParameterLog").condition("parameterId", "HvacTempSetpoint").condition("sequenceNum", 101).deleteAll();
        deleteRequests("GatewayTestPlcLogPublish", "GatewayTestPlcLogUnsubscribe");
        call("update#moqui.device.DeviceRequest", {requestName: "HVAC_DEMO_LiveParametersWrite", onlyChangedParameters: "Y"});
    },
});

feature("PLC log events go to ParameterLog or DeviceLog, and a malformed batch does not stop the subscription", {
    ignore: !System.getProperty("gateway_mqtt_test"),
    when() {
        storeRequest({requestName: "GatewayTestPlcLog", deviceId: "VIRTUAL_PLC_01", requestTypeEnumId: "DrtSubscribe",
            purposeEnumId: "DrpLogging", routerEnumId: "DrrMQTTBroker", runServiceName: plcLogService,
            query: "moqui/test/plc-log"});
        storeRequest({requestName: "GatewayTestPlcLogStop", parentRequestName: "GatewayTestPlcLog", deviceId: "VIRTUAL_PLC_01",
            requestTypeEnumId: "DrtUnsubscribe", routerEnumId: "DrrMQTTBroker", runServiceName: plcLogService});
        const parameterLogs = () => ec.entity.find("moqui.math.ParameterLog").condition("parameterId", "TestPlcFeedbackParam").useCache(false).count();
        const deviceLogs = () => ec.entity.find("moqui.device.DeviceLog").condition("deviceId", "VIRTUAL_PLC_01").useCache(false).count();
        const started = run("GatewayTestPlcLog");
        const p0 = parameterLogs(), d0 = deviceLogs();
        // numeric and text events with a source, an event without one, a CODESYS DT# date
        publish("moqui/test/plc-log", {loggerName: "VIRTUAL_PLC_01", source: "TestPlcFeedbackParam", type: 1, numericValue: 7.5,
            logEventDate: "2026-10-21T08:00:01Z"});
        publish("moqui/test/plc-log", {loggerName: "VIRTUAL_PLC_01", source: "TestPlcFeedbackParam", type: 3, message: "running",
            logEventDate: "2026-10-21T08:00:02Z"});
        publish("moqui/test/plc-log", {loggerName: "VIRTUAL_PLC_01", source: "", message: "cycle overrun", logEventDate: "2026-10-21T08:00:03Z"});
        publish("moqui/test/plc-log", {loggerName: "VIRTUAL_PLC_01", source: "TestPlcFeedbackParam", type: 1, numericValue: 8.5,
            logEventDate: "DT#2026-10-21-08:00:04"});
        // a mixed batch: one event per key
        publish("moqui/test/plc-log", {"1": {loggerName: "VIRTUAL_PLC_01", source: "TestPlcFeedbackParam", type: 1, numericValue: 9.5,
            logEventDate: "2026-10-21T08:00:05Z"}, "2": {loggerName: "VIRTUAL_PLC_01", message: "batch event", logEventDate: "2026-10-21T08:00:06Z"}});
        // malformed (no loggerName), then a valid event that must still be stored
        publish("moqui/test/plc-log", {unexpected: true});
        publish("moqui/test/plc-log", {loggerName: "VIRTUAL_PLC_01", source: "TestPlcFeedbackParam", type: 1, numericValue: 10.5,
            logEventDate: "2026-10-21T08:00:07Z"});
        waitFor(() => parameterLogs() >= p0 + 5 && deviceLogs() >= d0 + 2);
        const dtLog = ec.entity.find("moqui.math.ParameterLog").condition("parameterId", "TestPlcFeedbackParam")
            .condition("numericValue", 8.5).useCache(false).one();
        const textLog = ec.entity.find("moqui.math.ParameterLog").condition("parameterId", "TestPlcFeedbackParam")
            .condition("symbolicValue", "running").useCache(false).one();
        run("GatewayTestPlcLogStop");
        return {started, parameterLogs: parameterLogs() - p0, deviceLogs: deviceLogs() - d0,
            dtMillis: dtLog ? Number(String(dtLog.observedDate)) : null, textLog: textLog != null};
    },
    then(out) {
        out.started.status == "active";
        out.parameterLogs == 5;
        out.deviceLogs == 2;
        out.textLog == true;
        // DT#2026-10-21-08:00:04 is UTC
        out.dtMillis == Date.UTC(2026, 9, 21, 8, 0, 4);
    },
    cleanup() { deleteRequests("GatewayTestPlcLogStop", "GatewayTestPlcLog", "GatewayTestPublish"); },
});

feature("persistence failures and the recovery are notified to the user", {
    ignore: !System.getProperty("gateway_mqtt_test") || confValue("moqui-device-gateway@error-notification-seconds") !== "0",
    when() {
        storeRequest({requestName: "GatewayTestFailing", deviceId: "VIRTUAL_PLC_01", requestTypeEnumId: "DrtSubscribe",
            purposeEnumId: "DrpLogging", routerEnumId: "DrrMQTTBroker", runServiceName: plcLogService,
            query: "moqui/test/failing"});
        storeRequest({requestName: "GatewayTestFailingStop", parentRequestName: "GatewayTestFailing", deviceId: "VIRTUAL_PLC_01",
            requestTypeEnumId: "DrtUnsubscribe", routerEnumId: "DrrMQTTBroker", runServiceName: plcLogService});
        const failedNotices = () => notificationCount("DeviceInboundFailed");
        const recoveredNotices = () => notificationCount("DeviceInboundRecovered");
        const f0 = failedNotices(), r0 = recoveredNotices();
        run("GatewayTestFailing");
        // an exact parameterId that does not exist: the store fails
        publish("moqui/test/failing", {loggerName: "VIRTUAL_PLC_01", source: "NoSuchParameter", type: 1, numericValue: 1,
            logEventDate: "2026-10-21T09:00:01Z"});
        const failing = waitFor(() => failedNotices() >= f0 + 1);
        publish("moqui/test/failing", {loggerName: "VIRTUAL_PLC_01", source: "TestPlcFeedbackParam", type: 1, numericValue: 2,
            logEventDate: "2026-10-21T09:00:02Z"});
        const recovered = waitFor(() => recoveredNotices() >= r0 + 1);
        run("GatewayTestFailingStop");
        return {failing, recovered};
    },
    then(out) {
        out.failing == true;
        out.recovered == true;
    },
    cleanup() { deleteRequests("GatewayTestFailingStop", "GatewayTestFailing", "GatewayTestPublish"); },
});

feature("startup restores eligible subscriptions without resurrecting explicitly stopped ones", {
    // the restoration state is per process: no other feature calls start#Subscriptions
    ignore: !System.getProperty("gateway_mqtt_test") || confValue("moqui-device@gateway-device-id") !== "GW_EDGE_01",
    when() {
        // earlier features explicitly unsubscribed the seeded MQTT request: the restoration must respect that stop
        const topic = "mqtt-read-device-request/native/startup-fresh";
        storeRequest({requestName: "GatewayStartupFresh", deviceId: "VIRTUAL_PLC_01", requestTypeEnumId: "DrtSubscribe",
            routerEnumId: "DrrMQTTBroker", query: topic});
        storeItem({requestName: "GatewayStartupFresh", parameterId: "TestPlcFeedbackParam", requestItemName: "value",
            query: topic, itemTypeEnumId: "DritReal"});
        storeRequest({requestName: "GatewayStartupFreshStop", parentRequestName: "GatewayStartupFresh", deviceId: "VIRTUAL_PLC_01",
            requestTypeEnumId: "DrtUnsubscribe", routerEnumId: "DrrMQTTBroker"});
        const first = call("moqui.device.DeviceAcquisitionServices.start#Subscriptions", {});
        const second = call("moqui.device.DeviceAcquisitionServices.start#Subscriptions", {});
        run("GatewayStartupFreshStop");
        const afterStop = call("moqui.device.DeviceAcquisitionServices.start#Subscriptions", {});
        if (first.startedRequestNames.includes("TestPlcOpcUaReadReq01")) run("TestPlcOpcUaUnsubscribeReq01");
        return {first, second, afterStop, opcua: !!System.getProperty("gateway_opcua_test")};
    },
    then(out) {
        out.first.startedRequestNames.includes("GatewayStartupFresh");
        !out.first.startedRequestNames.includes("TestPlcMqttSubscribeReq01");
        out.first.stoppedRequestNames.includes("TestPlcMqttSubscribeReq01");
        // the OPC UA subscription starts only with the OPC UA test server
        (out.opcua ? out.first.startedRequestNames : out.first.failedRequestNames).includes("TestPlcOpcUaReadReq01");
        !out.second.startedRequestNames.includes("GatewayStartupFresh");
        !out.second.failedRequestNames.includes("GatewayStartupFresh");
        out.afterStop.stoppedRequestNames.includes("GatewayStartupFresh");
        out.afterStop.startedRequestNames.length == 0;
    },
    cleanup() { deleteRequests("GatewayStartupFreshStop", "GatewayStartupFresh"); },
});

// ---- OPC UA

const writeReference = (value) => {
    setReference(value);
    return run("TestPlcOpcUaWriteReq01");
};

feature("OPC UA write and one-shot read go through the node into the parameters", {
    ignore: !System.getProperty("gateway_opcua_test"),
    when() {
        const write = writeReference(123.5);
        const read = run("TestPlcOpcUaOneshotReq01");
        return {write, read, feedback: parameterNumber("TestPlcFeedbackParam")};
    },
    then(out) {
        out.write.status == "completed";
        out.read.status == "completed" || out.read.status == "unchanged";
        out.feedback == 123.5;
    },
    cleanup() { resetReference(); },
});

feature("OPC UA cyclic subscription stores the node changes until the unsubscribe", {
    ignore: !System.getProperty("gateway_opcua_test"),
    when() {
        const started = run("TestPlcOpcUaReadReq01");
        writeReference(124.5);
        const stored = waitFor(() => parameterNumber("TestPlcFeedbackParam") === 124.5);
        const stopped = run("TestPlcOpcUaUnsubscribeReq01");
        return {started, stored, stopped};
    },
    then(out) {
        out.started.status == "active";
        out.stored == true;
        out.stopped.status == "completed";
    },
    cleanup() { resetReference(); },
});

feature("an OPC UA monitored item stores the live node value", {
    ignore: !System.getProperty("gateway_opcua_test"),
    when() {
        storeRequest({requestName: "GatewayTestOpcUaMonitor", deviceId: "VIRTUAL_PLC_01", requestTypeEnumId: "DrtStateChange",
            routerEnumId: "DrrDirect", connectionName: "TestPlcOpcUaConn01"});
        storeItem({requestName: "GatewayTestOpcUaMonitor", requestItemName: "virtualPlcFeedback",
            parameterId: "TestPlcFeedbackParam", sequenceNum: 10, query: "ns=2;s=Demo.Dynamic.Scalar.Double", itemTypeEnumId: "DritLReal"});
        storeRequest({requestName: "GatewayTestOpcUaMonitorStop", parentRequestName: "GatewayTestOpcUaMonitor", deviceId: "VIRTUAL_PLC_01",
            requestTypeEnumId: "DrtUnsubscribe", routerEnumId: "DrrDirect", connectionName: "TestPlcOpcUaConn01"});
        const started = run("GatewayTestOpcUaMonitor");
        writeReference(125.5);
        const stored = waitFor(() => parameterNumber("TestPlcFeedbackParam") === 125.5);
        run("GatewayTestOpcUaMonitorStop");
        return {started, stored};
    },
    then(out) {
        out.started.status == "active";
        out.stored == true;
    },
    cleanup() {
        deleteRequests("GatewayTestOpcUaMonitorStop", "GatewayTestOpcUaMonitor");
        resetReference();
    },
});

// ---- Modbus TCP and EtherNet/IP: a write, a one-shot read and a polling of the same address

// stores the four requests of a write, read and polling round trip on one address, and runs it
const roundTrip = (prefix, endpoint, item, timeout, first, second) => {
    storeConnection(prefix + "Conn", endpoint, endpoint.startsWith("logix:") ? "DcdLogixCIPEIP" : "DcdModbusTCP");
    const common = {deviceId: "VIRTUAL_PLC_01", routerEnumId: "DrrDirect", connectionName: prefix + "Conn", timeout};
    storeRequest({...common, requestName: prefix + "Write", requestTypeEnumId: "DrtWrite"});
    storeRequest({...common, requestName: prefix + "Read", requestTypeEnumId: "DrtRead"});
    storeRequest({...common, requestName: prefix + "Poll", requestTypeEnumId: "DrtRead", pollingInterval: 50});
    storeRequest({...common, requestName: prefix + "Stop", requestTypeEnumId: "DrtUnsubscribe", parentRequestName: prefix + "Poll"});
    for (const requestName of [prefix + "Write", prefix + "Read", prefix + "Poll"]) {
        storeItem(Object.assign({requestName,
            parameterId: requestName === prefix + "Write" ? "TestPlcReferenceParam" : "TestPlcFeedbackParam"}, item));
    }
    const write = (value) => {
        setReference(value);
        return run(prefix + "Write");
    };
    const written = write(first);
    const read = run(prefix + "Read");
    const readValue = parameterNumber("TestPlcFeedbackParam");
    const started = run(prefix + "Poll");
    write(second);
    const collected = waitFor(() => parameterNumber("TestPlcFeedbackParam") === second);
    run(prefix + "Stop");
    return {written, read, readValue, started, collected};
};

feature("Modbus TCP write, read and cyclic acquisition persist through the common services", {
    ignore: !System.getProperty("gateway_modbus_test"),
    when() {
        return roundTrip("GatewayModbus", System.getProperty("gateway_modbus_endpoint"),
            {requestItemName: "value", query: "holding-register:0:UINT", itemTypeEnumId: "DritUInt"}, 2000, 37, 38);
    },
    then(r) {
        r.written.status == "completed";
        r.read.status == "completed";
        r.readValue == 37;
        r.started.status == "active";
        r.collected == true;
    },
    cleanup() {
        deleteRequests("GatewayModbusStop", "GatewayModbusPoll", "GatewayModbusRead", "GatewayModbusWrite");
        deleteConnections("GatewayModbusConn");
        resetReference();
    },
});

feature("EtherNet/IP write, read and polling of a Logix tag persist through the common services", {
    ignore: !System.getProperty("gateway_eip_endpoint"),
    when() {
        return roundTrip("GatewayEip", System.getProperty("gateway_eip_endpoint"),
            {requestItemName: "speed", query: "Speed", itemTypeEnumId: "DritDInt"}, 3000, 41, 42);
    },
    then(r) {
        r.written.status == "completed";
        r.read.status == "completed";
        r.readValue == 41;
        r.started.status == "active";
        r.collected == true;
    },
    cleanup() {
        deleteRequests("GatewayEipStop", "GatewayEipPoll", "GatewayEipRead", "GatewayEipWrite");
        deleteConnections("GatewayEipConn");
        resetReference();
    },
});
