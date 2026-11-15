// moqui-device-gateway inside MR-Moqui, with moqui-math and moqui-device installed. From the framework root:
//     ./moq test --debug moqui-device-gateway
// The requests are those of moqui-device's DeviceTestData.xml and HVAC demo data. Optional features:
//   gateway_mqtt_test=1: an MQTT 5 broker on 127.0.0.1:1883 (or gateway_mqtt_test_broker) with user artemis/artemis;
//     the HVAC demo also needs MQTT_USERNAME=artemis MQTT_PASSWORD=artemis (HVACDemoData.xml reads them);
//   device_gateway_error_notification_seconds=0 with gateway_mqtt_test: the persistence failure notification;
//   device_gateway_device_id=GW_EDGE_01: requests outside the gateway scope are refused;
//   gateway_opcua_test=1: the OPC UA server of `cargo run --example opcua_test_server` on 127.0.0.1:12686;
//   gateway_sftp_test=1: an SFTP server on 127.0.0.1:2223, user foo/pass, directory /upload, whose host key
//     fingerprint (SHA256:...) is in gateway_sftp_fingerprint.
// Files go to /tmp/moqui-gateway-spec.
spec({
    setup() { ec.artifactExecution.disableAuthz(); },
    cleanup() { ec.artifactExecution.enableAuthz(); },
});

const specDir = "/tmp/moqui-gateway-spec";
const readText = (path) => ec.resource.getLocationReference(path).getText();
const removeFile = (path) => { try { ec.resource.getLocationReference(path).delete(); } catch (e) { /* not there */ } };
const mqttBroker = System.getProperty("gateway_mqtt_test_broker") || "tcp://localhost:1883";
const mqttUri = `paho-mqtt5:?brokerUrl=${mqttBroker}&userName=artemis&password=artemis`;
const gatewayService = "moqui.device.gateway.DeviceGatewayServices.run#DeviceGatewayRequest";
const plcLogService = "moqui.device.gateway.DeviceGatewayServices.run#PlcLogRequest";
const storeRequest = (fields) => call("store#moqui.device.DeviceRequest", Object.assign({timeout: 5000}, fields));
const deleteRequests = (...names) => names.forEach(requestName => {
    ec.entity.find("moqui.device.DeviceRequestItem").condition("requestName", requestName).deleteAll();
    ec.entity.find("moqui.device.DeviceRequest").condition("requestName", requestName).deleteAll();
});
// publish one JSON message on a topic, through an MQTT write request without items
const publish = (topic, payload) => {
    storeRequest({requestName: "GatewayTestPublish", deviceId: "VIRTUAL_PLC_01", requestTypeEnumId: "DrtWrite",
        routerEnumId: "DrrMQTTBroker", runServiceName: gatewayService,
        brokerUri: mqttUri, query: topic});
    return call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "GatewayTestPublish", payload});
};
// wait for a condition the persistence worker makes true, at most a few seconds
const waitFor = (condition, millis = 8000) => {
    const started = Date.now();
    let value = condition();
    while (!value && Date.now() - started < millis) value = condition();
    return value;
};
// a group membership status, set directly: the status flow of DeviceGroupMember does not go back from Down to Alive
const setMemberStatus = (deviceId, memberDeviceId, statusId) => {
    const member = ec.entity.find("moqui.device.DeviceGroupMember").condition("deviceId", deviceId)
        .condition("memberDeviceId", memberDeviceId).useCache(false).one();
    member.statusId = statusId;
    member.update();
};
const failureOf = (action) => {
    try {
        action();
        return ec.message.hasError() ? ec.message.getErrorsString() : null;
    } catch (e) {
        return String(e.message || e);
    }
};

feature("a recipe export writes one file per rule priority to its connection destination", {
    when() {
        call("store#moqui.device.DeviceConnection", {connectionName: "GatewayTestExportConn", deviceId: "VIRTUAL_PLC_01",
            connectionTypeEnumId: "DctDirectConnection", purposeEnumId: "DcpMaintenance",
            transportConfig: `file:${specDir}/plc1`});
        storeRequest({requestName: "GatewayTestExport", deviceId: "VIRTUAL_PLC_01", requestTypeEnumId: "DrtConfigWrite",
            routerEnumId: "DrrDirect", connectionName: "GatewayTestExportConn"});
        const out = call("moqui.device.DeviceServices.export#DeviceConfig",
            {requestName: "GatewayTestExport", deviceRuleSetId: "TestPlcGatewayRuleSet1"});
        return {out, primary: readText(`${specDir}/plc1/Test PLC Gateway Rule Set_p10.txt`)};
    },
    then(out) {
        out.out.status == "completed";
        out.out.files.length == 1;
        out.out.files[0].rowCount == 2;
        out.out.transferredFiles.length == 1;
        out.out.transferredFiles[0].endsWith("Test PLC Gateway Rule Set_p10.txt");
        out.primary.includes("dev.PLC_RECIPE_REFERENCE:=250");
        out.primary.includes("dev.PLC_RECIPE_STATE:=AUTO");
    },
    cleanup() {
        deleteRequests("GatewayTestExport");
        call("delete#moqui.device.DeviceConnection", {connectionName: "GatewayTestExportConn"});
        removeFile(`${specDir}/plc1/Test PLC Gateway Rule Set_p10.txt`);
    },
});

feature("a content transfer sends the DeviceContent of the request device", {
    when() {
        const location = `${specDir}/moqui-gateway-transfer.bin`;
        ec.resource.getLocationReference(location).putBytes(new Uint8Array([0, 255, 16]));
        call("store#moqui.device.DeviceContent", {deviceContentId: "GatewayTransferContent", deviceId: "VIRTUAL_PLC_01",
            contentTypeEnumId: "DCntImage", contentLocation: location});
        return call("moqui.device.DeviceServices.transfer#DeviceContent",
            {requestName: "VIRTUAL_PLC_01_TransferContent", deviceContentId: "GatewayTransferContent"});
    },
    then(out) {
        out.status == "completed";
        out.transferredFiles.length == 1;
        out.transferredFiles[0].endsWith("moqui-gateway-transfer.bin");
    },
    cleanup() {
        call("delete#moqui.device.DeviceContent", {deviceContentId: "GatewayTransferContent"});
        removeFile(`${specDir}/moqui-gateway-transfer.bin`);
        removeFile("target/transferred-content/plc/moqui-gateway-transfer.bin");
    },
});

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
        const failure = failureOf(() => call("moqui.device.DeviceServices.run#DeviceRequest",
            {requestName: "TestPlcOpcUaWriteReq01", payload: {x: 1}}));
        ec.message.clearErrors();
        return {failure};
    },
    then(out) {
        out.failure != null;
        out.failure.includes("payload");
    },
});

feature("stored samples update the parameter once and skip an equal value", {
    when() {
        const samples = [{deviceId: "VIRTUAL_PLC_01", parameterId: "TestPlcFeedbackParam", value: 42.5}];
        const first = call("moqui.device.DeviceAcquisitionServices.store#Samples", {requestName: "TestPlcOpcUaReadReq01", samples});
        const second = call("moqui.device.DeviceAcquisitionServices.store#Samples", {requestName: "TestPlcOpcUaReadReq01", samples});
        const parameter = ec.entity.find("moqui.math.Parameter").condition("parameterId", "TestPlcFeedbackParam").one();
        return {first, second, value: parameter.numericValue.toNumber()};
    },
    then(out) {
        out.first.results[0].status == "stored";
        out.second.results[0].status == "unchanged";
        out.value == 42.5;
    },
});

feature("a sample of another device is not stored", {
    when() {
        return call("moqui.device.DeviceAcquisitionServices.store#Samples", {requestName: "TestPlcOpcUaReadReq01",
            samples: [{deviceId: "DRV_EDGE_01", parameterId: "TestPlcFeedbackParam", value: 1}]});
    },
    then(out) {
        out.failedCount == 1;
        out.results[0].status == "failed";
    },
});

feature("an MQTT write publishes the request items", {
    ignore: !System.getProperty("gateway_mqtt_test"),
    when() {
        return call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "TestPlcMqttPublishReq01"});
    },
    then(out) {
        out.status == "completed";
        out.itemResults.length == 2;
    },
});

feature("a trajectory export publishes one MQTT message", {
    ignore: !System.getProperty("gateway_mqtt_test"),
    when() {
        const planned = call("moqui.device.TrajectoryPlannerServices.run#RobotArmTrajectoryPlanner", {
            mathModelId: "TrjPlannerMlp6DofV1", plannerMode: "linear-demo",
            startPose: [0, 0, 0, 0, 0, 0], goalPose: [9, 18, 27, 0, 0, 0],
        });
        call("store#moqui.device.DeviceRequest", {requestName: "GatewayTestTrajectory", deviceId: "VIRTUAL_PLC_01",
            requestTypeEnumId: "DrtWrite", routerEnumId: "DrrMQTTBroker",
            runServiceName: gatewayService,
            brokerUri: mqttUri,
            query: "moqui/test/trajectory", timeout: 5000});
        return call("moqui.device.DeviceServices.export#Trajectory",
            {approximatedFunctionId: planned.approximatedFunctionId, requestName: "GatewayTestTrajectory"});
    },
    then(out) {
        out.status == "completed";
    },
    cleanup() {
        call("delete#moqui.device.DeviceRequest", {requestName: "GatewayTestTrajectory"});
    },
});

feature("an MQTT subscription starts once, answers unchanged while running, and stops on unsubscribe", {
    ignore: !System.getProperty("gateway_mqtt_test"),
    when() {
        const started = call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "TestPlcMqttSubscribeReq01"});
        const again = call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "TestPlcMqttSubscribeReq01"});
        const stopped = call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "TestPlcMqttUnsubscribeReq01"});
        return {started, again, stopped};
    },
    then(out) {
        out.started.status == "active";
        out.again.status == "unchanged";
        out.stopped.status == "completed";
    },
});

feature("the HVAC demo publishes the live parameters and stores the PLC log across the plant hierarchy", {
    // HVAC_DEMO_PLC is in the scope of HVAC_DEMO_GATEWAY only
    ignore: !System.getProperty("gateway_mqtt_test")
        || !["", "HVAC_DEMO_GATEWAY"].includes(System.getProperty("device_gateway_device_id") || ""),
    when() {
        call("update#moqui.device.DeviceRequest", {requestName: "HVAC_DEMO_LiveParametersWrite", onlyChangedParameters: "N"});
        const write = call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "HVAC_DEMO_LiveParametersWrite"});
        const subscription = call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "HVAC_DEMO_PlcLogSubscribe"});
        // what the CODESYS LogDispatcher publishes, sent here by an MQTT write without items
        call("store#moqui.device.DeviceRequest", {requestName: "GatewayTestPlcLogPublish", deviceId: "HVAC_DEMO_PLC",
            requestTypeEnumId: "DrtWrite", routerEnumId: "DrrMQTTBroker",
            runServiceName: gatewayService,
            brokerUri: mqttUri, query: "moqui-plc", timeout: 5000});
        const paramCount = () => ec.entity.find("moqui.math.ParameterLog").condition("parameterId", "HvacTempSetpoint").useCache(false).count();
        const pumpCount = () => ec.entity.find("moqui.device.DeviceLog").condition("deviceId", "HVAC_COLD_GLYCOL_PUMP").useCache(false).count();
        const beforeParam = paramCount();
        const beforePump = pumpCount();
        const publishParam = call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "GatewayTestPlcLogPublish",
            payload: {loggerName: "HVAC_DEMO_PLC", source: "HvacTempSetpoint", type: 1, numericValue: 21.5,
                sequence: 101, logEventDate: "2026-10-21T10:00:00Z"}});
        const publishPump = call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "GatewayTestPlcLogPublish",
            payload: {loggerName: "HVAC_COLD_GLYCOL_PUMP", source: "", type: 3, message: "Glycol pump started",
                sequence: 102, logEventDate: "2026-10-21T10:00:01Z"}});
        waitFor(() => paramCount() > beforeParam && pumpCount() > beforePump);
        const pumpLog = ec.entity.find("moqui.device.DeviceLog")
            .condition("deviceId", "HVAC_COLD_GLYCOL_PUMP")
            .condition("sequenceNum", 102)
            .useCache(false)
            .one();
        call("store#moqui.device.DeviceRequest", {requestName: "GatewayTestPlcLogUnsubscribe", deviceId: "HVAC_DEMO_PLC",
            parentRequestName: "HVAC_DEMO_PlcLogSubscribe", requestTypeEnumId: "DrtUnsubscribe", routerEnumId: "DrrMQTTBroker",
            runServiceName: plcLogService});
        call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "GatewayTestPlcLogUnsubscribe"});
        return {
            write,
            subscription,
            publishParam,
            publishPump,
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
        ec.entity.find("moqui.device.DeviceLog")
            .condition("deviceId", "HVAC_COLD_GLYCOL_PUMP")
            .condition("sequenceNum", 102)
            .deleteAll();
        ec.entity.find("moqui.math.ParameterLog")
            .condition("parameterId", "HvacTempSetpoint")
            .condition("sequenceNum", 101)
            .deleteAll();
        call("delete#moqui.device.DeviceRequest", {requestName: "GatewayTestPlcLogPublish"});
        call("delete#moqui.device.DeviceRequest", {requestName: "GatewayTestPlcLogUnsubscribe"});
        call("update#moqui.device.DeviceRequest", {requestName: "HVAC_DEMO_LiveParametersWrite", onlyChangedParameters: "Y"});
    },
});

feature("a trajectory recipe is written as a CODESYS txt recipe", {
    when() {
        call("store#moqui.device.DeviceConnection", {connectionName: "GatewayTestTrajectoryConn", deviceId: "VIRTUAL_PLC_01",
            connectionTypeEnumId: "DctDirectConnection", purposeEnumId: "DcpMaintenance",
            transportConfig: `file:${specDir}/trajectory`});
        storeRequest({requestName: "GatewayTestTrajectoryExport", deviceId: "VIRTUAL_PLC_01", requestTypeEnumId: "DrtConfigWrite",
            routerEnumId: "DrrDirect", connectionName: "GatewayTestTrajectoryConn"});
        const out = call("moqui.device.DeviceServices.export#DeviceConfig",
            {requestName: "GatewayTestTrajectoryExport", deviceRuleSetId: "TestPlcTrajectoryRuleSet1"});
        return {out, recipe: readText(`${specDir}/trajectory/TestPlcTrajectoryRuleSet_p01.txt`)};
    },
    then(out) {
        out.out.status == "completed";
        out.out.files[0].name == "TestPlcTrajectoryRuleSet_p01.txt";
        out.recipe.includes("Trajectory.trajectoryId:=TestPlcTrajectory01");
        out.recipe.includes("Trajectory.pointCount:=2");
        out.recipe.includes("Trajectory.points[0].isBreakPoint:=FALSE");
        out.recipe.includes("Trajectory.points[1].isBreakPoint:=TRUE");
        out.recipe.includes("Trajectory.points[0].sequenceNum:=1");
        out.recipe.includes("Trajectory.points[0].pos.v[0]:=");
        out.recipe.includes("Trajectory.points[0].blendingMode:=10");
        out.recipe.includes("Trajectory.points[1].blendingMode:=0");
    },
    cleanup() {
        deleteRequests("GatewayTestTrajectoryExport");
        call("delete#moqui.device.DeviceConnection", {connectionName: "GatewayTestTrajectoryConn"});
        removeFile(`${specDir}/trajectory/TestPlcTrajectoryRuleSet_p01.txt`);
    },
});

feature("the gateway scope is its groups and their alive members", {
    when() {
        return {
            scope: call("moqui.device.DeviceAcquisitionServices.get#GatewayScope", {gatewayDeviceId: "GW_EDGE_01"}),
            notGateway: call("moqui.device.DeviceAcquisitionServices.get#GatewayScope", {gatewayDeviceId: "DRV_EDGE_01"}),
            missing: call("moqui.device.DeviceAcquisitionServices.get#GatewayScope", {gatewayDeviceId: "NO_SUCH_GATEWAY"}),
        };
    },
    then(out) {
        out.scope.ready == true;
        out.scope.scopedDeviceIds.includes("VIRTUAL_PLC_01");
        out.scope.scopedDeviceIds.includes("DRV_EDGE_01");
        !out.scope.scopedDeviceIds.includes("HVAC_DEMO_PLC");
        out.scope.controllerDeviceIds.includes("VIRTUAL_PLC_01");
        !out.scope.controllerDeviceIds.includes("DRV_EDGE_01");
        out.notGateway.ready == false;
        out.notGateway.message.includes("DgmpEdgeGateway");
        out.missing.ready == false;
        out.missing.message.includes("PhysicalDevice");
    },
});

feature("the startup subscriptions are those of the controllers in the gateway scope, in order", {
    when() {
        return call("moqui.device.DeviceAcquisitionServices.get#StartupSubscriptions", {gatewayDeviceId: "GW_EDGE_01"});
    },
    then(out) {
        out.ready == true;
        out.requestNames.includes("TestPlcMqttSubscribeReq01");
        out.requestNames.includes("TestPlcOpcUaReadReq01");
        out.requestNames.indexOf("TestPlcMqttSubscribeReq01") < out.requestNames.indexOf("TestPlcOpcUaReadReq01");
    },
});

feature("an inactive gateway membership leaves the gateway without a scope", {
    when() {
        setMemberStatus("DG_EDGE_01", "GW_EDGE_01", "DgmsDown");
        return call("moqui.device.DeviceAcquisitionServices.get#StartupSubscriptions", {gatewayDeviceId: "GW_EDGE_01"});
    },
    then(out) {
        out.ready == false;
        out.requestNames.length == 0;
    },
    cleanup() {
        setMemberStatus("DG_EDGE_01", "GW_EDGE_01", "DgmsAlive");
    },
});

feature("an inactive controller membership excludes its subscriptions", {
    when() {
        setMemberStatus("DG_EDGE_01", "VIRTUAL_PLC_01", "DgmsDown");
        return call("moqui.device.DeviceAcquisitionServices.get#StartupSubscriptions", {gatewayDeviceId: "GW_EDGE_01"});
    },
    then(out) {
        out.ready == true;
        out.requestNames.length == 0;
    },
    cleanup() {
        setMemberStatus("DG_EDGE_01", "VIRTUAL_PLC_01", "DgmsAlive");
    },
});

feature("with a configured gateway a request outside its scope is refused", {
    ignore: System.getProperty("device_gateway_device_id") !== "GW_EDGE_01",
    allowMessageErrors: true,
    when() {
        const failure = failureOf(() => call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "HVAC_DEMO_PlcLogSubscribe"}));
        ec.message.clearErrors();
        return {failure};
    },
    then(out) {
        out.failure != null;
        out.failure.includes("not in the scope of gateway GW_EDGE_01");
    },
});

feature("an MQTT subscription stores the values it receives", {
    ignore: !System.getProperty("gateway_mqtt_test"),
    when() {
        const started = call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "TestPlcMqttSubscribeReq01"});
        const published = publish("mqtt-subscribe-device-request/virtual-plc/feedback", {virtualPlcFeedback: 42.25});
        const value = () => ec.entity.find("moqui.math.Parameter").condition("parameterId", "TestPlcFeedbackParam").useCache(false).one().numericValue.toNumber();
        const stored = waitFor(() => value() === 42.25);
        call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "TestPlcMqttUnsubscribeReq01"});
        return {started, published, stored, value: value()};
    },
    then(out) {
        out.started.status == "active";
        out.published.status == "completed";
        out.stored == true;
        out.value == 42.25;
    },
    cleanup() { deleteRequests("GatewayTestPublish"); },
});

feature("a burst of ten MQTT messages is logged ten times", {
    ignore: !System.getProperty("gateway_mqtt_test"),
    when() {
        storeRequest({requestName: "GatewayTestBurst", deviceId: "VIRTUAL_PLC_01", requestTypeEnumId: "DrtSubscribe",
            purposeEnumId: "DrpLogging", routerEnumId: "DrrMQTTBroker", runServiceName: gatewayService,
            brokerUri: mqttUri, query: "moqui/test/burst"});
        call("store#moqui.device.DeviceRequestItem", {requestName: "GatewayTestBurst", requestItemName: "value",
            parameterId: "TestPlcFeedbackParam", sequenceNum: 10, query: "moqui/test/burst", itemTypeEnumId: "DritReal"});
        storeRequest({requestName: "GatewayTestBurstStop", parentRequestName: "GatewayTestBurst", deviceId: "VIRTUAL_PLC_01",
            requestTypeEnumId: "DrtUnsubscribe", routerEnumId: "DrrMQTTBroker", runServiceName: gatewayService});
        const count = () => ec.entity.find("moqui.math.ParameterLog").condition("parameterId", "TestPlcFeedbackParam").useCache(false).count();
        const before = count();
        const started = call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "GatewayTestBurst"});
        for (let i = 1; i <= 10; i++) publish("moqui/test/burst", {value: 100 + i});
        waitFor(() => count() >= before + 10);
        const logged = count() - before;
        call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "GatewayTestBurstStop"});
        return {started, logged};
    },
    then(out) {
        out.started.status == "active";
        out.logged == 10;
    },
    cleanup() { deleteRequests("GatewayTestBurstStop", "GatewayTestBurst", "GatewayTestPublish"); },
});

feature("PLC log events go to ParameterLog or DeviceLog, and a malformed batch does not stop the subscription", {
    ignore: !System.getProperty("gateway_mqtt_test"),
    when() {
        storeRequest({requestName: "GatewayTestPlcLog", deviceId: "VIRTUAL_PLC_01", requestTypeEnumId: "DrtSubscribe",
            purposeEnumId: "DrpLogging", routerEnumId: "DrrMQTTBroker", runServiceName: plcLogService,
            brokerUri: mqttUri, query: "moqui/test/plc-log"});
        storeRequest({requestName: "GatewayTestPlcLogStop", parentRequestName: "GatewayTestPlcLog", deviceId: "VIRTUAL_PLC_01",
            requestTypeEnumId: "DrtUnsubscribe", routerEnumId: "DrrMQTTBroker", runServiceName: plcLogService});
        const parameterLogs = () => ec.entity.find("moqui.math.ParameterLog").condition("parameterId", "TestPlcFeedbackParam").useCache(false).count();
        const deviceLogs = () => ec.entity.find("moqui.device.DeviceLog").condition("deviceId", "VIRTUAL_PLC_01").useCache(false).count();
        const started = call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "GatewayTestPlcLog"});
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
        call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "GatewayTestPlcLogStop"});
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
    ignore: !System.getProperty("gateway_mqtt_test") || System.getProperty("device_gateway_error_notification_seconds") !== "0",
    when() {
        storeRequest({requestName: "GatewayTestFailing", deviceId: "VIRTUAL_PLC_01", requestTypeEnumId: "DrtSubscribe",
            purposeEnumId: "DrpLogging", routerEnumId: "DrrMQTTBroker", runServiceName: plcLogService,
            brokerUri: mqttUri, query: "moqui/test/failing"});
        storeRequest({requestName: "GatewayTestFailingStop", parentRequestName: "GatewayTestFailing", deviceId: "VIRTUAL_PLC_01",
            requestTypeEnumId: "DrtUnsubscribe", routerEnumId: "DrrMQTTBroker", runServiceName: plcLogService});
        const failedNotices = () => ec.entity.find("moqui.security.user.NotificationMessage").condition("topic", "DeviceInboundFailed").useCache(false).count();
        const recoveredNotices = () => ec.entity.find("moqui.security.user.NotificationMessage").condition("topic", "DeviceInboundRecovered").useCache(false).count();
        const f0 = failedNotices(), r0 = recoveredNotices();
        call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "GatewayTestFailing"});
        // an exact parameterId that does not exist: the store fails
        publish("moqui/test/failing", {loggerName: "VIRTUAL_PLC_01", source: "NoSuchParameter", type: 1, numericValue: 1,
            logEventDate: "2026-10-21T09:00:01Z"});
        const failing = waitFor(() => failedNotices() >= f0 + 1);
        publish("moqui/test/failing", {loggerName: "VIRTUAL_PLC_01", source: "TestPlcFeedbackParam", type: 1, numericValue: 2,
            logEventDate: "2026-10-21T09:00:02Z"});
        const recovered = waitFor(() => recoveredNotices() >= r0 + 1);
        call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "GatewayTestFailingStop"});
        return {failing, recovered};
    },
    then(out) {
        out.failing == true;
        out.recovered == true;
    },
    cleanup() { deleteRequests("GatewayTestFailingStop", "GatewayTestFailing", "GatewayTestPublish"); },
});

feature("a recipe export reaches a CODESYS PLC over SFTP", {
    ignore: !System.getProperty("gateway_sftp_test"),
    when() {
        call("store#moqui.device.DeviceConnection", {connectionName: "GatewayTestSftp", deviceId: "VIRTUAL_PLC_01",
            connectionTypeEnumId: "DctDirectConnection", purposeEnumId: "DcpMaintenance", driverEnumId: "DcdSftp",
            transportConfig: "sftp://127.0.0.1:2223/upload",
            options: `username=foo&password=pass&host-key-fingerprint=${System.getProperty("gateway_sftp_fingerprint")}`});
        storeRequest({requestName: "GatewayTestSftpExport", deviceId: "VIRTUAL_PLC_01", requestTypeEnumId: "DrtConfigWrite",
            routerEnumId: "DrrDirect", connectionName: "GatewayTestSftp", timeout: 15000});
        return call("moqui.device.DeviceServices.export#DeviceConfig",
            {requestName: "GatewayTestSftpExport", deviceRuleSetId: "TestPlcGatewayRuleSet1"});
    },
    then(out) {
        out.status == "completed";
        out.transferredFiles[0] == "/upload/Test PLC Gateway Rule Set_p10.txt";
    },
    cleanup() {
        deleteRequests("GatewayTestSftpExport");
        call("delete#moqui.device.DeviceConnection", {connectionName: "GatewayTestSftp"});
    },
});

feature("start#Subscriptions restores the scoped subscriptions and respects an explicit unsubscribe", {
    ignore: !System.getProperty("gateway_mqtt_test") || System.getProperty("device_gateway_device_id") !== "GW_EDGE_01",
    when() {
        const first = call("moqui.device.DeviceAcquisitionServices.start#Subscriptions", {});
        const second = call("moqui.device.DeviceAcquisitionServices.start#Subscriptions", {});
        call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "TestPlcMqttUnsubscribeReq01"});
        const afterStop = call("moqui.device.DeviceAcquisitionServices.start#Subscriptions", {});
        if (first.startedRequestNames.includes("TestPlcOpcUaReadReq01")) {
            call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "TestPlcOpcUaUnsubscribeReq01"});
        }
        return {first, second, afterStop, opcua: !!System.getProperty("gateway_opcua_test")};
    },
    then(out) {
        out.first.startedRequestNames.includes("TestPlcMqttSubscribeReq01");
        // the OPC UA subscription starts only with the OPC UA test server
        (out.opcua ? out.first.startedRequestNames : out.first.failedRequestNames).includes("TestPlcOpcUaReadReq01");
        out.second.startedRequestNames.length == 0;
        out.afterStop.stoppedRequestNames.includes("TestPlcMqttSubscribeReq01");
        !out.afterStop.startedRequestNames.includes("TestPlcMqttSubscribeReq01");
    },
});

const parameterNumber = (parameterId) => ec.entity.find("moqui.math.Parameter").condition("parameterId", parameterId)
    .useCache(false).one().numericValue?.toNumber();
const writeReference = (value) => {
    call("update#moqui.math.Parameter", {parameterId: "TestPlcReferenceParam", numericValue: value});
    return call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "TestPlcOpcUaWriteReq01"});
};

feature("OPC UA write and one-shot read go through the node into the parameters", {
    ignore: !System.getProperty("gateway_opcua_test"),
    when() {
        const write = writeReference(123.5);
        const read = call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "TestPlcOpcUaOneshotReq01"});
        return {write, read, feedback: parameterNumber("TestPlcFeedbackParam")};
    },
    then(out) {
        out.write.status == "completed";
        out.read.status == "completed" || out.read.status == "unchanged";
        out.feedback == 123.5;
    },
    cleanup() { call("update#moqui.math.Parameter", {parameterId: "TestPlcReferenceParam", numericValue: 300}); },
});

feature("OPC UA cyclic subscription stores the node changes until the unsubscribe", {
    ignore: !System.getProperty("gateway_opcua_test"),
    when() {
        const started = call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "TestPlcOpcUaReadReq01"});
        writeReference(124.5);
        const stored = waitFor(() => parameterNumber("TestPlcFeedbackParam") === 124.5);
        const fault = ec.entity.find("moqui.math.Parameter").condition("parameterId", "TestPlcFaultParam").useCache(false).one();
        const stopped = call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "TestPlcOpcUaUnsubscribeReq01"});
        return {started, stored, fault, stopped};
    },
    then(out) {
        out.started.status == "active";
        out.stored == true;
        out.stopped.status == "completed";
    },
    cleanup() { call("update#moqui.math.Parameter", {parameterId: "TestPlcReferenceParam", numericValue: 300}); },
});

feature("an OPC UA monitored item stores the live node value", {
    ignore: !System.getProperty("gateway_opcua_test"),
    when() {
        storeRequest({requestName: "GatewayTestOpcUaMonitor", deviceId: "VIRTUAL_PLC_01", requestTypeEnumId: "DrtStateChange",
            routerEnumId: "DrrDirect", runServiceName: gatewayService,
            connectionName: "TestPlcOpcUaConn01"});
        call("store#moqui.device.DeviceRequestItem", {requestName: "GatewayTestOpcUaMonitor", requestItemName: "virtualPlcFeedback",
            parameterId: "TestPlcFeedbackParam", sequenceNum: 10, query: "ns=2;s=Demo.Dynamic.Scalar.Double", itemTypeEnumId: "DritLReal"});
        storeRequest({requestName: "GatewayTestOpcUaMonitorStop", parentRequestName: "GatewayTestOpcUaMonitor", deviceId: "VIRTUAL_PLC_01",
            requestTypeEnumId: "DrtUnsubscribe", routerEnumId: "DrrDirect", runServiceName: gatewayService,
            connectionName: "TestPlcOpcUaConn01"});
        const started = call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "GatewayTestOpcUaMonitor"});
        writeReference(125.5);
        const stored = waitFor(() => parameterNumber("TestPlcFeedbackParam") === 125.5);
        call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "GatewayTestOpcUaMonitorStop"});
        return {started, stored};
    },
    then(out) {
        out.started.status == "active";
        out.stored == true;
    },
    cleanup() {
        deleteRequests("GatewayTestOpcUaMonitorStop", "GatewayTestOpcUaMonitor");
        call("update#moqui.math.Parameter", {parameterId: "TestPlcReferenceParam", numericValue: 300});
    },
});
