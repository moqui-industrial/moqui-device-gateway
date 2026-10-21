// moqui-device-gateway inside MR-Moqui, with moqui-math and moqui-device installed. From the framework root:
//     ./moq test --debug moqui-device-gateway
// The requests are those of moqui-device's DeviceTestData.xml and HVAC demo data. Optional features:
//   gateway_mqtt_test=1: an MQTT 5 broker on 127.0.0.1:1883 with user artemis/artemis; the HVAC demo also needs
//     MQTT_USERNAME=artemis MQTT_PASSWORD=artemis (HVACDemoGatewayData.xml reads them);
//   device_gateway_error_notification_seconds=0 with gateway_mqtt_test: the persistence failure notification;
//   device_gateway_device_id=GW_EDGE_01: requests outside the gateway scope are refused;
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
const mqttUri = "paho-mqtt5:?brokerUrl=tcp://localhost:1883&userName=artemis&password=artemis";
const storeRequest = (fields) => call("store#moqui.device.DeviceRequest", Object.assign({timeout: 5000}, fields));
const deleteRequests = (...names) => names.forEach(requestName => {
    ec.entity.find("moqui.device.DeviceRequestItem").condition("requestName", requestName).deleteAll();
    ec.entity.find("moqui.device.DeviceRequest").condition("requestName", requestName).deleteAll();
});
// publish one JSON message on a topic, through an MQTT write request without items
const publish = (topic, payload) => {
    storeRequest({requestName: "GatewayTestPublish", deviceId: "VIRTUAL_PLC_01", requestTypeEnumId: "DrtWrite",
        routerEnumId: "DrrMQTTBroker", runServiceName: "moqui.device.gateway.MqttServices.run#DeviceRequest",
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

feature("a recipe export writes one file per rule priority", {
    when() {
        return call("moqui.device.DeviceConfigServices.export#DeviceConfig", {requestName: "VIRTUAL_PLC_01_ExportProcess"});
    },
    then(out) {
        out.status == "completed";
        out.files.length == 1;
        out.files[0].rowCount == 2;
        out.transferredFiles.length == 1;
        out.transferredFiles[0].endsWith("Test PLC Gateway Rule Set_p10.txt");
        out.secondaryResults.length == 1;
        out.secondaryResults[0].status == "completed";
    },
});

feature("running a recipe export request alone exports the rule set in its query", {
    when() {
        return call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "VIRTUAL_PLC_01_ExportProcess"});
    },
    then(out) {
        out.status == "completed";
        out.transferredFiles.length == 1;
    },
});

feature("a content transfer sends the DeviceContent of the request device", {
    when() {
        const location = "/tmp/moqui-gateway-transfer.bin";
        ec.resource.getLocationReference(location).putBytes(new Uint8Array([0, 255, 16]));
        call("store#moqui.device.DeviceContent", {deviceContentId: "GatewayTransferContent", deviceId: "VIRTUAL_PLC_01",
            contentTypeEnumId: "DCntImage", contentLocation: location});
        return call("moqui.device.DeviceServices.run#DeviceRequest",
            {requestName: "VIRTUAL_PLC_01_TransferContent", deviceContentId: "GatewayTransferContent"});
    },
    then(out) {
        out.status == "completed";
        out.transferredFiles[0].endsWith("moqui-gateway-transfer.bin");
        out.secondaryResults.length == 1;
        out.secondaryResults[0].requestName == "VIRTUAL_PLC_02_TransferContent";
        out.secondaryResults[0].status == "completed";
        out.secondaryResults[0].transferredFiles[0].includes("plc-redundant");
    },
    cleanup() {
        call("delete#moqui.device.DeviceContent", {deviceContentId: "GatewayTransferContent"});
        ec.resource.getLocationReference("/tmp/moqui-gateway-transfer.bin").delete();
    },
});

feature("a protocol service refuses a request whose endpoint is another protocol", {
    allowMessageErrors: true,
    when() {
        const request = ec.entity.find("moqui.device.DeviceRequest").condition("requestName", "TestPlcMqttPublishReq01").one();
        const device = ec.entity.find("moqui.device.Device").condition("deviceId", request.deviceId).one();
        const failure = failureOf(() => call("moqui.device.gateway.OpcUaServices.run#DeviceRequest", {request, device}));
        ec.message.clearErrors();
        return {failure};
    },
    then(out) {
        out.failure != null;
        out.failure.includes("OpcUaServices");
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
        const first = call("moqui.device.gateway.InboundServices.store#Samples", {requestName: "TestPlcOpcUaReadReq01", samples});
        const second = call("moqui.device.gateway.InboundServices.store#Samples", {requestName: "TestPlcOpcUaReadReq01", samples});
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
        return call("moqui.device.gateway.InboundServices.store#Samples", {requestName: "TestPlcOpcUaReadReq01",
            samples: [{deviceId: "DRV_EDGE_01", parameterId: "TestPlcFeedbackParam", value: 1}]});
    },
    then(out) {
        out.failedCount == 1;
        out.results[0].status == "failed";
    },
});

feature("no subscription runs before any start", {
    when() {
        return call("moqui.device.gateway.SubscriptionServices.get#SubscriptionState", {});
    },
    then(out) {
        out.activeRequestNames.length == 0;
        out.droppedRequestNames.length == 0;
        out.startupClaimed == false;
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
            runServiceName: "moqui.device.gateway.MqttServices.run#DeviceRequest",
            brokerUri: "paho-mqtt5:?brokerUrl=tcp://localhost:1883&userName=artemis&password=artemis",
            query: "moqui/test/trajectory", timeout: 5000});
        return call("moqui.device.TrajectoryPlannerServices.export#Trajectory",
            {approximatedFunctionId: planned.approximatedFunctionId, requestName: "GatewayTestTrajectory"});
    },
    then(out) {
        out.status == "completed";
    },
    cleanup() {
        call("delete#moqui.device.DeviceRequest", {requestName: "GatewayTestTrajectory"});
    },
});

feature("an MQTT subscription starts once and stops on unsubscribe", {
    ignore: !System.getProperty("gateway_mqtt_test"),
    when() {
        const started = call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "TestPlcMqttSubscribeReq01"});
        const state = call("moqui.device.gateway.SubscriptionServices.get#SubscriptionState", {});
        const stopped = call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "TestPlcMqttUnsubscribeReq01"});
        const after = call("moqui.device.gateway.SubscriptionServices.get#SubscriptionState", {});
        return {started, state, stopped, after};
    },
    then(out) {
        out.started.status == "active";
        out.state.activeRequestNames.includes("TestPlcMqttSubscribeReq01");
        out.stopped.status == "completed";
        !out.after.activeRequestNames.includes("TestPlcMqttSubscribeReq01");
        !out.after.droppedRequestNames.includes("TestPlcMqttSubscribeReq01");
    },
});

feature("the HVAC demo publishes the live parameters and stores the PLC log", {
    // HVAC_DEMO_PLC is in the scope of HVAC_DEMO_GATEWAY only
    ignore: !System.getProperty("gateway_mqtt_test")
        || !["", "HVAC_DEMO_GATEWAY"].includes(System.getProperty("device_gateway_device_id") || ""),
    when() {
        call("update#moqui.device.DeviceRequest", {requestName: "HVAC_DEMO_LiveParametersWrite", onlyChangedParameters: "N"});
        const write = call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "HVAC_DEMO_LiveParametersWrite"});
        const subscription = call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "HVAC_DEMO_PlcLogSubscribe"});
        // what the CODESYS ParameterLogger publishes, sent here by an MQTT write without items
        call("store#moqui.device.DeviceRequest", {requestName: "GatewayTestPlcLogPublish", deviceId: "HVAC_DEMO_PLC",
            requestTypeEnumId: "DrtWrite", routerEnumId: "DrrMQTTBroker",
            runServiceName: "moqui.device.gateway.MqttServices.run#DeviceRequest", connectionName: "HVAC_DEMO_MQTT",
            query: "moqui-plc", timeout: 5000});
        const count = () => ec.entity.find("moqui.math.ParameterLog").condition("parameterId", "HvacTempSetpoint").count();
        const before = count();
        const publish = call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "GatewayTestPlcLogPublish",
            payload: {loggerName: "HVAC_DEMO_PLC", source: "HvacTempSetpoint", type: 1, numericValue: 21.5,
                logEventDate: "2026-10-21T10:00:00Z"}});
        let after = before;
        const started = Date.now();
        while (after === before && Date.now() - started < 8000) after = count();
        call("store#moqui.device.DeviceRequest", {requestName: "GatewayTestPlcLogUnsubscribe", deviceId: "HVAC_DEMO_PLC",
            parentRequestName: "HVAC_DEMO_PlcLogSubscribe", requestTypeEnumId: "DrtUnsubscribe", routerEnumId: "DrrMQTTBroker",
            runServiceName: "moqui.device.gateway.PlcLogServices.run#DeviceRequest"});
        call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "GatewayTestPlcLogUnsubscribe"});
        return {write, subscription, publish, stored: after - before};
    },
    then(out) {
        out.write.status == "completed";
        out.write.itemResults.length == 20;
        out.subscription.status == "active";
        out.publish.status == "completed";
        out.stored == 1;
    },
    cleanup() {
        call("delete#moqui.device.DeviceRequest", {requestName: "GatewayTestPlcLogPublish"});
        call("delete#moqui.device.DeviceRequest", {requestName: "GatewayTestPlcLogUnsubscribe"});
        call("update#moqui.device.DeviceRequest", {requestName: "HVAC_DEMO_LiveParametersWrite", onlyChangedParameters: "Y"});
    },
});

feature("a recipe export writes the recipe text to the primary and the redundant PLC", {
    when() {
        storeRequest({requestName: "GatewayTestExport", deviceId: "VIRTUAL_PLC_01", requestTypeEnumId: "DrtConfigWrite",
            routerEnumId: "DrrDirect", runServiceName: "moqui.device.gateway.FileServices.run#DeviceRequest",
            brokerUri: `file:${specDir}/plc1`, query: "TestPlcGatewayRuleSet1"});
        storeRequest({requestName: "GatewayTestExportRedundant", parentRequestName: "GatewayTestExport", deviceId: "VIRTUAL_PLC_02",
            requestTypeEnumId: "DrtConfigWrite", routerEnumId: "DrrDirect",
            runServiceName: "moqui.device.gateway.FileServices.run#DeviceRequest", brokerUri: `file:${specDir}/plc2`});
        const out = call("moqui.device.DeviceConfigServices.export#DeviceConfig", {requestName: "GatewayTestExport"});
        return {out, primary: readText(`${specDir}/plc1/Test PLC Gateway Rule Set_p10.txt`),
            redundant: readText(`${specDir}/plc2/Test PLC Gateway Rule Set_p10.txt`)};
    },
    then(out) {
        out.out.status == "completed";
        out.out.secondaryResults[0].status == "completed";
        out.primary.includes("dev.PLC_RECIPE_REFERENCE:=250");
        out.primary.includes("dev.PLC_RECIPE_STATE:=AUTO");
        out.redundant == out.primary;
    },
    cleanup() {
        deleteRequests("GatewayTestExportRedundant", "GatewayTestExport");
        removeFile(`${specDir}/plc1/Test PLC Gateway Rule Set_p10.txt`);
        removeFile(`${specDir}/plc2/Test PLC Gateway Rule Set_p10.txt`);
    },
});

feature("a trajectory recipe is written as the former gateway wrote it", {
    when() {
        storeRequest({requestName: "GatewayTestTrajectoryExport", deviceId: "VIRTUAL_PLC_01", requestTypeEnumId: "DrtConfigWrite",
            routerEnumId: "DrrDirect", runServiceName: "moqui.device.gateway.FileServices.run#DeviceRequest",
            brokerUri: `file:${specDir}/trajectory`});
        const out = call("moqui.device.DeviceConfigServices.export#DeviceConfig",
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
        removeFile(`${specDir}/trajectory/TestPlcTrajectoryRuleSet_p01.txt`);
    },
});

feature("the gateway scope is its groups and their alive members", {
    when() {
        return {
            scope: call("moqui.device.gateway.SubscriptionServices.get#GatewayScope", {gatewayDeviceId: "GW_EDGE_01"}),
            notGateway: call("moqui.device.gateway.SubscriptionServices.get#GatewayScope", {gatewayDeviceId: "DRV_EDGE_01"}),
            missing: call("moqui.device.gateway.SubscriptionServices.get#GatewayScope", {gatewayDeviceId: "NO_SUCH_GATEWAY"}),
        };
    },
    then(out) {
        out.scope.ready == true;
        out.scope.scopedDeviceIds.includes("VIRTUAL_PLC_01");
        out.scope.scopedDeviceIds.includes("VIRTUAL_PLC_02");
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
        return call("moqui.device.gateway.SubscriptionServices.get#StartupSubscriptions", {gatewayDeviceId: "GW_EDGE_01"});
    },
    then(out) {
        out.ready == true;
        out.requestNames.length == 2;
        out.requestNames[0] == "TestPlcMqttSubscribeReq01";
        out.requestNames[1] == "TestPlcOpcUaReadReq01";
    },
});

feature("an inactive gateway membership leaves the gateway without a scope", {
    when() {
        setMemberStatus("DG_EDGE_01", "GW_EDGE_01", "DgmsDown");
        return call("moqui.device.gateway.SubscriptionServices.get#StartupSubscriptions", {gatewayDeviceId: "GW_EDGE_01"});
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
        return call("moqui.device.gateway.SubscriptionServices.get#StartupSubscriptions", {gatewayDeviceId: "GW_EDGE_01"});
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
        const failure = failureOf(() => call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "HVAC_DEMO_PLC_ExportProcess"}));
        ec.message.clearErrors();
        const inScope = call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "VIRTUAL_PLC_01_ExportProcess"});
        return {failure, inScope};
    },
    then(out) {
        out.failure.includes("not in the scope of gateway GW_EDGE_01");
        out.inScope.status == "completed";
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
            purposeEnumId: "DrpLogging", routerEnumId: "DrrMQTTBroker", runServiceName: "moqui.device.gateway.MqttServices.run#DeviceRequest",
            brokerUri: mqttUri, query: "moqui/test/burst"});
        call("store#moqui.device.DeviceRequestItem", {requestName: "GatewayTestBurst", requestItemName: "value",
            parameterId: "TestPlcFeedbackParam", sequenceNum: 10, query: "moqui/test/burst", itemTypeEnumId: "DritReal"});
        storeRequest({requestName: "GatewayTestBurstStop", parentRequestName: "GatewayTestBurst", deviceId: "VIRTUAL_PLC_01",
            requestTypeEnumId: "DrtUnsubscribe", routerEnumId: "DrrMQTTBroker", runServiceName: "moqui.device.gateway.MqttServices.run#DeviceRequest"});
        const count = () => ec.entity.find("moqui.math.ParameterLog").condition("parameterId", "TestPlcFeedbackParam").count();
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
            purposeEnumId: "DrpLogging", routerEnumId: "DrrMQTTBroker", runServiceName: "moqui.device.gateway.PlcLogServices.run#DeviceRequest",
            brokerUri: mqttUri, query: "moqui/test/plc-log"});
        storeRequest({requestName: "GatewayTestPlcLogStop", parentRequestName: "GatewayTestPlcLog", deviceId: "VIRTUAL_PLC_01",
            requestTypeEnumId: "DrtUnsubscribe", routerEnumId: "DrrMQTTBroker", runServiceName: "moqui.device.gateway.PlcLogServices.run#DeviceRequest"});
        const parameterLogs = () => ec.entity.find("moqui.math.ParameterLog").condition("parameterId", "TestPlcFeedbackParam").count();
        const deviceLogs = () => ec.entity.find("moqui.device.DeviceLog").condition("deviceId", "VIRTUAL_PLC_01").count();
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
            .condition("numericValue", 8.5).one();
        const textLog = ec.entity.find("moqui.math.ParameterLog").condition("parameterId", "TestPlcFeedbackParam")
            .condition("symbolicValue", "running").one();
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
            purposeEnumId: "DrpLogging", routerEnumId: "DrrMQTTBroker", runServiceName: "moqui.device.gateway.PlcLogServices.run#DeviceRequest",
            brokerUri: mqttUri, query: "moqui/test/failing"});
        storeRequest({requestName: "GatewayTestFailingStop", parentRequestName: "GatewayTestFailing", deviceId: "VIRTUAL_PLC_01",
            requestTypeEnumId: "DrtUnsubscribe", routerEnumId: "DrrMQTTBroker", runServiceName: "moqui.device.gateway.PlcLogServices.run#DeviceRequest"});
        const notices = () => ec.entity.find("moqui.security.user.NotificationMessage").condition("topic", "DeviceGatewayInbound").count();
        const n0 = notices();
        call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "GatewayTestFailing"});
        // an exact parameterId that does not exist: the store fails
        publish("moqui/test/failing", {loggerName: "VIRTUAL_PLC_01", source: "NoSuchParameter", type: 1, numericValue: 1,
            logEventDate: "2026-10-21T09:00:01Z"});
        const failing = waitFor(() => notices() >= n0 + 1);
        publish("moqui/test/failing", {loggerName: "VIRTUAL_PLC_01", source: "TestPlcFeedbackParam", type: 1, numericValue: 2,
            logEventDate: "2026-10-21T09:00:02Z"});
        const recovered = waitFor(() => notices() >= n0 + 2);
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
            options: `username=foo&password=pass&host-key-fingerprint=${System.getProperty("gateway_sftp_fingerprint")}`});
        storeRequest({requestName: "GatewayTestSftpExport", deviceId: "VIRTUAL_PLC_01", requestTypeEnumId: "DrtConfigWrite",
            routerEnumId: "DrrDirect", runServiceName: "moqui.device.gateway.FileServices.run#DeviceRequest",
            connectionName: "GatewayTestSftp", brokerUri: "sftp://127.0.0.1:2223/upload", query: "TestPlcGatewayRuleSet1", timeout: 15000});
        return call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "GatewayTestSftpExport"});
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

feature("the first start#Subscriptions restores the scoped subscriptions once", {
    // the startup claim is once per process: no other feature calls start#Subscriptions
    ignore: !System.getProperty("gateway_mqtt_test") || System.getProperty("device_gateway_device_id") !== "GW_EDGE_01",
    when() {
        const first = call("moqui.device.gateway.SubscriptionServices.start#Subscriptions", {});
        const state = call("moqui.device.gateway.SubscriptionServices.get#SubscriptionState", {});
        const second = call("moqui.device.gateway.SubscriptionServices.start#Subscriptions", {});
        call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: "TestPlcMqttUnsubscribeReq01"});
        return {first, state, second};
    },
    then(out) {
        out.first.startedRequestNames.includes("TestPlcMqttSubscribeReq01");
        // no OPC UA server in this run
        out.first.failedRequestNames.includes("TestPlcOpcUaReadReq01");
        out.state.activeRequestNames.includes("TestPlcMqttSubscribeReq01");
        out.second.startedRequestNames.length == 0;
        out.second.failedRequestNames.length == 0;
    },
});
