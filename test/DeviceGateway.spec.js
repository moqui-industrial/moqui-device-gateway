// moqui-device-gateway inside MR-Moqui, with moqui-math and moqui-device installed. From the framework root:
//     ./moq test --debug moqui-device-gateway
// The requests are those of moqui-device's DeviceTestData.xml. The MQTT features need an MQTT 5 broker on
// 127.0.0.1:1883 accepting user artemis/artemis, and gateway_mqtt_test=1 (see test/README.md).
spec({
    setup() { ec.artifactExecution.disableAuthz(); },
    cleanup() { ec.artifactExecution.enableAuthz(); },
});

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
