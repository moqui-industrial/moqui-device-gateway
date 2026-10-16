use moqui_device_gateway::config::AppConfig;
use moqui_device_gateway::db::ExportRow;
use moqui_device_gateway::export::RecipeExporter;
use moqui_device_gateway::model::{RequestContext, RequestItem};
use moqui_device_gateway::pipeline::Normalizer;
use moqui_device_gateway::protocols::OpcUaService;

#[test]
fn test_normalizer_scalar_numeric_and_boolean() {
    let context = RequestContext {
        request_name: "TestReq".to_string(),
        device_id: Some("PLC_01".to_string()),
        parent_request_name: None,
        request_type_enum_id: "DrtSubscribe".to_string(),
        purpose_enum_id: Some("DrpControl".to_string()),
        router_enum_id: None,
        connection_name: None,
        broker_uri: None,
        timeout: None,
        polling_interval: None,
        request_query: Some("iot/sensor/speed".to_string()),
        only_changed_parameters: None,
        driver_enum_id: None,
        driver_enum_code: None,
        transport_enum_id: None,
        transport_enum_code: None,
        transport_config: None,
        options: None,
        items: vec![RequestItem {
            parameter_id: "PARAM_SPEED".to_string(),
            request_item_name: Some("speed".to_string()),
            sequence_num: Some(1),
            numeric_value: None,
            symbolic_value: None,
            parameter_enum_id: None,
            target_query: Some("iot/sensor/speed".to_string()),
        }],
    };

    // Test numeric scalar
    let rows = Normalizer::normalize_inbound_payload(&context, "iot/sensor/speed", "1450.5");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].parameter_id, "PARAM_SPEED");
    assert_eq!(rows[0].numeric_value, Some(1450.5));
    assert_eq!(rows[0].purpose_enum_id.as_deref(), Some("DrpControl"));

    // Test boolean scalar
    let rows_bool = Normalizer::normalize_inbound_payload(&context, "iot/sensor/speed", "true");
    assert_eq!(rows_bool.len(), 1);
    assert_eq!(rows_bool[0].parameter_id, "PARAM_SPEED");
    assert_eq!(rows_bool[0].symbolic_value.as_deref(), Some("Y"));
}

#[test]
fn test_normalizer_json_object_and_array() {
    let context = RequestContext {
        request_name: "TestJsonReq".to_string(),
        device_id: Some("PLC_01".to_string()),
        parent_request_name: None,
        request_type_enum_id: "DrtSubscribe".to_string(),
        purpose_enum_id: Some("DrpControl".to_string()),
        router_enum_id: None,
        connection_name: None,
        broker_uri: None,
        timeout: None,
        polling_interval: None,
        request_query: None,
        only_changed_parameters: None,
        driver_enum_id: None,
        driver_enum_code: None,
        transport_enum_id: None,
        transport_enum_code: None,
        transport_config: None,
        options: None,
        items: vec![],
    };

    let json_array = r#"[
        {"parameterId": "P1", "numericValue": 10.0},
        {"parameterId": "P2", "symbolicValue": "ACTIVE", "parameterEnumId": "PstActive"}
    ]"#;

    let rows = Normalizer::normalize_inbound_payload(&context, "topic/any", json_array);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].parameter_id, "P1");
    assert_eq!(rows[0].numeric_value, Some(10.0));
    assert_eq!(rows[1].parameter_id, "P2");
    assert_eq!(rows[1].symbolic_value.as_deref(), Some("ACTIVE"));
    assert_eq!(rows[1].parameter_enum_id.as_deref(), Some("PstActive"));
}

#[test]
fn test_partition_plc_log_batch() {
    let sample_batch = r#"{
        "0": {
            "loggerName": "VIRTUAL_PLC_01",
            "logEventDate": "DT#2026-10-14-22:30:00",
            "source": "PARAM_FEEDBACK_01",
            "type": 1,
            "numericValue": 300.05
        },
        "1": {
            "loggerName": "VIRTUAL_PLC_01",
            "logEventDate": "DT#2026-10-14-22:30:01",
            "source": "",
            "sequenceNum": 42,
            "message": "Emergency stop button pressed"
        }
    }"#;

    let batch =
        Normalizer::partition_plc_log_batch(sample_batch).expect("Partitioning should succeed");

    // Entry 0 has source -> with_source
    assert_eq!(batch.with_source.len(), 1);
    assert_eq!(batch.with_source[0].parameter_id, "PARAM_FEEDBACK_01");
    assert_eq!(batch.with_source[0].numeric_value, Some(300.05));
    assert_eq!(
        batch.with_source[0].purpose_enum_id.as_deref(),
        Some("DrpLogging")
    );

    // Entry 1 has no source -> without_source (DeviceLog)
    assert_eq!(batch.without_source.len(), 1);
    assert_eq!(batch.without_source[0].device_id, "VIRTUAL_PLC_01");
    assert_eq!(batch.without_source[0].sequence_num, 42);
    assert!(batch.without_source[0].payload.contains("Emergency stop"));
}

#[test]
fn test_recipe_export_priority_grouping() {
    let rows = vec![
        ExportRow {
            rule_set_name: "RECIPE_A".to_string(),
            priority: 2,
            parameter_name: "Trajectory.pointCount".to_string(),
            parameter_value: "3".to_string(),
        },
        ExportRow {
            rule_set_name: "RECIPE_A".to_string(),
            priority: 1,
            parameter_name: "dev.SpeedSetpoint".to_string(),
            parameter_value: "1500".to_string(),
        },
        ExportRow {
            rule_set_name: "RECIPE_A".to_string(),
            priority: 1,
            parameter_name: "dev.TorqueLimit".to_string(),
            parameter_value: "180".to_string(),
        },
    ];

    let files = RecipeExporter::build_export_files(&rows);
    assert_eq!(files.len(), 2);

    // Priority 1
    assert_eq!(files[0].filename, "RECIPE_A_p01.txt");
    assert_eq!(files[0].priority, 1);
    assert_eq!(files[0].row_count, 2);
    assert_eq!(
        files[0].content,
        "dev.SpeedSetpoint:=1500\ndev.TorqueLimit:=180"
    );

    // Priority 2
    assert_eq!(files[1].filename, "RECIPE_A_p02.txt");
    assert_eq!(files[1].priority, 2);
    assert_eq!(files[1].row_count, 1);
    assert_eq!(files[1].content, "Trajectory.pointCount:=3");
}

#[test]
fn test_opcua_node_id_parsing() {
    let raw = "RAW(ns=2;s=Machine.Spindle.Speed)";
    let node_id = OpcUaService::parse_node_id(raw).expect("Should parse RAW node id");
    assert_eq!(node_id.namespace, 2);

    let numeric = "1005";
    let node_id_num = OpcUaService::parse_node_id(numeric).expect("Should parse numeric node id");
    assert_eq!(node_id_num.namespace, 0);

    let plain_string = "Line1.Conveyor.Running";
    let node_id_plain =
        OpcUaService::parse_node_id(plain_string).expect("Should parse plain string");
    assert_eq!(node_id_plain.namespace, 2);
}

#[test]
fn test_security_config_validator() {
    let mut config = AppConfig::load();
    config.api_auth_enabled = true;
    config.profile = "prod".to_string();
    config.api_auth_token = "change-me-in-production".to_string();

    assert!(config.validate_security().is_err());

    config.api_auth_token = "secure-production-random-secret-key-12345".to_string();
    assert!(config.validate_security().is_ok());

    // In dev profile, unsafe tokens are permitted
    config.profile = "local".to_string();
    config.api_auth_token = "change-me-in-production".to_string();
    assert!(config.validate_security().is_ok());
}

#[tokio::test]
async fn test_file_transfer_confines_filename_and_replaces_contents() {
    use moqui_device_gateway::protocols::FileTransferService;
    let dir = std::env::temp_dir().join(format!("moqui-transfer-test-{}", uuid::Uuid::new_v4()));
    for filename in ["../escape", "/absolute", "a/b", "a\\b", "", ".", ".."] {
        assert!(
            FileTransferService::transfer_bytes(dir.to_str().unwrap(), filename, b"x")
                .await
                .is_err()
        );
    }
    FileTransferService::transfer_bytes(dir.to_str().unwrap(), "recipe.bin", &[0, 255, 17])
        .await
        .unwrap();
    FileTransferService::transfer_bytes(dir.to_str().unwrap(), "recipe.bin", &[42])
        .await
        .unwrap();
    assert_eq!(std::fs::read(dir.join("recipe.bin")).unwrap(), vec![42]);
    assert!(
        FileTransferService::transfer_bytes("https://invalid.example", "recipe.bin", b"x")
            .await
            .is_err()
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn test_mqtt_v5_does_not_complete_without_broker_acknowledgement() {
    use moqui_device_gateway::protocols::MqttService;
    use tokio::{io::AsyncReadExt, net::TcpListener};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = [0u8; 1024];
        let n = socket.read(&mut bytes).await.unwrap();
        let at = bytes[..n].windows(4).position(|w| w == b"MQTT").unwrap();
        assert_eq!(bytes[at + 4], 5, "must use MQTT v5");
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    });
    let context: RequestContext = serde_json::from_value(serde_json::json!({
        "request_name":"ack-probe", "request_type_enum_id":"DrtWrite", "timeout":1,
        "broker_uri":format!("paho-mqtt5:?brokerUrl=tcp://127.0.0.1:{port}&password=must-not-leak"),
        "request_query":"test/ack-probe", "items":[{"parameter_id":"P1", "numeric_value":1.0}]
    }))
    .unwrap();
    let config = AppConfig::load();
    let error = MqttService::run_mqtt_write(&context, &config)
        .await
        .unwrap_err();
    assert!(error.contains("timed out"));
    assert!(!error.contains("must-not-leak"));
    peer.await.unwrap();
}
