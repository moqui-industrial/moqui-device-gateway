use std::time::Duration;
use moqui_device_gateway::config::AppConfig;
use moqui_device_gateway::db::DatabaseService;
use moqui_device_gateway::model::RequestContext;
use moqui_device_gateway::pipeline::error_notifier::InboundErrorNotifier;
use moqui_device_gateway::protocols::mqtt::MqttService;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use tokio_util::sync::CancellationToken;

fn get_db_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://moqui:moqui@localhost:5432/moqui".to_string()
    })
}

fn get_mqtt_broker_uri() -> String {
    std::env::var("MQTT_BROKER_URI").unwrap_or_else(|_| {
        "paho-mqtt5:?brokerUrl=tcp://localhost:1883&userName=artemis&password=artemis".to_string()
    })
}

#[tokio::test]
async fn test_container_gateway_identity_and_scope() {
    let db_url = get_db_url();
    let db = match DatabaseService::init(&db_url, &db_url).await {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Skipping test: cannot connect to PostgreSQL container: {}", e);
            return;
        }
    };

    // 1. Verify GW_EDGE_01 identity (seeded in postgres-compose)
    let identity = db.inspect_gateway_identity("GW_EDGE_01").await.unwrap();
    assert!(identity.ready(), "Gateway GW_EDGE_01 must be ready: {}", identity.message);
    assert_eq!(identity.gateway_device_id, "GW_EDGE_01");
    assert!(identity.physical_device_exists);
    assert!(identity.gateway_member_found);

    // 2. Verify non-existent gateway identity
    let bad_identity = db.inspect_gateway_identity("NON_EXISTENT_GATEWAY").await.unwrap();
    assert!(!bad_identity.ready());

    // 3. Verify recursive gateway scope
    // VIRTUAL_PLC_01 and DRV_EDGE_01 are members of DG_EDGE_01 along with GW_EDGE_01
    let in_scope_plc = db.assert_gateway_scope("GW_EDGE_01", "VIRTUAL_PLC_01").await.unwrap();
    assert!(in_scope_plc, "VIRTUAL_PLC_01 must be in scope of GW_EDGE_01");

    let in_scope_drv = db.assert_gateway_scope("GW_EDGE_01", "DRV_EDGE_01").await.unwrap();
    assert!(in_scope_drv, "DRV_EDGE_01 must be in scope of GW_EDGE_01");

    let out_of_scope = db.assert_gateway_scope("GW_EDGE_01", "ROGUE_DEVICE_99").await.unwrap();
    assert!(!out_of_scope, "ROGUE_DEVICE_99 must NOT be in scope of GW_EDGE_01");
}

#[tokio::test]
async fn test_container_db_startup_discovery_and_request_context() {
    let db_url = get_db_url();
    let db = match DatabaseService::init(&db_url, &db_url).await {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Skipping test: cannot connect to PostgreSQL container: {}", e);
            return;
        }
    };

    // 1. Test startup subscriptions discovered from DB
    let startup_requests = db.load_startup_subscription_request_names("GW_EDGE_01").await.unwrap();
    assert!(
        startup_requests.contains(&"TestPlcMqttSubscribeReq01".to_string()),
        "Startup requests must contain TestPlcMqttSubscribeReq01, found: {:?}",
        startup_requests
    );

    // 2. Test full RequestContext loading
    let req_ctx = db.load_request_context("TestPlcMqttPublishReq01").await.unwrap();
    assert_eq!(req_ctx.request_name, "TestPlcMqttPublishReq01");
    assert_eq!(req_ctx.device_id.as_deref(), Some("VIRTUAL_PLC_01"));
    assert_eq!(req_ctx.request_type_enum_id, "DrtWrite");
    assert_eq!(req_ctx.purpose_enum_id.as_deref(), Some("DrpControl"));
    assert_eq!(req_ctx.router_enum_id.as_deref(), Some("DrrMoquiDeviceGateway"));

    // Check request items
    assert_eq!(req_ctx.items.len(), 2);
    let item1 = &req_ctx.items[0];
    assert_eq!(item1.parameter_id, "TestPlcReferenceParam");
    assert_eq!(item1.request_item_name.as_deref(), Some("virtualPlcReference"));
    assert!(item1.numeric_value.is_some());

    let item2 = &req_ctx.items[1];
    assert_eq!(item2.parameter_id, "TestPlcMainControlWordParam");
    assert_eq!(item2.symbolic_value.as_deref(), Some("START"));
}

#[tokio::test]
async fn test_container_mqtt_inbound_to_postgres_parameter_log() {
    let db_url = get_db_url();
    let db = match DatabaseService::init(&db_url, &db_url).await {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Skipping test: cannot connect to PostgreSQL container: {}", e);
            return;
        }
    };

    let test_topic = format!("test/container/parameter-log/in-{}", uuid::Uuid::new_v4());
    let broker_uri = get_mqtt_broker_uri();

    let dummy_context = RequestContext {
        request_name: "test-container-mqtt-read".to_string(),
        device_id: Some("VIRTUAL_PLC_01".to_string()),
        parent_request_name: None,
        request_type_enum_id: "DrtSubscribe".to_string(),
        purpose_enum_id: Some("DrpLogging".to_string()),
        router_enum_id: None,
        connection_name: None,
        broker_uri: Some(broker_uri.clone()),
        timeout: None,
        polling_interval: None,
        request_query: Some(test_topic.clone()),
        only_changed_parameters: None,
        driver_enum_id: None,
        driver_enum_code: None,
        transport_enum_id: None,
        transport_enum_code: None,
        transport_config: None,
        options: None,
        items: vec![],
    };

    let cancel_token = CancellationToken::new();
    let notifier = InboundErrorNotifier::new(false, 60, None);
    let config = AppConfig::load();

    // Start subscriber on ActiveMQ Artemis
    let sub_res = MqttService::subscribe_mqtt(
        "test-container-mqtt-route".to_string(),
        dummy_context,
        test_topic.clone(),
        db.clone(),
        notifier,
        config,
        cancel_token.clone(),
    )
    .await;

    if let Err(e) = sub_res {
        eprintln!("Skipping test: cannot connect to Artemis MQTT container: {}", e);
        return;
    }

    // Give subscriber a brief moment to connect
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Connect publisher to Artemis with unique client ID
    let pub_id = format!("test-pub-inbound-{}", uuid::Uuid::new_v4());
    let mut pub_opts = MqttOptions::new(pub_id, "127.0.0.1", 1883);
    pub_opts.set_credentials("artemis", "artemis");
    let (pub_client, mut pub_eventloop) = AsyncClient::new(pub_opts, 10);

    tokio::spawn(async move {
        while let Ok(_) = pub_eventloop.poll().await {}
    });

    let test_val = 1984.75;
    let payload = serde_json::json!({
        "parameterId": "TestCellHeartbeatParam",
        "numericValue": test_val
    });

    pub_client
        .publish(&test_topic, QoS::AtLeastOnce, false, serde_json::to_vec(&payload).unwrap())
        .await
        .expect("Publish to Artemis failed");

    // Poll Postgres until row appears (up to 5s)
    let mut found = false;
    for _ in 0..25 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM PARAMETER_LOG WHERE PARAMETER_ID = $1 AND NUMERIC_VALUE = $2"
        )
        .bind("TestCellHeartbeatParam")
        .bind(rust_decimal::Decimal::from_str_exact("1984.75").unwrap())
        .fetch_one(&db.log_pool)
        .await
        .unwrap_or(0);

        if count > 0 {
            found = true;
            break;
        }
    }

    // Cancel subscriber task
    cancel_token.cancel();

    // Clean up test row
    let _ = sqlx::query(
        "DELETE FROM PARAMETER_LOG WHERE PARAMETER_ID = $1 AND NUMERIC_VALUE = $2"
    )
    .bind("TestCellHeartbeatParam")
    .bind(rust_decimal::Decimal::from_str_exact("1984.75").unwrap())
    .execute(&db.log_pool)
    .await;

    assert!(found, "Expected to find PARAMETER_LOG row with value 1984.75 in PostgreSQL container");
}

#[tokio::test]
async fn test_container_plc_log_ingest_batch() {
    let db_url = get_db_url();
    let db = match DatabaseService::init(&db_url, &db_url).await {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Skipping test: cannot connect to PostgreSQL container: {}", e);
            return;
        }
    };

    let test_topic = format!("test/container/plc-log-{}", uuid::Uuid::new_v4());
    let broker_uri = get_mqtt_broker_uri();

    let dummy_context = RequestContext {
        request_name: "test-container-plc-log".to_string(),
        device_id: Some("VIRTUAL_PLC_01".to_string()),
        parent_request_name: None,
        request_type_enum_id: "DrtSubscribe".to_string(),
        purpose_enum_id: Some("DrpLogging".to_string()),
        router_enum_id: None,
        connection_name: None,
        broker_uri: Some(broker_uri.clone()),
        timeout: None,
        polling_interval: None,
        request_query: Some(test_topic.clone()),
        only_changed_parameters: None,
        driver_enum_id: None,
        driver_enum_code: None,
        transport_enum_id: None,
        transport_enum_code: None,
        transport_config: None,
        options: None,
        items: vec![],
    };

    let cancel_token = CancellationToken::new();
    let notifier = InboundErrorNotifier::new(false, 60, None);
    let config = AppConfig::load();

    // Start PLC log subscriber on ActiveMQ Artemis
    let sub_res = MqttService::subscribe_plc_log(
        "test-container-plc-route".to_string(),
        dummy_context,
        test_topic.clone(),
        db.clone(),
        notifier,
        config,
        cancel_token.clone(),
    )
    .await;

    if let Err(e) = sub_res {
        eprintln!("Skipping test: cannot connect to Artemis MQTT container: {}", e);
        return;
    }

    tokio::time::sleep(Duration::from_millis(600)).await;

    let pub_id = format!("test-pub-plc-{}", uuid::Uuid::new_v4());
    let mut pub_opts = MqttOptions::new(pub_id, "127.0.0.1", 1883);
    pub_opts.set_credentials("artemis", "artemis");
    let (pub_client, mut pub_eventloop) = AsyncClient::new(pub_opts, 10);

    tokio::spawn(async move {
        while let Ok(_) = pub_eventloop.poll().await {}
    });

    let unique_payload_msg = format!("Test emergency halt trigger - {}", uuid::Uuid::new_v4());
    let test_numeric = 771.5;

    let batch = serde_json::json!([
        {
            "loggerName": "VIRTUAL_PLC_01",
            "source": "TestCellHeartbeatParam",
            "type": 1,
            "numericValue": test_numeric
        },
        {
            "deviceId": "VIRTUAL_PLC_01",
            "payload": unique_payload_msg
        }
    ]);

    pub_client
        .publish(&test_topic, QoS::AtLeastOnce, false, serde_json::to_vec(&batch).unwrap())
        .await
        .expect("Publish to Artemis failed");

    let mut param_log_found = false;
    let mut dev_log_found = false;

    for _ in 0..25 {
        tokio::time::sleep(Duration::from_millis(200)).await;

        if !param_log_found {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM PARAMETER_LOG WHERE PARAMETER_ID = $1 AND NUMERIC_VALUE = $2"
            )
            .bind("TestCellHeartbeatParam")
            .bind(rust_decimal::Decimal::from_str_exact("771.5").unwrap())
            .fetch_one(&db.log_pool)
            .await
            .unwrap_or(0);
            if count > 0 {
                param_log_found = true;
            }
        }

        if !dev_log_found {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM DEVICE_LOG WHERE DEVICE_ID = $1 AND PAYLOAD LIKE $2"
            )
            .bind("VIRTUAL_PLC_01")
            .bind(format!("%{}%", unique_payload_msg))
            .fetch_one(&db.log_pool)
            .await
            .unwrap_or(0);
            if count > 0 {
                dev_log_found = true;
            }
        }

        if param_log_found && dev_log_found {
            break;
        }
    }

    cancel_token.cancel();

    // Cleanup
    let _ = sqlx::query(
        "DELETE FROM PARAMETER_LOG WHERE PARAMETER_ID = $1 AND NUMERIC_VALUE = $2"
    )
    .bind("TestCellHeartbeatParam")
    .bind(rust_decimal::Decimal::from_str_exact("771.5").unwrap())
    .execute(&db.log_pool)
    .await;

    let _ = sqlx::query(
        "DELETE FROM DEVICE_LOG WHERE DEVICE_ID = $1 AND PAYLOAD LIKE $2"
    )
    .bind("VIRTUAL_PLC_01")
    .bind(format!("%{}%", unique_payload_msg))
    .execute(&db.log_pool)
    .await;

    assert!(param_log_found, "Expected PARAMETER_LOG entry for source item");
    assert!(dev_log_found, "Expected DEVICE_LOG entry for device log item");
}

#[tokio::test]
async fn test_container_mqtt_outbound_write() {
    let db_url = get_db_url();
    let db = match DatabaseService::init(&db_url, &db_url).await {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Skipping test: cannot connect to PostgreSQL container: {}", e);
            return;
        }
    };

    let target_topic = "mqtt-write-device-request/virtual-plc/reference";

    // Setup an independent subscriber to verify what the gateway publishes
    let mut sub_opts = MqttOptions::new("test-listener-client", "127.0.0.1", 1883);
    sub_opts.set_credentials("artemis", "artemis");
    let (sub_client, mut sub_eventloop) = AsyncClient::new(sub_opts, 20);

    sub_client.subscribe(target_topic, QoS::AtLeastOnce).await.unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(5);

    tokio::spawn(async move {
        while let Ok(event) = sub_eventloop.poll().await {
            if let rumqttc::Event::Incoming(rumqttc::Packet::Publish(p)) = event {
                if let Ok(s) = std::str::from_utf8(&p.payload) {
                    let _ = tx.send(s.to_string()).await;
                }
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Load request from PostgreSQL and execute write through MqttService
    let req_ctx = db.load_request_context("TestPlcMqttPublishReq01").await.unwrap();
    let config = AppConfig::load();

    let write_res = MqttService::run_mqtt_write(&req_ctx, &config).await;
    assert!(write_res.is_ok(), "MQTT write failed: {:?}", write_res);

    // Verify subscriber received the message
    let received = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;
    assert!(received.is_ok(), "Timed out waiting for MQTT publish message");
    let payload = received.unwrap().expect("Channel closed");

    // The seed has 300.000000 for TestPlcReferenceParam
    assert!(
        payload.contains("300"),
        "Expected payload to contain '300', received: {}",
        payload
    );
}
