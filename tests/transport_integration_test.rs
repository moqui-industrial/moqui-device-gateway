use moqui_device_gateway::{
    config::AppConfig,
    db::DatabaseService,
    model::RequestContext,
    protocols::{FileTransferService, OpcUaService},
};

#[tokio::test]
#[ignore = "requires an SFTP server and SFTP_TEST_URI"]
async fn sftp_writes_and_overwrites_remote_binary_file() {
    let uri = std::env::var("SFTP_TEST_URI").expect("Set SFTP_TEST_URI with a verified host key");
    FileTransferService::transfer_bytes(&uri, "regression.bin", &[0, 255, 16])
        .await
        .unwrap();
    FileTransferService::transfer_bytes(&uri, "regression.bin", &[42, 43])
        .await
        .unwrap();
    assert!(
        FileTransferService::transfer_bytes(&uri, "../escape.bin", &[1])
            .await
            .is_err()
    );
    // An empty trust file must reject the host; there is no local fallback.
    let trust = std::env::temp_dir().join(format!("empty-known-hosts-{}", uuid::Uuid::new_v4()));
    std::fs::write(&trust, b"").unwrap();
    let join = if uri.contains('?') { '&' } else { '?' };
    let untrusted = format!("{uri}{join}knownHostsFile={}", trust.display());
    assert!(
        FileTransferService::transfer_bytes(&untrusted, "untrusted.bin", &[1])
            .await
            .is_err()
    );
    std::fs::remove_file(trust).unwrap();
}

#[tokio::test]
#[ignore = "requires seeded PostgreSQL and tests/support/opcua_server.py"]
async fn opcua_write_read_and_subscription_store_values() {
    use moqui_device_gateway::{pipeline::InboundErrorNotifier, supervisor::SubscriptionManager};
    let url =
        std::env::var("DATABASE_URL").expect("Set DATABASE_URL to a disposable seeded database");
    let db = DatabaseService::init(&url, &url).await.unwrap();
    let context: RequestContext = serde_json::from_value(serde_json::json!({
        "request_name":"opcua-protocol-regression", "device_id":"VIRTUAL_PLC_01", "request_type_enum_id":"DrtWrite",
        "purpose_enum_id":"DrpControl", "driver_enum_id":"DcdOpcUa", "timeout":5,
        "broker_uri":"opc.tcp://127.0.0.1:12686/moqui", "polling_interval":100,
        "items":[{"parameter_id":"TestPlcFeedbackParam", "numeric_value":52.5,"target_query":"ns=2;s=review_value"}]
    })).unwrap();
    let config = AppConfig::load();
    OpcUaService::run_opcua_write(&context, &config)
        .await
        .unwrap();
    let result = OpcUaService::run_read(&context, &db).await.unwrap();
    assert_eq!(result["rowCount"], 1);
    let read: rust_decimal::Decimal = sqlx::query_scalar(
        "SELECT NUMERIC_VALUE FROM PARAMETER WHERE PARAMETER_ID='TestPlcFeedbackParam'",
    )
    .fetch_one(&db.moqui_pool)
    .await
    .unwrap();
    assert_eq!(read.to_string().parse::<f64>().unwrap(), 52.5);
    let manager = SubscriptionManager::new(
        db.clone(),
        config,
        InboundErrorNotifier::new(false, 60, None),
    );
    let mut subscription = context.clone();
    subscription.request_type_enum_id = "DrtCyclic".into();
    manager.dispatch(&subscription).await.unwrap();
    let mut changed = context.clone();
    changed.items[0].numeric_value = Some(53.5);
    OpcUaService::run_opcua_write(&changed, manager.config())
        .await
        .unwrap();
    let mut received = false;
    for _ in 0..40 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let current: rust_decimal::Decimal = sqlx::query_scalar(
            "SELECT NUMERIC_VALUE FROM PARAMETER WHERE PARAMETER_ID='TestPlcFeedbackParam'",
        )
        .fetch_one(&db.moqui_pool)
        .await
        .unwrap();
        if current.to_string().parse::<f64>().unwrap() == 53.5 {
            received = true;
            break;
        }
    }
    let mut unsubscribe = context.clone();
    unsubscribe.request_type_enum_id = "DrtUnsubscribe".into();
    manager.unsubscribe(&unsubscribe).await.unwrap();
    assert_eq!(manager.active_route_count().await, 0);
    assert!(received, "OPC UA monitored update must reach the model");
}

#[tokio::test]
#[ignore = "requires seeded PostgreSQL"]
async fn modbus_tcp_read_write_and_single_cyclic_task() {
    use moqui_device_gateway::{
        pipeline::InboundErrorNotifier, protocols::ModbusService, supervisor::SubscriptionManager,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicU16, Ordering},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let register = Arc::new(AtomicU16::new(0));
    let register_server = register.clone();
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let register = register_server.clone();
            tokio::spawn(async move {
                loop {
                    let mut header = [0u8; 7];
                    if socket.read_exact(&mut header).await.is_err() {
                        break;
                    }
                    let length = u16::from_be_bytes([header[4], header[5]]) as usize;
                    let mut pdu = vec![0u8; length - 1];
                    socket.read_exact(&mut pdu).await.unwrap();
                    let response = if pdu[0] == 6 {
                        register.store(u16::from_be_bytes([pdu[3], pdu[4]]), Ordering::SeqCst);
                        pdu
                    } else {
                        let value = register.load(Ordering::SeqCst).to_be_bytes();
                        vec![3, 2, value[0], value[1]]
                    };
                    let size = ((response.len() + 1) as u16).to_be_bytes();
                    header[4] = size[0];
                    header[5] = size[1];
                    if socket.write_all(&header).await.is_err() {
                        break;
                    }
                    if socket.write_all(&response).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    let url = std::env::var("DATABASE_URL").expect("Set DATABASE_URL");
    let db = DatabaseService::init(&url, &url).await.unwrap();
    let original: rust_decimal::Decimal = sqlx::query_scalar(
        "SELECT NUMERIC_VALUE FROM PARAMETER WHERE PARAMETER_ID='TestPlcReferenceParam'",
    )
    .fetch_one(&db.moqui_pool)
    .await
    .unwrap();
    let mut context: RequestContext = serde_json::from_value(serde_json::json!({
        "request_name":"modbus-regression","device_id":"VIRTUAL_PLC_01","request_type_enum_id":"DrtWrite",
        "driver_enum_id":"DcdModbusTCP","transport_config":format!("localhost:{port}"),"purpose_enum_id":"DrpControl", "polling_interval":20,
        "items":[{"parameter_id":"TestPlcReferenceParam","numeric_value":42,"target_query":"holding:0"}]
    })).unwrap();
    assert!(context.is_modbus());
    ModbusService::run_write(&context).await.unwrap();
    assert_eq!(register.load(Ordering::SeqCst), 42);
    ModbusService::run_read(&context, &db).await.unwrap();
    let manager = SubscriptionManager::new(
        db.clone(),
        AppConfig::load(),
        InboundErrorNotifier::new(false, 60, None),
    );
    context.request_type_enum_id = "DrtCyclic".into();
    manager.dispatch(&context).await.unwrap();
    assert_eq!(manager.active_route_count().await, 1);
    context.request_type_enum_id = "DrtUnsubscribe".into();
    manager.unsubscribe(&context).await.unwrap();
    sqlx::query("UPDATE PARAMETER SET NUMERIC_VALUE=$1 WHERE PARAMETER_ID='TestPlcReferenceParam'")
        .bind(original)
        .execute(&db.moqui_pool)
        .await
        .unwrap();
    server.abort();
}
