use crate::config::AppConfig;
use crate::db::DatabaseService;
use crate::model::{RequestContext, RequestItem};
use crate::pipeline::{InboundErrorNotifier, Normalizer};
use rumqttc::v5::mqttbytes::{QoS, v5::Packet};
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
use rumqttc::{Outgoing, Transport};
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

pub struct MqttService;

impl MqttService {
    /// Executes a direct MQTT write request (DrtWrite / DrtConfigWrite)
    pub async fn run_mqtt_write(
        context: &RequestContext,
        config: &AppConfig,
    ) -> Result<serde_json::Value, String> {
        let broker_uri = context.broker_uri.as_deref().ok_or_else(|| {
            format!(
                "DeviceRequest {} does not define brokerUri.",
                context.request_name
            )
        })?;

        let endpoint = parse_mqtt_uri(
            broker_uri,
            &format!("moqui-gw-write-{}", uuid::Uuid::new_v4()),
            config,
        )?;
        let mut messages = Vec::new();
        for item in &context.items {
            let topic = item
                .target_query
                .as_deref()
                .or(context.request_query.as_deref())
                .or(endpoint.topic.as_deref())
                .filter(|t| !t.is_empty())
                .ok_or_else(|| {
                    format!("Missing MQTT topic for request {}", context.request_name)
                })?;
            let payload =
                serde_json::to_vec(&build_mqtt_payload(item)?).map_err(|e| e.to_string())?;
            messages.push((topic.to_string(), payload));
        }
        let publish_uri_list = Self::publish_messages(endpoint, messages, context.timeout).await?;

        if config.mqtt_write_after_publish_enabled
            && let Some(uri) = &config.mqtt_write_after_publish_uri
        {
            send_callback(
                uri,
                "writeCompleted",
                &context.request_name,
                context.device_id.as_deref(),
                "completed",
                "MQTT",
                "mqtt-write-device-request",
                context.items.len(),
                &publish_uri_list,
            )
            .await;
        }

        Ok(json!({
            "status": "completed",
            "routeId": "mqtt-write-device-request",
            "rowCount": context.items.len(),
            "publishUriList": publish_uri_list
        }))
    }

    /// Spawns an async MQTT subscription task
    pub async fn subscribe_mqtt(
        route_id: String,
        context: RequestContext,
        topic: String,
        db: DatabaseService,
        notifier: InboundErrorNotifier,
        config: AppConfig,
        cancel_token: CancellationToken,
    ) -> Result<(), String> {
        let broker_uri = context.broker_uri.as_deref().ok_or_else(|| {
            format!(
                "DeviceRequest {} does not define brokerUri.",
                context.request_name
            )
        })?;

        let client_id = format!(
            "moqui-gw-{}-{}-{}",
            safe_part(&config.gateway_device_id),
            safe_part(&context.request_name),
            safe_part(&topic)
        );

        let endpoint = parse_mqtt_uri(broker_uri, &client_id, &config)?;
        let mut options = endpoint.options;
        options.set_keep_alive(Duration::from_secs(30));

        let (client, mut eventloop) = AsyncClient::new(options, 50);

        client
            .subscribe(&topic, endpoint.qos)
            .await
            .map_err(|e| format!("Failed to subscribe to MQTT topic {}: {}", topic, e))?;

        info!(
            "MQTT subscriber started for route {} on topic {}",
            route_id, topic
        );

        let route_clone = route_id.clone();
        let context_clone = context.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!("MQTT subscriber cancelled for route {}", route_clone);
                        let _ = client.unsubscribe(&topic).await;
                        let _ = tokio::time::timeout(Duration::from_secs(2), async {
                            while let Ok(event) = eventloop.poll().await {
                                if matches!(event, Event::Incoming(Packet::UnsubAck(_))) { break; }
                            }
                        }).await;
                        let _ = client.disconnect().await;
                        let _ = tokio::time::timeout(Duration::from_millis(200), eventloop.poll()).await;
                        break;
                    }
                    notification = eventloop.poll() => {
                        match notification {
                            Ok(Event::Incoming(Packet::Publish(publish))) => {
                                let incoming_topic = String::from_utf8_lossy(&publish.topic).into_owned();
                                let payload_str = match std::str::from_utf8(&publish.payload) {
                                    Ok(s) => s,
                                    Err(e) => {
                                        warn!("Non-UTF8 MQTT payload on topic {}: {}", incoming_topic, e);
                                        continue;
                                    }
                                };

                                let rows = Normalizer::normalize_inbound_payload(&context_clone, &incoming_topic, payload_str);
                                if rows.is_empty() {
                                    continue;
                                }

                                let is_parameter_upsert = context_clone.purpose_enum_id.is_some()
                                    && context_clone.purpose_enum_id.as_deref() != Some("DrpLogging");
                                let save_result = if is_parameter_upsert {
                                    db.upsert_parameter_batch(&rows).await
                                } else {
                                    db.insert_parameter_log_batch(&rows).await
                                };

                                match save_result {
                                    Ok(_) => {
                                        notifier.clear_error(&route_clone).await;
                                        if config.mqtt_read_after_store_enabled
                                            && let Some(uri) = &config.mqtt_read_after_store_uri {
                                                send_callback(
                                                    uri,
                                                    "readStored",
                                                    &context_clone.request_name,
                                                    context_clone.device_id.as_deref(),
                                                    "completed",
                                                    "MQTT",
                                                    &route_clone,
                                                    rows.len(),
                                                    std::slice::from_ref(&incoming_topic),
                                                ).await;
                                            }
                                    }
                                    Err(e) => {
                                        warn!("DB persistence failed for MQTT route {}: {}", route_clone, e);
                                        notifier.record_error(&route_clone, Some(&context_clone.request_name), "MQTT", &e.to_string()).await;
                                    }
                                }
                            }
                            Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                                if !ack.session_present {
                                    let _ = client.subscribe(&topic, endpoint.qos).await;
                                }
                            }
                            Ok(_) => {}
                            Err(e) => {
                                warn!("MQTT eventloop error for route {}: {}", route_clone, e);
                                tokio::time::sleep(Duration::from_millis(1000)).await;
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    /// Spawns an async MQTT subscriber for PLC diagnostic logs (CODESYS LoggerFacade batch format)
    pub async fn subscribe_plc_log(
        route_id: String,
        context: RequestContext,
        topic: String,
        db: DatabaseService,
        notifier: InboundErrorNotifier,
        config: AppConfig,
        cancel_token: CancellationToken,
    ) -> Result<(), String> {
        let broker_uri = context.broker_uri.as_deref().ok_or_else(|| {
            format!(
                "DeviceRequest {} does not define brokerUri.",
                context.request_name
            )
        })?;

        let client_id = format!(
            "moqui-gw-plc-{}-{}",
            safe_part(&config.gateway_device_id),
            safe_part(&context.request_name)
        );

        let endpoint = parse_mqtt_uri(broker_uri, &client_id, &config)?;
        let mut options = endpoint.options;
        options.set_keep_alive(Duration::from_secs(30));

        let (client, mut eventloop) = AsyncClient::new(options, 50);

        client
            .subscribe(&topic, endpoint.qos)
            .await
            .map_err(|e| format!("Failed to subscribe to PLC log topic {}: {}", topic, e))?;

        info!(
            "PLC log subscriber started for route {} on topic {}",
            route_id, topic
        );

        let route_clone = route_id.clone();
        let context_clone = context.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!("PLC log subscriber cancelled for route {}", route_clone);
                        let _ = client.unsubscribe(&topic).await;
                        let _ = tokio::time::timeout(Duration::from_secs(2), async {
                            while let Ok(event) = eventloop.poll().await {
                                if matches!(event, Event::Incoming(Packet::UnsubAck(_))) { break; }
                            }
                        }).await;
                        let _ = client.disconnect().await;
                        let _ = tokio::time::timeout(Duration::from_millis(200), eventloop.poll()).await;
                        break;
                    }
                    notification = eventloop.poll() => {
                        match notification {
                            Ok(Event::Incoming(Packet::Publish(publish))) => {
                                let incoming_topic = String::from_utf8_lossy(&publish.topic).into_owned();
                                let payload_str = match std::str::from_utf8(&publish.payload) {
                                    Ok(s) => s,
                                    Err(e) => {
                                        warn!("Non-UTF8 PLC log payload on topic {}: {}", incoming_topic, e);
                                        continue;
                                    }
                                };

                                match Normalizer::partition_plc_log_batch(payload_str) {
                                    Ok(batch) => {
                                        let mut failed = false;
                                        if !batch.with_source.is_empty()
                                            && let Err(e) = db.insert_parameter_log_batch(&batch.with_source).await {
                                                warn!("Failed to store PLC log parameter batch: {}", e);
                                                notifier.record_error(&route_clone, Some(&context_clone.request_name), "MQTT-PLC-LOG", &e.to_string()).await;
                                                failed = true;
                                            }
                                        if !batch.without_source.is_empty()
                                            && let Err(e) = db.insert_device_log_batch(&batch.without_source).await {
                                                warn!("Failed to store PLC log device batch: {}", e);
                                                notifier.record_error(&route_clone, Some(&context_clone.request_name), "MQTT-PLC-LOG", &e.to_string()).await;
                                                failed = true;
                                            }
                                        if !failed {
                                            notifier.clear_error(&route_clone).await;
                                        }
                                    }
                                    Err(err) => {
                                        warn!("PLC log batch parse failure for {}: {}", route_clone, err);
                                        notifier.record_error(&route_clone, Some(&context_clone.request_name), "MQTT-PLC-LOG", &err).await;
                                    }
                                }
                            }
                            Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                                if !ack.session_present {
                                    let _ = client.subscribe(&topic, endpoint.qos).await;
                                }
                            }
                            Ok(_) => {}
                            Err(e) => {
                                warn!("MQTT PLC log error for route {}: {}", route_clone, e);
                                tokio::time::sleep(Duration::from_millis(1000)).await;
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }
}

fn build_mqtt_payload(
    item: &RequestItem,
) -> Result<serde_json::Map<String, serde_json::Value>, String> {
    let mut map = serde_json::Map::new();
    map.insert("parameterId".to_string(), json!(item.parameter_id));

    if let Some(num) = item.numeric_value {
        map.insert("numericValue".to_string(), json!(num));
    }
    if let Some(sym) = &item.symbolic_value {
        map.insert("symbolicValue".to_string(), json!(sym));
    }
    if let Some(penum) = &item.parameter_enum_id {
        map.insert("parameterEnumId".to_string(), json!(penum));
    }

    if let Some(req_name) = &item.request_item_name {
        if matches!(
            req_name.as_str(),
            "parameterId" | "numericValue" | "symbolicValue" | "parameterEnumId"
        ) {
            return Err(format!("MQTT requestItemName is reserved: {}", req_name));
        }
        let val = if let Some(n) = item.numeric_value {
            json!(n)
        } else if let Some(penum) = &item.parameter_enum_id {
            json!(penum)
        } else if let Some(sym) = &item.symbolic_value {
            json!(sym)
        } else {
            serde_json::Value::Null
        };
        map.insert(req_name.clone(), val);
    }

    Ok(map)
}

struct MqttEndpoint {
    options: MqttOptions,
    topic: Option<String>,
    qos: QoS,
    retain: bool,
    public_uri: String,
}

fn parse_mqtt_uri(
    uri: &str,
    default_client_id: &str,
    config: &AppConfig,
) -> Result<MqttEndpoint, String> {
    let (broker_part, topic, query) = if let Some(rest) = uri.strip_prefix("paho-mqtt5:") {
        let (topic, query) = rest.split_once('?').unwrap_or((rest, ""));
        ("", Some(topic.to_string()).filter(|t| !t.is_empty()), query)
    } else {
        let (broker, query) = uri.split_once('?').unwrap_or((uri, ""));
        (broker, None, query)
    };
    let params: HashMap<String, String> = reqwest::Url::parse(&format!("http://options/?{query}"))
        .map_err(|_| "Invalid MQTT options".to_string())?
        .query_pairs()
        .into_owned()
        .collect();
    let broker = if broker_part.is_empty() {
        params
            .get("brokerUrl")
            .map(String::as_str)
            .unwrap_or(&config.mqtt_broker_url)
    } else {
        broker_part
    };
    let url = reqwest::Url::parse(broker).map_err(|_| "Invalid MQTT broker URL".to_string())?;
    let tls = matches!(url.scheme(), "ssl" | "mqtts" | "tls");
    if !tls && !matches!(url.scheme(), "tcp" | "mqtt") {
        return Err("Unsupported MQTT broker scheme".into());
    }
    let host = url.host_str().ok_or("Missing MQTT broker host")?;
    let port = url.port().unwrap_or(if tls { 8883 } else { 1883 });
    let mut options = MqttOptions::new(
        params
            .get("clientId")
            .map(String::as_str)
            .unwrap_or(default_client_id),
        host,
        port,
    );
    options.set_keep_alive(Duration::from_secs(30));
    if tls {
        options.set_transport(Transport::tls_with_default_config());
    }
    let clean_start = params
        .get("cleanStart")
        .map(|v| v.parse::<bool>())
        .transpose()
        .map_err(|_| "Invalid cleanStart".to_string())?
        .unwrap_or(false);
    options.set_clean_start(clean_start);
    let expiry = params
        .get("sessionExpiryInterval")
        .map(|v| v.parse::<u32>())
        .transpose()
        .map_err(|_| "Invalid sessionExpiryInterval".to_string())?
        .unwrap_or(86400);
    options.set_session_expiry_interval(Some(expiry));
    let username = params
        .get("userName")
        .cloned()
        .or_else(|| config.mqtt_username.clone());
    let password = params
        .get("password")
        .cloned()
        .or_else(|| config.mqtt_password.clone());
    if let Some(username) = username {
        options.set_credentials(username, password.unwrap_or_default());
    }
    let qos = match params.get("qos").map(String::as_str).unwrap_or("1") {
        "0" => QoS::AtMostOnce,
        "1" => QoS::AtLeastOnce,
        "2" => QoS::ExactlyOnce,
        _ => return Err("MQTT qos must be 0, 1 or 2".into()),
    };
    let retain = params
        .get("retained")
        .or_else(|| params.get("retain"))
        .map(|v| v.parse::<bool>())
        .transpose()
        .map_err(|_| "Invalid MQTT retained flag".to_string())?
        .unwrap_or(false);
    // Never return the original URI: its query or userinfo can contain credentials.
    let public_uri = format!("{}://{}:{}", url.scheme(), host, port);
    Ok(MqttEndpoint {
        options,
        topic,
        qos,
        retain,
        public_uri,
    })
}

impl MqttService {
    async fn publish_messages(
        endpoint: MqttEndpoint,
        messages: Vec<(String, Vec<u8>)>,
        timeout: Option<i32>,
    ) -> Result<Vec<String>, String> {
        let (client, mut eventloop) = AsyncClient::new(endpoint.options, 10);
        let operation = async {
            let mut uris = Vec::new();
            for (topic, payload) in messages {
                client
                    .publish(&topic, endpoint.qos, endpoint.retain, payload)
                    .await
                    .map_err(|e| e.to_string())?;
                loop {
                    match eventloop
                        .poll()
                        .await
                        .map_err(|_| "MQTT connection or publish failed".to_string())?
                    {
                        Event::Outgoing(Outgoing::Publish(_))
                            if endpoint.qos == QoS::AtMostOnce =>
                        {
                            break;
                        }
                        Event::Incoming(Packet::PubAck(ack)) => {
                            if (ack.reason as u8) >= 128 {
                                return Err("MQTT broker rejected publication".into());
                            }
                            break;
                        }
                        Event::Incoming(Packet::PubRec(ack)) if (ack.reason as u8) >= 128 => {
                            return Err("MQTT broker rejected publication".into());
                        }
                        Event::Incoming(Packet::PubComp(ack)) => {
                            if (ack.reason as u8) >= 128 {
                                return Err("MQTT broker rejected publication".into());
                            }
                            break;
                        }
                        _ => {}
                    }
                }
                uris.push(format!("{}/{}", endpoint.public_uri, topic));
            }
            // A disconnect request is best effort after every publication has completed.
            let _ = client.disconnect().await;
            let _ = tokio::time::timeout(Duration::from_millis(200), eventloop.poll()).await;
            Ok(uris)
        };
        tokio::time::timeout(
            Duration::from_secs(timeout.unwrap_or(30).max(1) as u64),
            operation,
        )
        .await
        .map_err(|_| "MQTT publication timed out before broker acknowledgement".to_string())?
    }

    pub async fn publish_json(
        config: &AppConfig,
        topic: &str,
        payload: &serde_json::Value,
    ) -> Result<(), String> {
        let endpoint = parse_mqtt_uri(
            &config.mqtt_broker_url,
            &format!("moqui-gw-bulk-{}", uuid::Uuid::new_v4()),
            config,
        )?;
        let bytes = serde_json::to_vec(payload).map_err(|e| e.to_string())?;
        Self::publish_messages(endpoint, vec![(topic.to_string(), bytes)], Some(30)).await?;
        Ok(())
    }
}

fn safe_part(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn send_callback(
    callback_uri: &str,
    event_type: &str,
    request_name: &str,
    device_id: Option<&str>,
    status: &str,
    protocol: &str,
    route_id: &str,
    row_count: usize,
    publish_uri_list: &[String],
) {
    if !callback_uri.starts_with("http://") && !callback_uri.starts_with("https://") {
        debug!("Skipping non-HTTP callback URI: {}", callback_uri);
        return;
    }
    let client = reqwest::Client::new();
    let mut payload = json!({
        "eventType": event_type,
        "requestName": request_name,
        "status": status,
        "protocol": protocol,
        "routeId": route_id,
        "rowCount": row_count,
        "publishUriList": publish_uri_list
    });
    if let Some(did) = device_id {
        payload["deviceId"] = json!(did);
    }

    let uri_string = callback_uri.to_string();
    tokio::spawn(async move {
        let _ = client.post(&uri_string).json(&payload).send().await;
    });
}
