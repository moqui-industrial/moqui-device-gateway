use crate::config::AppConfig;
use crate::db::DatabaseService;
use crate::model::{RequestContext, RequestItem};
use crate::pipeline::{InboundErrorNotifier, Normalizer};
use rumqttc::{AsyncClient, MqttOptions, QoS};
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
        let broker_uri = context
            .broker_uri
            .as_deref()
            .ok_or_else(|| format!("DeviceRequest {} does not define brokerUri.", context.request_name))?;

        let (mut options, topic_override) = parse_mqtt_uri(broker_uri, "moqui-gw-write")?;
        options.set_keep_alive(Duration::from_secs(10));

        let (client, mut eventloop) = AsyncClient::new(options, 10);
        let event_handle = tokio::spawn(async move {
            while let Ok(event) = eventloop.poll().await {
                debug!("MQTT write event: {:?}", event);
            }
        });

        let mut publish_uri_list = Vec::new();
        for item in &context.items {
            let topic = item
                .target_query
                .as_deref()
                .or(context.request_query.as_deref())
                .or(topic_override.as_deref())
                .ok_or_else(|| {
                    format!(
                        "Missing topic query for request {} item {}",
                        context.request_name, item.parameter_id
                    )
                })?;

            let payload = build_mqtt_payload(item)?;
            let payload_bytes = serde_json::to_vec(&payload)
                .map_err(|e| format!("Failed to serialize MQTT payload: {}", e))?;

            client
                .publish(topic, QoS::AtLeastOnce, false, payload_bytes)
                .await
                .map_err(|e| format!("MQTT publish to {} failed: {}", topic, e))?;

            publish_uri_list.push(format!("{}/{}", broker_uri, topic));
        }

        drop(client);
        let _ = tokio::time::timeout(Duration::from_millis(500), event_handle).await;

        if config.mqtt_write_after_publish_enabled
            && let Some(uri) = &config.mqtt_write_after_publish_uri {
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
        let broker_uri = context
            .broker_uri
            .as_deref()
            .ok_or_else(|| format!("DeviceRequest {} does not define brokerUri.", context.request_name))?;

        let client_id = format!(
            "moqui-gw-{}-{}-{}",
            safe_part(&config.gateway_device_id),
            safe_part(&context.request_name),
            safe_part(&topic)
        );

        let (mut options, _) = parse_mqtt_uri(broker_uri, &client_id)?;
        options.set_clean_session(false);
        options.set_keep_alive(Duration::from_secs(30));

        let (client, mut eventloop) = AsyncClient::new(options, 50);

        client
            .subscribe(&topic, QoS::AtLeastOnce)
            .await
            .map_err(|e| format!("Failed to subscribe to MQTT topic {}: {}", topic, e))?;

        info!("MQTT subscriber started for route {} on topic {}", route_id, topic);

        let route_clone = route_id.clone();
        let context_clone = context.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!("MQTT subscriber cancelled for route {}", route_clone);
                        let _ = client.unsubscribe(&topic).await;
                        break;
                    }
                    notification = eventloop.poll() => {
                        match notification {
                            Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish))) => {
                                let payload_str = match std::str::from_utf8(&publish.payload) {
                                    Ok(s) => s,
                                    Err(e) => {
                                        warn!("Non-UTF8 MQTT payload on topic {}: {}", publish.topic, e);
                                        continue;
                                    }
                                };

                                let rows = Normalizer::normalize_inbound_payload(&context_clone, &publish.topic, payload_str);
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
                                                    std::slice::from_ref(&publish.topic),
                                                ).await;
                                            }
                                    }
                                    Err(e) => {
                                        warn!("DB persistence failed for MQTT route {}: {}", route_clone, e);
                                        notifier.record_error(&route_clone, Some(&context_clone.request_name), "MQTT", &e.to_string()).await;
                                    }
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
        let broker_uri = context
            .broker_uri
            .as_deref()
            .ok_or_else(|| format!("DeviceRequest {} does not define brokerUri.", context.request_name))?;

        let client_id = format!(
            "moqui-gw-plc-{}-{}",
            safe_part(&config.gateway_device_id),
            safe_part(&context.request_name)
        );

        let (mut options, _) = parse_mqtt_uri(broker_uri, &client_id)?;
        options.set_clean_session(false);
        options.set_keep_alive(Duration::from_secs(30));

        let (client, mut eventloop) = AsyncClient::new(options, 50);

        client
            .subscribe(&topic, QoS::AtLeastOnce)
            .await
            .map_err(|e| format!("Failed to subscribe to PLC log topic {}: {}", topic, e))?;

        info!("PLC log subscriber started for route {} on topic {}", route_id, topic);

        let route_clone = route_id.clone();
        let context_clone = context.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!("PLC log subscriber cancelled for route {}", route_clone);
                        let _ = client.unsubscribe(&topic).await;
                        break;
                    }
                    notification = eventloop.poll() => {
                        match notification {
                            Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish))) => {
                                let payload_str = match std::str::from_utf8(&publish.payload) {
                                    Ok(s) => s,
                                    Err(e) => {
                                        warn!("Non-UTF8 PLC log payload on topic {}: {}", publish.topic, e);
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

fn build_mqtt_payload(item: &RequestItem) -> Result<serde_json::Map<String, serde_json::Value>, String> {
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

fn parse_mqtt_uri(uri: &str, default_client_id: &str) -> Result<(MqttOptions, Option<String>), String> {
    // Expected formats:
    // tcp://host:port
    // paho-mqtt5:topic?brokerUrl=tcp://host:port&clientId=...
    let (broker_part, topic_part, query_part) = if let Some(after) = uri.strip_prefix("paho-mqtt5:") {
        if let Some((top, q)) = after.split_once('?') {
            ("", Some(top.to_string()), q)
        } else {
            ("", Some(after.to_string()), "")
        }
    } else {
        let (main, q) = uri.split_once('?').unwrap_or((uri, ""));
        (main, None, q)
    };

    let mut query_params = HashMap::new();
    for pair in query_part.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            query_params.insert(k, v);
        }
    }

    let broker_url = if !broker_part.is_empty() {
        broker_part
    } else if let Some(bu) = query_params.get("brokerUrl") {
        *bu
    } else {
        "tcp://localhost:1883"
    };

    let client_id = query_params
        .get("clientId")
        .cloned()
        .unwrap_or(default_client_id);

    let clean_url = broker_url
        .strip_prefix("tcp://")
        .or_else(|| broker_url.strip_prefix("mqtt://"))
        .unwrap_or(broker_url);

    let (host, port) = if let Some((h, p)) = clean_url.split_once(':') {
        (h, p.parse::<u16>().unwrap_or(1883))
    } else {
        (clean_url, 1883)
    };

    let mut options = MqttOptions::new(client_id, host, port);

    if let (Some(u), Some(p)) = (query_params.get("userName"), query_params.get("password")) {
        options.set_credentials(*u, *p);
    }

    Ok((options, topic_part))
}

fn safe_part(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
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
