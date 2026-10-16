use crate::config::AppConfig;
use crate::db::DatabaseService;
use crate::model::{RequestContext, RequestItem};
use crate::pipeline::{InboundErrorNotifier, Normalizer};
use opcua::client::{ClientBuilder, DataChangeCallback, IdentityToken, MonitoredItem, Session};
use opcua::types::{
    AttributeId, DataValue, EndpointDescription, MessageSecurityMode, MonitoredItemCreateRequest,
    MonitoringMode, MonitoringParameters, NodeId, ReadValueId, TimestampsToReturn, UserTokenPolicy,
    Variant, WriteValue,
};
use serde_json::json;
use std::str::FromStr;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

pub struct OpcUaService;

struct SessionLoop(tokio::task::JoinHandle<opcua::types::StatusCode>);
impl Drop for SessionLoop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl OpcUaService {
    /// Builds endpoint description from RequestContext
    pub fn resolve_endpoint_url(context: &RequestContext) -> Result<String, String> {
        if let Some(transport) = &context.transport_config {
            let scheme = if let Some(code) = &context.transport_enum_code {
                if code == "tcp" { "opc.tcp" } else { code }
            } else {
                "opc.tcp"
            };
            if transport.starts_with("opc.") {
                return Ok(transport.clone());
            } else {
                return Ok(format!("{}://{}", scheme, transport));
            }
        }
        if let Some(broker) = &context.broker_uri {
            if let Some(stripped) = broker.strip_prefix("milo-client:") {
                return Ok(stripped.to_string());
            }
            if let Some(stripped) = broker.strip_prefix("opcua:") {
                return Ok(stripped.to_string());
            }
            if broker.starts_with("opc.tcp://") {
                return Ok(broker.clone());
            }
        }
        Err(format!(
            "DeviceRequest {} does not define transportConfig or brokerUri for OPC UA",
            context.request_name
        ))
    }

    /// Parses NodeId from string handling RAW(...) or standard ns=...;s=... or numeric
    pub fn parse_node_id(raw_str: &str) -> Result<NodeId, String> {
        let clean = raw_str.trim();
        let stripped = if clean.starts_with("RAW(") && clean.ends_with(')') {
            &clean[4..clean.len() - 1]
        } else {
            clean
        };

        if let Ok(node_id) = NodeId::from_str(stripped) {
            return Ok(node_id);
        }

        // Fallback: if it's pure numeric, treat as ns=0 numeric
        if let Ok(num) = stripped.parse::<u32>() {
            return Ok(NodeId::new(0, num));
        }

        // Fallback: treat as ns=2 string
        Ok(NodeId::new(2, stripped.to_string()))
    }

    /// Executes a one-shot read request (DrtRead)
    pub async fn run_read(
        context: &RequestContext,
        db: &DatabaseService,
    ) -> Result<serde_json::Value, String> {
        let endpoint_url = Self::resolve_endpoint_url(context)?;
        info!("Connecting to OPC UA endpoint for read: {}", endpoint_url);

        let mut client = ClientBuilder::new()
            .application_name("moqui-device-gateway")
            .application_uri("urn:moqui:device:gateway")
            .trust_server_certs(true)
            .create_sample_keypair(true)
            .client()
            .map_err(|e| format!("Failed to create OPC UA client: {:?}", e))?;

        let endpoint: EndpointDescription = (
            endpoint_url.as_str(),
            "None",
            MessageSecurityMode::None,
            UserTokenPolicy::anonymous(),
        )
            .into();

        let (session, event_loop) = client
            .connect_to_matching_endpoint(endpoint, IdentityToken::Anonymous)
            .await
            .map_err(|e| format!("OPC UA connect error to {}: {:?}", endpoint_url, e))?;

        let _loop_guard = SessionLoop(event_loop.spawn());
        let connected = tokio::time::timeout(
            Duration::from_secs(context.timeout.unwrap_or(30).max(1) as u64),
            session.wait_for_connection(),
        )
        .await
        .map_err(|_| "OPC UA connection timed out".to_string())?;
        if !connected {
            return Err("OPC UA session connection failed".into());
        }

        let mut read_nodes = Vec::new();
        let mut source_queries = Vec::new();

        for item in &context.items {
            let query = item
                .target_query
                .as_deref()
                .or(context.request_query.as_deref())
                .ok_or_else(|| {
                    format!(
                        "Missing query for request {} item {}",
                        context.request_name, item.parameter_id
                    )
                })?;

            let node_id = Self::parse_node_id(query)?;
            read_nodes.push(ReadValueId::from(node_id));
            source_queries.push(query.to_string());
        }

        let values = session
            .read(&read_nodes, TimestampsToReturn::Both, 0.0)
            .await
            .map_err(|e| format!("OPC UA read error: {:?}", e))?;

        let mut normalized_rows = Vec::new();
        for (i, val) in values.into_iter().enumerate() {
            if val.status.is_some_and(|status| status.is_bad()) || val.value.is_none() {
                return Err("OPC UA server returned a bad or empty read result".into());
            }
            let query = &source_queries[i];
            let raw_str = variant_to_string(&val.value);
            let rows = Normalizer::normalize_inbound_payload(context, query, &raw_str);
            normalized_rows.extend(rows);
        }

        if !normalized_rows.is_empty() {
            let is_parameter_upsert = context.purpose_enum_id.is_some()
                && context.purpose_enum_id.as_deref() != Some("DrpLogging");
            if is_parameter_upsert {
                db.upsert_parameter_batch(&normalized_rows)
                    .await
                    .map_err(|e| format!("DB parameter write failed: {}", e))?;
            } else {
                db.insert_parameter_log_batch(&normalized_rows)
                    .await
                    .map_err(|e| format!("DB log write failed: {}", e))?;
            }
        }

        let read_uris = source_queries
            .into_iter()
            .map(|q| format!("{}/{}", endpoint_url, q))
            .collect::<Vec<_>>();

        Ok(json!({
            "status": "completed",
            "routeId": "opcua-read-device-request",
            "rowCount": normalized_rows.len(),
            "readUriList": read_uris
        }))
    }

    /// Executes an OPC UA write request (DrtWrite / DrtConfigWrite) with retry
    pub async fn run_opcua_write(
        context: &RequestContext,
        config: &AppConfig,
    ) -> Result<serde_json::Value, String> {
        let endpoint_url = Self::resolve_endpoint_url(context)?;
        info!("Connecting to OPC UA endpoint for write: {}", endpoint_url);

        let timeout_ms = context.timeout.unwrap_or(5).max(1).saturating_mul(1000);

        let mut client = ClientBuilder::new()
            .application_name("moqui-device-gateway")
            .application_uri("urn:moqui:device:gateway")
            .trust_server_certs(true)
            .create_sample_keypair(true)
            .client()
            .map_err(|e| format!("Failed to create OPC UA client: {:?}", e))?;

        let endpoint: EndpointDescription = (
            endpoint_url.as_str(),
            "None",
            MessageSecurityMode::None,
            UserTokenPolicy::anonymous(),
        )
            .into();

        let (session, event_loop) = client
            .connect_to_matching_endpoint(endpoint, IdentityToken::Anonymous)
            .await
            .map_err(|e| format!("OPC UA connect error to {}: {:?}", endpoint_url, e))?;

        let _loop_guard = SessionLoop(event_loop.spawn());
        let connected = tokio::time::timeout(
            Duration::from_secs(context.timeout.unwrap_or(30).max(1) as u64),
            session.wait_for_connection(),
        )
        .await
        .map_err(|_| "OPC UA connection timed out".to_string())?;
        if !connected {
            return Err("OPC UA session connection failed".into());
        }

        let mut publish_uri_list = Vec::new();
        for item in &context.items {
            let query = item
                .target_query
                .as_deref()
                .or(context.request_query.as_deref())
                .ok_or_else(|| {
                    format!(
                        "Missing node definition for request {} item {}",
                        context.request_name, item.parameter_id
                    )
                })?;

            let node_id = Self::parse_node_id(query)?;
            let variant = resolve_outbound_variant(item);

            let write_val = WriteValue {
                node_id,
                attribute_id: AttributeId::Value as u32,
                index_range: opcua::types::NumericRange::None,
                value: DataValue::value_only(variant),
            };

            write_with_retry(&session, write_val, timeout_ms).await?;
            publish_uri_list.push(format!("{}/{}", endpoint_url, query));
        }

        if config.opcua_write_after_publish_enabled
            && let Some(uri) = &config.opcua_write_after_publish_uri
            && (uri.starts_with("http://") || uri.starts_with("https://"))
        {
            let client = reqwest::Client::new();
            let payload = json!({
                "eventType": "writeCompleted",
                "requestName": context.request_name,
                "status": "completed",
                "protocol": "OPC UA",
                "routeId": "opcua-write-device-request",
                "rowCount": context.items.len(),
                "publishUriList": publish_uri_list
            });
            let u = uri.clone();
            tokio::spawn(async move {
                let _ = client.post(&u).json(&payload).send().await;
            });
        }

        Ok(json!({
            "status": "completed",
            "routeId": "opcua-write-device-request",
            "rowCount": context.items.len(),
            "publishUriList": publish_uri_list
        }))
    }

    /// Spawns an async OPC UA subscription task with monitored items
    pub async fn subscribe_opcua(
        route_id: String,
        context: RequestContext,
        node_query: String,
        db: DatabaseService,
        notifier: InboundErrorNotifier,
        cancel_token: CancellationToken,
    ) -> Result<(), String> {
        let endpoint_url = Self::resolve_endpoint_url(&context)?;
        info!(
            "Starting OPC UA subscription for route {} on {}",
            route_id, endpoint_url
        );

        let node_id = Self::parse_node_id(&node_query)?;
        let polling_interval = context.polling_interval.unwrap_or(100).max(1) as u64;

        let mut client = ClientBuilder::new()
            .application_name("moqui-device-gateway")
            .application_uri("urn:moqui:device:gateway")
            .trust_server_certs(true)
            .create_sample_keypair(true)
            .client()
            .map_err(|e| format!("Failed to create OPC UA client: {:?}", e))?;

        let endpoint: EndpointDescription = (
            endpoint_url.as_str(),
            "None",
            MessageSecurityMode::None,
            UserTokenPolicy::anonymous(),
        )
            .into();

        let (session, event_loop) = client
            .connect_to_matching_endpoint(endpoint, IdentityToken::Anonymous)
            .await
            .map_err(|e| format!("OPC UA connect error to {}: {:?}", endpoint_url, e))?;

        let _loop_guard = SessionLoop(event_loop.spawn());
        let connected = tokio::time::timeout(
            Duration::from_secs(context.timeout.unwrap_or(30).max(1) as u64),
            session.wait_for_connection(),
        )
        .await
        .map_err(|_| "OPC UA connection timed out".to_string())?;
        if !connected {
            return Err("OPC UA session connection failed".into());
        }

        let db_clone = db.clone();
        let notifier_clone = notifier.clone();
        let route_clone = route_id.clone();
        let context_clone = context.clone();
        let query_clone = node_query.clone();

        let callback = DataChangeCallback::new(move |val: DataValue, _item: &MonitoredItem| {
            let db = db_clone.clone();
            let notifier = notifier_clone.clone();
            let route_id = route_clone.clone();
            let context = context_clone.clone();
            let query = query_clone.clone();

            tokio::spawn(async move {
                let raw_str = variant_to_string(&val.value);
                let rows = Normalizer::normalize_inbound_payload(&context, &query, &raw_str);
                if rows.is_empty() {
                    return;
                }

                let is_parameter_upsert = context.purpose_enum_id.is_some()
                    && context.purpose_enum_id.as_deref() != Some("DrpLogging");
                let save_result = if is_parameter_upsert {
                    db.upsert_parameter_batch(&rows).await
                } else {
                    db.insert_parameter_log_batch(&rows).await
                };

                match save_result {
                    Ok(_) => {
                        notifier.clear_error(&route_id).await;
                    }
                    Err(e) => {
                        warn!("DB persistence failed for OPC UA route {}: {}", route_id, e);
                        notifier
                            .record_error(
                                &route_id,
                                Some(&context.request_name),
                                "OPC UA",
                                &e.to_string(),
                            )
                            .await;
                    }
                }
            });
        });

        let sub_id = session
            .create_subscription(
                Duration::from_millis(polling_interval),
                10,
                30,
                0,
                0,
                true,
                callback,
            )
            .await
            .map_err(|e| format!("Failed to create subscription: {:?}", e))?;

        let monitored_items = vec![MonitoredItemCreateRequest::new(
            ReadValueId::from(node_id),
            MonitoringMode::Reporting,
            MonitoringParameters::default(),
        )];

        session
            .create_monitored_items(sub_id, TimestampsToReturn::Both, monitored_items)
            .await
            .map_err(|e| format!("Failed to create monitored items: {:?}", e))?;

        let route_for_cancel = route_id.clone();
        tokio::spawn(async move {
            let _loop_guard = _loop_guard;
            cancel_token.cancelled().await;
            info!(
                "OPC UA subscription cancelled for route {}",
                route_for_cancel
            );
            let _ = session.delete_subscriptions(&[sub_id]).await;
            let _ = session.disconnect().await;
        });

        Ok(())
    }
}

async fn write_with_retry(
    session: &Session,
    write_val: WriteValue,
    timeout_ms: i32,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms as u64);
    let mut attempts = 0;

    while tokio::time::Instant::now() < deadline {
        attempts += 1;
        match session.write(std::slice::from_ref(&write_val)).await {
            Ok(status_codes) => {
                if let Some(sc) = status_codes.first()
                    && sc.is_good()
                {
                    return Ok(());
                }
            }
            Err(e) => {
                debug!("OPC UA write attempt {} failed: {:?}", attempts, e);
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    Err(format!(
        "OPC UA write failed after {} attempts in {}ms",
        attempts, timeout_ms
    ))
}

fn resolve_outbound_variant(item: &RequestItem) -> Variant {
    if let Some(n) = item.numeric_value {
        return Variant::Double(n);
    }
    if let Some(penum) = &item.parameter_enum_id {
        return Variant::from(penum.as_str());
    }
    if let Some(sym) = &item.symbolic_value {
        return Variant::from(sym.as_str());
    }
    Variant::Empty
}

fn variant_to_string(variant: &Option<Variant>) -> String {
    match variant {
        Some(Variant::Boolean(b)) => {
            if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        Some(Variant::SByte(v)) => v.to_string(),
        Some(Variant::Byte(v)) => v.to_string(),
        Some(Variant::Int16(v)) => v.to_string(),
        Some(Variant::UInt16(v)) => v.to_string(),
        Some(Variant::Int32(v)) => v.to_string(),
        Some(Variant::UInt32(v)) => v.to_string(),
        Some(Variant::Int64(v)) => v.to_string(),
        Some(Variant::UInt64(v)) => v.to_string(),
        Some(Variant::Float(v)) => v.to_string(),
        Some(Variant::Double(v)) => v.to_string(),
        Some(Variant::String(s)) => s.to_string(),
        Some(other) => format!("{:?}", other),
        None => "".to_string(),
    }
}
