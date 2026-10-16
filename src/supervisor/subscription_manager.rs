use crate::config::AppConfig;
use crate::db::DatabaseService;
use crate::model::RequestContext;
use crate::pipeline::InboundErrorNotifier;
use crate::protocols::{ModbusService, MqttService, OpcUaService};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[allow(dead_code)]
pub struct ActiveSubscription {
    pub route_id: String,
    pub request_name: String,
    pub cancel_token: CancellationToken,
    pub started_at: jiff::Timestamp,
}

#[derive(Clone)]
pub struct SubscriptionManager {
    db: DatabaseService,
    config: AppConfig,
    notifier: InboundErrorNotifier,
    subscriptions: Arc<RwLock<HashMap<String, ActiveSubscription>>>,
}

impl SubscriptionManager {
    pub fn new(db: DatabaseService, config: AppConfig, notifier: InboundErrorNotifier) -> Self {
        Self {
            db,
            config,
            notifier,
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn db(&self) -> &DatabaseService {
        &self.db
    }

    pub fn config(&self) -> &AppConfig {
        &self.config
    }

    /// Restores database-defined subscriptions at gateway startup
    pub async fn on_start(&self) {
        // 1. Start static MQTT read consumer if configured
        if self.config.mqtt_read_route_auto_startup
            && let Some(uri) = &self.config.mqtt_read_consume_uri
            && (uri.starts_with("paho-mqtt5:")
                || uri.starts_with("tcp://")
                || uri.starts_with("mqtt://"))
        {
            let route_id = "mqtt-read-device-request-consumer".to_string();
            let cancel_token = CancellationToken::new();
            let topic = parse_static_topic(uri).unwrap_or_else(|| "iot/parameters/in".to_string());
            let dummy_context = RequestContext {
                request_name: "static-mqtt-read".to_string(),
                device_id: None,
                parent_request_name: None,
                request_type_enum_id: "DrtSubscribe".to_string(),
                purpose_enum_id: None,
                router_enum_id: None,
                connection_name: None,
                broker_uri: Some(uri.clone()),
                timeout: None,
                polling_interval: None,
                request_query: Some(topic.clone()),
                only_changed_parameters: None,
                driver_enum_id: None,
                driver_enum_code: None,
                transport_enum_id: None,
                transport_enum_code: None,
                transport_config: None,
                options: None,
                items: vec![],
            };
            if let Err(e) = MqttService::subscribe_mqtt(
                route_id.clone(),
                dummy_context,
                topic,
                self.db.clone(),
                self.notifier.clone(),
                self.config.clone(),
                cancel_token.clone(),
            )
            .await
            {
                warn!("Failed to start static MQTT consumer: {}", e);
            } else {
                info!("Started static MQTT consumer on {}", uri);
            }
        }

        // 2. Start static PLC log consumer if configured
        if self.config.plc_log_route_auto_startup
            && let Some(uri) = &self.config.plc_log_consume_uri
            && (uri.starts_with("paho-mqtt5:")
                || uri.starts_with("tcp://")
                || uri.starts_with("mqtt://"))
        {
            let route_id = "plc-log-consumer".to_string();
            let cancel_token = CancellationToken::new();
            let topic = parse_static_topic(uri).unwrap_or_else(|| "moqui-plc".to_string());
            let dummy_context = RequestContext {
                request_name: "static-plc-log".to_string(),
                device_id: None,
                parent_request_name: None,
                request_type_enum_id: "DrtSubscribe".to_string(),
                purpose_enum_id: Some("DrpLogging".to_string()),
                router_enum_id: None,
                connection_name: None,
                broker_uri: Some(uri.clone()),
                timeout: None,
                polling_interval: None,
                request_query: Some(topic.clone()),
                only_changed_parameters: None,
                driver_enum_id: None,
                driver_enum_code: None,
                transport_enum_id: None,
                transport_enum_code: None,
                transport_config: None,
                options: None,
                items: vec![],
            };
            if let Err(e) = MqttService::subscribe_plc_log(
                route_id.clone(),
                dummy_context,
                topic,
                self.db.clone(),
                self.notifier.clone(),
                self.config.clone(),
                cancel_token.clone(),
            )
            .await
            {
                warn!("Failed to start static PLC log consumer: {}", e);
            } else {
                info!("Started static PLC log consumer on {}", uri);
            }
        }

        if !self.config.startup_discovery_enabled {
            info!("Gateway startup DB discovery is disabled for this profile.");
            return;
        }

        let identity = match self
            .db
            .inspect_gateway_identity(&self.config.gateway_device_id)
            .await
        {
            Ok(id) => id,
            Err(e) => {
                error!(
                    "Gateway startup validation failed before route restore: {}",
                    e
                );
                return;
            }
        };

        if !identity.ready() {
            error!("Gateway startup validation failed: {}", identity.message);
            return;
        }

        let request_names = match self
            .db
            .load_startup_subscription_request_names(&identity.gateway_device_id)
            .await
        {
            Ok(names) => names,
            Err(e) => {
                warn!("Failed to query startup subscriptions: {}", e);
                return;
            }
        };

        let mut restored_requests = 0;
        let mut restored_routes = 0;

        for req_name in &request_names {
            match self.db.load_request_context(req_name).await {
                Ok(ctx) => match self.dispatch(&ctx).await {
                    Ok(res) => {
                        restored_requests += 1;
                        if let Some(arr) = res.get("routeIdList").and_then(|v| v.as_array()) {
                            restored_routes += arr.len();
                        } else {
                            restored_routes += 1;
                        }
                        info!(
                            "Restored DB-defined subscription for DeviceRequest {}",
                            req_name
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to restore subscription for DeviceRequest {}: {}",
                            req_name, e
                        );
                    }
                },
                Err(e) => {
                    warn!("Unable to load RequestContext for {}: {}", req_name, e);
                }
            }
        }

        info!(
            "Gateway {} restored {} DB-defined subscription route(s) from {} DeviceRequest row(s).",
            identity.gateway_device_id, restored_routes, restored_requests
        );
    }

    /// Dispatches a device request according to its requestTypeEnumId and protocol
    pub async fn dispatch(&self, context: &RequestContext) -> Result<serde_json::Value, String> {
        let req_type = context.request_type_enum_id.as_str();

        match req_type {
            "DrtRead" => {
                if context.is_opcua() {
                    OpcUaService::run_read(context, &self.db).await
                } else if context.is_modbus() {
                    ModbusService::run_read(context, &self.db).await
                } else {
                    Err(format!(
                        "DeviceRequest {} is a read request, but only OPC UA and Modbus one-shot reads are supported",
                        context.request_name
                    ))
                }
            }
            "DrtWrite" | "DrtConfigWrite" => {
                if context.is_opcua() {
                    OpcUaService::run_opcua_write(context, &self.config).await
                } else if context.is_modbus() {
                    ModbusService::run_write(context).await
                } else {
                    MqttService::run_mqtt_write(context, &self.config).await
                }
            }
            "DrtSubscribe" | "DrtEvent" | "DrtStateChange" | "DrtCyclic" => {
                self.register_subscriptions(context).await
            }
            "DrtUnsubscribe" => self.unsubscribe(context).await,
            other => Err(format!(
                "Unsupported gateway request type {} for request {}",
                other, context.request_name
            )),
        }
    }

    /// Registers dynamic subscriptions for MQTT, OPC UA, or Modbus
    async fn register_subscriptions(
        &self,
        context: &RequestContext,
    ) -> Result<serde_json::Value, String> {
        if context.is_plc_log() {
            let topic = context.request_query.as_deref().ok_or_else(|| {
                format!(
                    "PLC log request {} has no query topic",
                    context.request_name
                )
            })?;

            let route_id = format!("device-request-consumer-{}-0", context.request_name);

            let mut subs = self.subscriptions.write().await;
            if subs.contains_key(&route_id) {
                return Ok(json!({
                    "status": "already_active",
                    "routeId": "plc-log-subscribe-device-request",
                    "routeIdList": [route_id]
                }));
            }

            let cancel_token = CancellationToken::new();
            MqttService::subscribe_plc_log(
                route_id.clone(),
                context.clone(),
                topic.to_string(),
                self.db.clone(),
                self.notifier.clone(),
                self.config.clone(),
                cancel_token.clone(),
            )
            .await?;

            subs.insert(
                route_id.clone(),
                ActiveSubscription {
                    route_id: route_id.clone(),
                    request_name: context.request_name.clone(),
                    cancel_token,
                    started_at: jiff::Timestamp::now(),
                },
            );

            return Ok(json!({
                "status": "completed",
                "routeId": "plc-log-subscribe-device-request",
                "routeIdList": [route_id],
                "consumerUriList": [topic]
            }));
        }

        let topics = if context.is_modbus() && !context.items.is_empty() {
            // A Modbus polling task reads the whole request, not one task per register.
            vec![context.request_name.clone()]
        } else {
            resolve_topics(context)
        };
        if topics.is_empty() {
            return Err(format!(
                "DeviceRequest {} has no topic/address to subscribe.",
                context.request_name
            ));
        }

        let mut route_id_list = Vec::new();
        let mut consumer_uri_list = Vec::new();

        let mut subs = self.subscriptions.write().await;

        for (idx, topic) in topics.into_iter().enumerate() {
            let route_id = format!("device-request-consumer-{}-{}", context.request_name, idx);

            if subs.contains_key(&route_id) {
                route_id_list.push(route_id);
                consumer_uri_list.push(topic);
                continue;
            }

            let cancel_token = CancellationToken::new();

            if context.is_opcua() {
                OpcUaService::subscribe_opcua(
                    route_id.clone(),
                    context.clone(),
                    topic.clone(),
                    self.db.clone(),
                    self.notifier.clone(),
                    cancel_token.clone(),
                )
                .await?;
            } else if context.is_modbus() {
                ModbusService::subscribe_cyclic(
                    route_id.clone(),
                    context.clone(),
                    self.db.clone(),
                    self.notifier.clone(),
                    cancel_token.clone(),
                )
                .await?;
            } else {
                MqttService::subscribe_mqtt(
                    route_id.clone(),
                    context.clone(),
                    topic.clone(),
                    self.db.clone(),
                    self.notifier.clone(),
                    self.config.clone(),
                    cancel_token.clone(),
                )
                .await?;
            }

            subs.insert(
                route_id.clone(),
                ActiveSubscription {
                    route_id: route_id.clone(),
                    request_name: context.request_name.clone(),
                    cancel_token,
                    started_at: jiff::Timestamp::now(),
                },
            );

            route_id_list.push(route_id);
            consumer_uri_list.push(topic);
        }

        let protocol_name = if context.is_opcua() {
            "opcua"
        } else if context.is_modbus() {
            "modbus"
        } else {
            "mqtt"
        };

        Ok(json!({
            "status": "completed",
            "routeId": format!("{}-subscribe-device-request", protocol_name),
            "routeIdList": route_id_list,
            "consumerUriList": consumer_uri_list
        }))
    }

    /// Unsubscribes active tasks matching target request name
    pub async fn unsubscribe(&self, context: &RequestContext) -> Result<serde_json::Value, String> {
        let target_request_name = context
            .parent_request_name
            .as_deref()
            .unwrap_or(&context.request_name);

        let prefix = format!("device-request-consumer-{}-", target_request_name);

        let mut subs = self.subscriptions.write().await;
        let mut stopped_routes = Vec::new();

        let keys_to_remove: Vec<String> = subs
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();

        for k in keys_to_remove {
            if let Some(sub) = subs.remove(&k) {
                sub.cancel_token.cancel();
                stopped_routes.push(k);
            }
        }

        info!(
            "Unsubscribed {} route(s) for request {}",
            stopped_routes.len(),
            target_request_name
        );

        Ok(json!({
            "status": "completed",
            "routeId": "rest-unsubscribe-device-request",
            "routeIdList": stopped_routes,
            "targetRequestName": target_request_name
        }))
    }

    /// Returns active route count for readiness checks
    pub async fn active_route_count(&self) -> usize {
        self.subscriptions.read().await.len()
    }
}

fn resolve_topics(context: &RequestContext) -> Vec<String> {
    let mut topics = Vec::new();
    if let Some(req_q) = &context.request_query
        && !req_q.trim().is_empty()
        && !context.items.is_empty()
    {
        topics.push(req_q.clone());
    }
    for item in &context.items {
        if let Some(tq) = &item.target_query
            && !tq.trim().is_empty()
            && !topics.contains(tq)
        {
            topics.push(tq.clone());
        }
    }
    topics
}

fn parse_static_topic(uri: &str) -> Option<String> {
    if let Some(after_scheme) = uri
        .strip_prefix("paho-mqtt5:")
        .or_else(|| uri.strip_prefix("paho:"))
    {
        let topic = after_scheme.split('?').next().unwrap_or("").trim();
        if !topic.is_empty() {
            return Some(topic.to_string());
        }
    }
    None
}
