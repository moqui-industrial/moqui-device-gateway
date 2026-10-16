use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

#[derive(Debug)]
struct ErrorState {
    first_error_time: jiff::Timestamp,
    notified: AtomicBool,
}

#[derive(Clone)]
pub struct InboundErrorNotifier {
    enabled: bool,
    threshold_seconds: u64,
    notification_uri: Option<String>,
    errors: Arc<RwLock<HashMap<String, Arc<ErrorState>>>>,
    http_client: reqwest::Client,
}

impl InboundErrorNotifier {
    pub fn new(enabled: bool, threshold_seconds: u64, notification_uri: Option<String>) -> Self {
        Self {
            enabled,
            threshold_seconds,
            notification_uri,
            errors: Arc::new(RwLock::new(HashMap::new())),
            http_client: reqwest::Client::new(),
        }
    }

    pub async fn record_error(
        &self,
        route_id: &str,
        request_name: Option<&str>,
        protocol: &str,
        error_msg: &str,
    ) {
        if !self.enabled {
            return;
        }

        let state = {
            let mut errors = self.errors.write().await;
            errors
                .entry(route_id.to_string())
                .or_insert_with(|| {
                    warn!(
                        "Inbound error tracking started for route {}: {}",
                        route_id, error_msg
                    );
                    Arc::new(ErrorState {
                        first_error_time: jiff::Timestamp::now(),
                        notified: AtomicBool::new(false),
                    })
                })
                .clone()
        };

        let now = jiff::Timestamp::now();
        let elapsed = (now.as_millisecond() - state.first_error_time.as_millisecond()) / 1000;

        if elapsed >= self.threshold_seconds as i64
            && state
                .notified
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.send_notification(
                "inboundError",
                route_id,
                request_name,
                Some(protocol),
                Some(error_msg),
                state.first_error_time,
                elapsed,
            );
        }
    }

    pub async fn clear_error(&self, route_id: &str) {
        if !self.enabled {
            return;
        }

        let prev = {
            let mut errors = self.errors.write().await;
            errors.remove(route_id)
        };

        if let Some(state) = prev
            && state.notified.load(Ordering::SeqCst) {
                let now = jiff::Timestamp::now();
                let duration = (now.as_millisecond() - state.first_error_time.as_millisecond()) / 1000;
                self.send_notification(
                    "inboundRecovered",
                    route_id,
                    None,
                    None,
                    None,
                    state.first_error_time,
                    duration,
                );
            }
    }

    #[allow(clippy::too_many_arguments)]
    fn send_notification(
        &self,
        event_type: &'static str,
        route_id: &str,
        request_name: Option<&str>,
        protocol: Option<&str>,
        error_message: Option<&str>,
        first_error_time: jiff::Timestamp,
        duration_seconds: i64,
    ) {
        let uri = match &self.notification_uri {
            Some(u) if !u.trim().is_empty() => u.clone(),
            _ => {
                warn!(
                    "Inbound {} on route {} — no notification URI configured",
                    event_type, route_id
                );
                return;
            }
        };

        let mut payload = json!({
            "eventType": event_type,
            "routeId": route_id,
            "firstErrorTime": first_error_time.to_string(),
            "errorDurationSeconds": duration_seconds
        });

        if let Some(rn) = request_name {
            payload["requestName"] = json!(rn);
        }
        if let Some(p) = protocol {
            payload["protocol"] = json!(p);
        }
        if let Some(em) = error_message {
            payload["errorMessage"] = json!(em);
        }

        let client = self.http_client.clone();
        let route_str = route_id.to_string();

        tokio::spawn(async move {
            info!("Sending {} notification for route {}", event_type, route_str);
            match client.post(&uri).json(&payload).send().await {
                Ok(resp) => {
                    if resp.status().is_success() {
                        info!("Successfully sent {} notification for route {}", event_type, route_str);
                    } else {
                        warn!(
                            "Notification webhook for route {} returned status {}",
                            route_str,
                            resp.status()
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to send {} notification for route {}: {}",
                        event_type, route_str, e
                    );
                }
            }
        });
    }
}
