use std::env;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub http_host: String,
    pub http_port: u16,

    pub api_auth_enabled: bool,
    pub api_auth_header: String,
    pub api_auth_token: String,
    pub profile: String,

    pub gateway_device_id: String,
    pub startup_discovery_enabled: bool,

    pub db_url: String,
    pub log_db_url: String,

    #[allow(dead_code)]
    pub mqtt_broker_url: String,

    pub inbound_error_notification_enabled: bool,
    pub inbound_error_notification_threshold_seconds: u64,
    pub inbound_error_notification_uri: Option<String>,

    pub file_transfer_uri: String,
    pub file_transfer_uri_2_enabled: bool,
    pub file_transfer_uri_2: Option<String>,

    pub mqtt_write_after_publish_enabled: bool,
    pub mqtt_write_after_publish_uri: Option<String>,

    pub opcua_write_after_publish_enabled: bool,
    pub opcua_write_after_publish_uri: Option<String>,

    pub mqtt_read_after_store_enabled: bool,
    pub mqtt_read_after_store_uri: Option<String>,

    pub mqtt_read_route_auto_startup: bool,
    pub mqtt_read_consume_uri: Option<String>,
    pub plc_log_route_auto_startup: bool,
    pub plc_log_consume_uri: Option<String>,
}

impl AppConfig {
    pub fn load() -> Self {
        // Load .env if present
        if Path::new(".env").exists() {
            let _ = dotenvy::from_filename(".env");
        } else if Path::new("runtime/.env").exists() {
            let _ = dotenvy::from_filename("runtime/.env");
        }

        // Also parse properties file if configured in SmallRye config path
        let mut props = std::collections::HashMap::new();
        if let Ok(config_loc) = env::var("SMALLRYE_CONFIG_LOCATIONS") {
            let config_dir = Path::new(&config_loc);
            if config_dir.is_dir() {
                if let Ok(entries) = fs::read_dir(config_dir) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.extension().and_then(|s| s.to_str()) == Some("properties") {
                            load_properties_into(&path, &mut props);
                        }
                    }
                }
            } else if config_dir.is_file() {
                load_properties_into(config_dir, &mut props);
            }
        }

        let get = |key: &str, alt_env: Option<&str>, default: &str| -> String {
            if let Ok(val) = env::var(key)
                && !val.trim().is_empty() {
                    return val.trim().to_string();
                }
            if let Some(alt) = alt_env
                && let Ok(val) = env::var(alt)
                    && !val.trim().is_empty() {
                        return val.trim().to_string();
                    }
            if let Some(val) = props.get(key)
                && !val.trim().is_empty() {
                    return val.trim().to_string();
                }
            default.to_string()
        };

        let profile = get("APP_PROFILE", Some("QUARKUS_PROFILE"), "prod");
        let http_host = get("HTTP_HOST", Some("QUARKUS_HTTP_HOST"), "0.0.0.0");
        let http_port = get("HTTP_PORT", Some("QUARKUS_HTTP_PORT"), "8081")
            .parse::<u16>()
            .unwrap_or(8081);

        let api_auth_enabled = get("GATEWAY_API_AUTH_ENABLED", Some("gateway.api.auth.enabled"), "true")
            .parse::<bool>()
            .unwrap_or(true);
        let api_auth_header = get("GATEWAY_API_AUTH_HEADER", Some("gateway.api.auth.header"), "X-API-Key");
        let api_auth_token = get("GATEWAY_API_AUTH_TOKEN", Some("gateway.api.auth.token"), "change-me-in-production");

        let gateway_device_id = get("GATEWAY_DEVICE_ID", Some("gateway.device.id"), "");
        let startup_discovery_enabled = get("GATEWAY_STARTUP_DISCOVERY_ENABLED", Some("gateway.startup.discovery.enabled"), "true")
            .parse::<bool>()
            .unwrap_or(true);

        let raw_db_url = get(
            "DATABASE_URL",
            Some("QUARKUS_DATASOURCE_JDBC_URL"),
            "postgres://moqui:moqui@localhost:5432/moqui",
        );
        let db_url = normalize_postgres_url(&raw_db_url);

        let raw_log_db_url = get(
            "LOG_DATABASE_URL",
            Some("QUARKUS_DATASOURCE_LOG_JDBC_URL"),
            &raw_db_url,
        );
        let log_db_url = normalize_postgres_url(&raw_log_db_url);

        let mqtt_broker_url = get("MQTT_BROKER_URL", Some("mqtt.broker.url"), "tcp://localhost:1883");

        let inbound_error_notification_enabled = get(
            "GATEWAY_INBOUND_ERROR_NOTIFICATION_ENABLED",
            Some("gateway.inbound.error.notification.enabled"),
            "true",
        )
        .parse::<bool>()
        .unwrap_or(true);

        let inbound_error_notification_threshold_seconds = get(
            "GATEWAY_INBOUND_ERROR_NOTIFICATION_THRESHOLD_SECONDS",
            Some("gateway.inbound.error.notification.threshold.seconds"),
            "60",
        )
        .parse::<u64>()
        .unwrap_or(60);

        let raw_notify_uri = get("GATEWAY_INBOUND_ERROR_NOTIFICATION_URI", Some("gateway.inbound.error.notification.uri"), "");
        let inbound_error_notification_uri = if raw_notify_uri.is_empty() {
            None
        } else {
            Some(raw_notify_uri)
        };

        let file_transfer_uri = get("FILE_TRANSFER_URI", Some("file.transfer.uri"), "target/test-recipes");
        let file_transfer_uri_2_enabled = get("FILE_TRANSFER_URI_2_ENABLED", Some("file.transfer.uri.2.enabled"), "false")
            .parse::<bool>()
            .unwrap_or(false);
        let raw_ft2 = get("FILE_TRANSFER_URI_2", Some("file.transfer.uri.2"), "");
        let file_transfer_uri_2 = if raw_ft2.is_empty() { None } else { Some(raw_ft2) };

        let mqtt_write_after_publish_enabled = get("MQTT_WRITE_AFTER_PUBLISH_ENABLED", Some("mqtt.write.afterPublish.enabled"), "true")
            .parse::<bool>()
            .unwrap_or(true);
        let raw_mqtt_after_pub = get("MQTT_WRITE_AFTER_PUBLISH_URI", Some("mqtt.write.afterPublish.uri"), "");
        let mqtt_write_after_publish_uri = if raw_mqtt_after_pub.is_empty() { None } else { Some(raw_mqtt_after_pub) };

        let opcua_write_after_publish_enabled = get("OPCUA_WRITE_AFTER_PUBLISH_ENABLED", Some("opcua.write.afterPublish.enabled"), "true")
            .parse::<bool>()
            .unwrap_or(true);
        let raw_opcua_after_pub = get("OPCUA_WRITE_AFTER_PUBLISH_URI", Some("opcua.write.afterPublish.uri"), "");
        let opcua_write_after_publish_uri = if raw_opcua_after_pub.is_empty() { None } else { Some(raw_opcua_after_pub) };

        let mqtt_read_after_store_enabled = get("MQTT_READ_AFTER_STORE_ENABLED", Some("mqtt.read.afterStore.enabled"), "true")
            .parse::<bool>()
            .unwrap_or(true);
        let raw_mqtt_after_store = get("MQTT_READ_AFTER_STORE_URI", Some("mqtt.read.afterStore.uri"), "");
        let mqtt_read_after_store_uri = if raw_mqtt_after_store.is_empty() { None } else { Some(raw_mqtt_after_store) };

        let mqtt_read_route_auto_startup = get("MQTT_READ_ROUTE_AUTO_STARTUP", Some("mqtt.read.route.autoStartup"), "true")
            .parse::<bool>()
            .unwrap_or(true);
        let raw_mqtt_consume = get("MQTT_READ_CONSUME_URI", Some("mqtt.read.consume.uri"), "");
        let mqtt_read_consume_uri = if raw_mqtt_consume.is_empty() { None } else { Some(raw_mqtt_consume) };

        let plc_log_route_auto_startup = get("PLC_LOG_ROUTE_AUTO_STARTUP", Some("plc.log.route.autoStartup"), "false")
            .parse::<bool>()
            .unwrap_or(false);
        let raw_plc_consume = get("PLC_LOG_CONSUME_URI", Some("plc.log.consume.uri"), "");
        let plc_log_consume_uri = if raw_plc_consume.is_empty() { None } else { Some(raw_plc_consume) };

        Self {
            http_host,
            http_port,
            api_auth_enabled,
            api_auth_header,
            api_auth_token,
            profile,
            gateway_device_id,
            startup_discovery_enabled,
            db_url,
            log_db_url,
            mqtt_broker_url,
            inbound_error_notification_enabled,
            inbound_error_notification_threshold_seconds,
            inbound_error_notification_uri,
            file_transfer_uri,
            file_transfer_uri_2_enabled,
            file_transfer_uri_2,
            mqtt_write_after_publish_enabled,
            mqtt_write_after_publish_uri,
            opcua_write_after_publish_enabled,
            opcua_write_after_publish_uri,
            mqtt_read_after_store_enabled,
            mqtt_read_after_store_uri,
            mqtt_read_route_auto_startup,
            mqtt_read_consume_uri,
            plc_log_route_auto_startup,
            plc_log_consume_uri,
        }
    }

    pub fn is_safe_dev_profile(&self) -> bool {
        matches!(self.profile.as_str(), "dev" | "test" | "local" | "integration")
    }

    pub fn validate_security(&self) -> Result<(), String> {
        if !self.api_auth_enabled || self.is_safe_dev_profile() {
            return Ok(());
        }
        if self.api_auth_token.is_empty()
            || self.api_auth_token == "change-me"
            || self.api_auth_token == "change-me-in-production"
        {
            return Err("Unsafe gateway API token. Set GATEWAY_API_AUTH_TOKEN to a real secret.".to_string());
        }
        Ok(())
    }
}

fn normalize_postgres_url(url: &str) -> String {
    let trimmed = url.trim();
    if let Some(stripped) = trimmed.strip_prefix("jdbc:postgresql://") {
        format!("postgres://{}", stripped)
    } else if let Some(stripped) = trimmed.strip_prefix("jdbc:postgres://") {
        format!("postgres://{}", stripped)
    } else {
        trimmed.to_string()
    }
}

fn load_properties_into(path: &Path, props: &mut std::collections::HashMap<String, String>) {
    if let Ok(content) = fs::read_to_string(path) {
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
                continue;
            }
            if let Some((k, v)) = line.split_once('=') {
                props.insert(k.trim().to_string(), v.trim().to_string());
            }
        }
    }
}
