use crate::db::DatabaseService;
use crate::model::{InboundRow, RequestContext};
use crate::pipeline::InboundErrorNotifier;
use rustls_pki_types::{CertificateDer, pem::PemObject};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio_modbus::client::Context;
use tokio_modbus::prelude::*;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub struct ModbusService;

impl ModbusService {
    /// Connects to Modbus TCP or Modbus TLS
    pub async fn connect(context: &RequestContext) -> Result<Context, String> {
        let transport = context
            .transport_config
            .as_deref()
            .or(context.broker_uri.as_deref())
            .ok_or_else(|| {
                format!(
                    "DeviceRequest {} does not define transportConfig or brokerUri for Modbus",
                    context.request_name
                )
            })?;

        let is_tls = context.transport_enum_code.as_deref() == Some("tls")
            || context.transport_enum_id.as_deref() == Some("DctrTls")
            || transport.starts_with("modbus.tls://");
        let clean_addr = transport
            .strip_prefix("modbus://")
            .or_else(|| transport.strip_prefix("modbus.tcp://"))
            .or_else(|| transport.strip_prefix("modbus.tls://"))
            .unwrap_or(transport);
        let url = reqwest::Url::parse(&format!("tcp://{clean_addr}"))
            .map_err(|_| "Invalid Modbus endpoint".to_string())?;
        let host = url.host_str().ok_or("Missing Modbus host")?;
        let port = url.port().unwrap_or(if is_tls { 802 } else { 502 });
        let tcp_stream = tokio::net::TcpStream::connect((host, port))
            .await
            .map_err(|e| format!("Modbus TCP connection failed: {e}"))?;

        if is_tls {
            let path = std::env::var("MODBUS_TLS_CA_FILE")
                .map_err(|_| "Set MODBUS_TLS_CA_FILE to the trusted CA PEM bundle")?;
            let mut root_store = rustls::RootCertStore::empty();
            for certificate in CertificateDer::pem_file_iter(path).map_err(|e| e.to_string())? {
                root_store
                    .add(certificate.map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
            }
            if root_store.is_empty() {
                return Err("Modbus TLS CA bundle has no certificates".into());
            }
            let client_config = rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();

            let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
            let domain = rustls_pki_types::ServerName::try_from(host)
                .map_err(|e| format!("Invalid TLS server name: {:?}", e))?
                .to_owned();

            let tls_stream = connector
                .connect(domain, tcp_stream)
                .await
                .map_err(|e| format!("Modbus TLS handshake error: {}", e))?;

            let client_ctx = tokio_modbus::client::tcp::attach(tls_stream);

            Ok(client_ctx)
        } else {
            let client_ctx = tokio_modbus::client::tcp::attach(tcp_stream);

            Ok(client_ctx)
        }
    }

    pub async fn run_read(
        context: &RequestContext,
        db: &DatabaseService,
    ) -> Result<serde_json::Value, String> {
        tokio::time::timeout(
            Duration::from_secs(context.timeout.unwrap_or(30).max(1) as u64),
            Self::run_read_inner(context, db),
        )
        .await
        .map_err(|_| "Modbus read timed out".to_string())?
    }

    pub async fn run_write(context: &RequestContext) -> Result<serde_json::Value, String> {
        tokio::time::timeout(
            Duration::from_secs(context.timeout.unwrap_or(30).max(1) as u64),
            Self::run_write_inner(context),
        )
        .await
        .map_err(|_| "Modbus write timed out".to_string())?
    }

    /// Executes Modbus one-shot read
    async fn run_read_inner(
        context: &RequestContext,
        db: &DatabaseService,
    ) -> Result<serde_json::Value, String> {
        let mut client = Self::connect(context).await?;
        let mut normalized_rows = Vec::new();

        for item in &context.items {
            let query = item
                .target_query
                .as_deref()
                .or(context.request_query.as_deref())
                .ok_or_else(|| {
                    format!(
                        "Missing Modbus query for request {} item {}",
                        context.request_name, item.parameter_id
                    )
                })?;

            let (reg_type, addr) = parse_modbus_target(query)?;
            let val_num = match reg_type {
                ModbusTargetType::Holding => {
                    let vals = client
                        .read_holding_registers(addr, 1)
                        .await
                        .map_err(|e| format!("Modbus read holding reg error: {:?}", e))?
                        .map_err(|e| format!("Modbus exception: {:?}", e))?;
                    vals.first().copied().map(|v| v as f64)
                }
                ModbusTargetType::Input => {
                    let vals = client
                        .read_input_registers(addr, 1)
                        .await
                        .map_err(|e| format!("Modbus read input reg error: {:?}", e))?
                        .map_err(|e| format!("Modbus exception: {:?}", e))?;
                    vals.first().copied().map(|v| v as f64)
                }
                ModbusTargetType::Coil => {
                    let vals = client
                        .read_coils(addr, 1)
                        .await
                        .map_err(|e| format!("Modbus read coils error: {:?}", e))?
                        .map_err(|e| format!("Modbus exception: {:?}", e))?;
                    vals.first().copied().map(|v| if v { 1.0 } else { 0.0 })
                }
                ModbusTargetType::Discrete => {
                    let vals = client
                        .read_discrete_inputs(addr, 1)
                        .await
                        .map_err(|e| format!("Modbus read discrete error: {:?}", e))?
                        .map_err(|e| format!("Modbus exception: {:?}", e))?;
                    vals.first().copied().map(|v| if v { 1.0 } else { 0.0 })
                }
            };

            if let Some(num) = val_num {
                normalized_rows.push(InboundRow {
                    parameter_id: item.parameter_id.clone(),
                    numeric_value: Some(num),
                    symbolic_value: None,
                    parameter_enum_id: None,
                    purpose_enum_id: context.purpose_enum_id.clone(),
                    observed_date: Some(jiff::Timestamp::now()),
                    sequence_num: None,
                    parameter_log_id: None,
                });
            }
        }

        if !normalized_rows.is_empty() {
            let is_parameter_upsert = context.purpose_enum_id.is_some()
                && context.purpose_enum_id.as_deref() != Some("DrpLogging");
            if is_parameter_upsert {
                db.upsert_parameter_batch(&normalized_rows)
                    .await
                    .map_err(|e| format!("DB write failed: {}", e))?;
            } else {
                db.insert_parameter_log_batch(&normalized_rows)
                    .await
                    .map_err(|e| format!("DB write failed: {}", e))?;
            }
        }

        Ok(json!({
            "status": "completed",
            "routeId": "modbus-read-device-request",
            "rowCount": normalized_rows.len()
        }))
    }

    /// Executes Modbus write (DrtWrite / DrtConfigWrite)
    async fn run_write_inner(context: &RequestContext) -> Result<serde_json::Value, String> {
        let mut client = Self::connect(context).await?;

        for item in &context.items {
            let query = item
                .target_query
                .as_deref()
                .or(context.request_query.as_deref())
                .ok_or_else(|| {
                    format!(
                        "Missing Modbus query for request {} item {}",
                        context.request_name, item.parameter_id
                    )
                })?;

            let (reg_type, addr) = parse_modbus_target(query)?;
            let num = match (item.numeric_value, item.symbolic_value.as_deref()) {
                (Some(number), _) if number.is_finite() => number,
                (None, Some("Y" | "true" | "TRUE")) => 1.0,
                (None, Some("N" | "false" | "FALSE")) => 0.0,
                _ => return Err("Missing or invalid Modbus write value".into()),
            };
            if matches!(reg_type, ModbusTargetType::Holding)
                && (!(0.0..=65535.0).contains(&num) || num.fract() != 0.0)
            {
                return Err("Modbus holding register requires an integer from 0 to 65535".into());
            }

            match reg_type {
                ModbusTargetType::Holding => {
                    client
                        .write_single_register(addr, num as u16)
                        .await
                        .map_err(|e| format!("Modbus write register error: {:?}", e))?
                        .map_err(|e| format!("Modbus exception: {:?}", e))?;
                }
                ModbusTargetType::Coil => {
                    client
                        .write_single_coil(addr, num > 0.0)
                        .await
                        .map_err(|e| format!("Modbus write coil error: {:?}", e))?
                        .map_err(|e| format!("Modbus exception: {:?}", e))?;
                }
                _ => {
                    return Err(format!("Cannot write to read-only Modbus target {}", query));
                }
            }
        }

        Ok(json!({
            "status": "completed",
            "routeId": "modbus-write-device-request",
            "rowCount": context.items.len()
        }))
    }

    /// Spawns a cyclic polling task for Modbus targets
    pub async fn subscribe_cyclic(
        route_id: String,
        context: RequestContext,
        db: DatabaseService,
        notifier: InboundErrorNotifier,
        cancel_token: CancellationToken,
    ) -> Result<(), String> {
        let polling_interval =
            Duration::from_millis(context.polling_interval.unwrap_or(200).max(1) as u64);
        info!(
            "Starting cyclic Modbus task for route {} with interval {:?}",
            route_id, polling_interval
        );

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(polling_interval);
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!("Modbus cyclic subscriber cancelled for route {}", route_id);
                        break;
                    }
                    _ = ticker.tick() => {
                        match Self::run_read(&context, &db).await {
                            Ok(_) => {
                                notifier.clear_error(&route_id).await;
                            }
                            Err(e) => {
                                warn!("Modbus cyclic read error for {}: {}", route_id, e);
                                notifier.record_error(&route_id, Some(&context.request_name), "Modbus", &e).await;
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }
}

enum ModbusTargetType {
    Holding,
    Input,
    Coil,
    Discrete,
}

fn parse_modbus_target(s: &str) -> Result<(ModbusTargetType, u16), String> {
    let clean = s.trim().to_lowercase();
    if let Some((kind, addr_str)) = clean.split_once(':') {
        let addr = addr_str
            .parse::<u16>()
            .map_err(|e| format!("Invalid register address {}: {}", addr_str, e))?;
        let t = match kind {
            "holding" | "hr" | "h" => ModbusTargetType::Holding,
            "input" | "ir" | "i" => ModbusTargetType::Input,
            "coil" | "c" => ModbusTargetType::Coil,
            "discrete" | "di" | "d" => ModbusTargetType::Discrete,
            _ => return Err(format!("Unknown Modbus register kind {}", kind)),
        };
        Ok((t, addr))
    } else if let Ok(addr) = clean.parse::<u16>() {
        Ok((ModbusTargetType::Holding, addr))
    } else {
        Err(format!("Invalid Modbus target syntax {}", s))
    }
}
