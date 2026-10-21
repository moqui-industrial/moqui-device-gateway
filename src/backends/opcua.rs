//! OPC UA sessions and native data-change subscriptions. No writes are replayed.
use crate::{
    client::{DeviceConnectorClient, SampleSink},
    model::*,
};
use async_trait::async_trait;
use opcua::{
    client::{ClientBuilder, DataChangeCallback, IdentityToken, MonitoredItem, Session},
    types::*,
};
use std::{
    collections::HashMap,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
#[derive(Default)]
pub struct OpcUaClient {
    sessions: Mutex<HashMap<String, Arc<SessionGuard>>>,
}
struct SessionGuard {
    session: Arc<Session>,
    task: tokio::task::JoinHandle<StatusCode>,
}
impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.task.abort();
    }
}
pub fn node_id(text: &str) -> Result<NodeId, DeviceConnectorError> {
    let text = text.trim();
    let text = text
        .strip_prefix("RAW(")
        .and_then(|t| t.strip_suffix(')'))
        .unwrap_or(text);
    NodeId::from_str(text)
        .map_err(|_| DeviceConnectorError::invalid("Invalid OPC UA NodeId; use an explicit namespace and identifier"))
}
pub fn from_variant(v: &Variant) -> Result<FieldValue, DeviceConnectorError> {
    Ok(match v {
        Variant::Empty => FieldValue::Null,
        Variant::Boolean(v) => FieldValue::Bool(*v),
        Variant::SByte(v) => FieldValue::Signed(*v as i64),
        Variant::Int16(v) => FieldValue::Signed(*v as i64),
        Variant::Int32(v) => FieldValue::Signed(*v as i64),
        Variant::Int64(v) => FieldValue::Signed(*v),
        Variant::Byte(v) => FieldValue::Unsigned(*v as u64),
        Variant::UInt16(v) => FieldValue::Unsigned(*v as u64),
        Variant::UInt32(v) => FieldValue::Unsigned(*v as u64),
        Variant::UInt64(v) => FieldValue::Unsigned(*v),
        Variant::Float(v) => FieldValue::Float(*v as f64),
        Variant::Double(v) => FieldValue::Float(*v),
        Variant::String(v) => FieldValue::Text(v.to_string()),
        Variant::ByteString(v) => FieldValue::Bytes(v.value.clone().unwrap_or_default()),
        _ => {
            return Err(DeviceConnectorError::unsupported(
                "Unsupported OPC UA variant type; no string coercion is applied",
            ));
        }
    })
}
pub fn to_variant(v: &FieldValue, ty: PhysicalType) -> Result<Variant, DeviceConnectorError> {
    let e = || DeviceConnectorError::invalid("Value does not match OPC UA physical type");
    let v = if matches!(ty, PhysicalType::Bool | PhysicalType::Text | PhysicalType::Bytes) {
        v.clone()
    } else {
        crate::codec::ValueCodec::default().encode(v, ParameterKind::Numeric, ty)?
    };
    Ok(match (ty, v) {
        (PhysicalType::Bool, FieldValue::Bool(v)) => Variant::Boolean(v),
        (PhysicalType::U8, FieldValue::Unsigned(v)) => Variant::Byte(v as u8),
        (PhysicalType::U16, FieldValue::Unsigned(v)) => Variant::UInt16(v as u16),
        (PhysicalType::U32, FieldValue::Unsigned(v)) => Variant::UInt32(v as u32),
        (PhysicalType::U64, FieldValue::Unsigned(v)) => Variant::UInt64(v),
        (PhysicalType::I8, FieldValue::Signed(v)) => Variant::SByte(v as i8),
        (PhysicalType::I16, FieldValue::Signed(v)) => Variant::Int16(v as i16),
        (PhysicalType::I32, FieldValue::Signed(v)) => Variant::Int32(v as i32),
        (PhysicalType::I64, FieldValue::Signed(v)) => Variant::Int64(v),
        (PhysicalType::F32, FieldValue::Float(v)) => Variant::Float(v as f32),
        (PhysicalType::F64, FieldValue::Float(v)) => Variant::Double(v),
        (PhysicalType::Text, FieldValue::Text(v)) => Variant::from(v.as_str()),
        (PhysicalType::Bytes, FieldValue::Bytes(v)) => Variant::ByteString(ByteString::from(v)),
        _ => return Err(e()),
    })
}
fn result(id: &str, v: DataValue) -> ItemResult {
    let status = v.status.unwrap_or(StatusCode::Good);
    let mut result = if !status.is_good() {
        ItemResult::error(id, DeviceConnectorError::transport(format!("OPC UA quality: {status}")))
    } else {
        match v.value.as_ref().map(from_variant).transpose() {
            Ok(Some(v)) => ItemResult::value(id, v),
            Ok(None) => ItemResult::error(id, DeviceConnectorError::transport("OPC UA result has no value")),
            Err(e) => ItemResult::error(id, e),
        }
    };
    result.quality = Some(status.to_string());
    result.source_time = v.source_timestamp.map(|t| SystemTime::from(t.as_chrono()));
    result
}
async fn connect(r: &PreparedDeviceRequest, deadline: Instant) -> Result<SessionGuard, DeviceConnectorError> {
    let c = &r.connection;
    let option = |k: &str| c.options.get(k).map(String::as_str);
    for key in c.options.keys() {
        if !matches!(
            key.as_str(),
            "securityPolicy" | "securityMode" | "pkiDir" | "certificateFile" | "privateKeyFile" | "applicationUri"
        ) {
            return Err(DeviceConnectorError::unsupported(format!(
                "Unknown OPC UA option: {key}"
            )));
        }
    }
    let url = url::Url::parse(&c.endpoint).map_err(|_| DeviceConnectorError::invalid("Invalid OPC UA endpoint"))?;
    if url.scheme() != "opc.tcp" || !url.username().is_empty() || url.password().is_some() || url.query().is_some() {
        return Err(DeviceConnectorError::invalid(
            "OPC UA endpoint must be opc.tcp:// without embedded credentials or options",
        ));
    }
    let policy = option("securityPolicy").unwrap_or("None");
    if !matches!(
        policy,
        "None" | "Basic256Sha256" | "Aes128_Sha256_RsaOaep" | "Aes256_Sha256_RsaPss"
    ) {
        return Err(DeviceConnectorError::unsupported(
            "Unsupported or deprecated OPC UA security policy",
        ));
    }
    let mode = match option("securityMode").unwrap_or("None") {
        "None" => MessageSecurityMode::None,
        "Sign" => MessageSecurityMode::Sign,
        "SignAndEncrypt" => MessageSecurityMode::SignAndEncrypt,
        _ => return Err(DeviceConnectorError::invalid("Invalid OPC UA security mode")),
    };
    if (policy == "None") != (mode == MessageSecurityMode::None) {
        return Err(DeviceConnectorError::invalid(
            "OPC UA policy and security mode disagree",
        ));
    }
    let mut builder = ClientBuilder::new()
        .application_name("moqui-device-connector")
        .application_uri(option("applicationUri").unwrap_or("urn:moqui:device-connector"))
        .pki_dir(option("pkiDir").unwrap_or("runtime/device-connector/opcua-pki"))
        .trust_server_certs(false)
        .verify_server_certs(true)
        .create_sample_keypair(false)
        .request_timeout(r.timeout)
        .session_retry_limit(0);
    match (option("certificateFile"), option("privateKeyFile")) {
        (Some(c), Some(k)) => {
            builder = builder.certificate_path(c).private_key_path(k);
        }
        (None, None) if mode == MessageSecurityMode::None => {}
        _ => {
            return Err(DeviceConnectorError::invalid(
                "Secure OPC UA requires certificateFile and privateKeyFile",
            ));
        }
    }
    let (identity, token_type) = match (&c.credentials.username, &c.credentials.password) {
        (None, None) => (IdentityToken::Anonymous, UserTokenType::Anonymous),
        (Some(u), Some(p)) if mode == MessageSecurityMode::SignAndEncrypt => {
            (IdentityToken::new_user_name(u, p), UserTokenType::UserName)
        }
        _ => {
            return Err(DeviceConnectorError::invalid(
                "Username/password require SignAndEncrypt",
            ));
        }
    };
    let mut token = UserTokenPolicy::anonymous();
    token.token_type = token_type;
    let policy = format!("http://opcfoundation.org/UA/SecurityPolicy#{policy}");
    let endpoint: EndpointDescription = (c.endpoint.as_str(), policy.as_str(), mode, token).into();
    let mut client = builder
        .client()
        .map_err(|_| DeviceConnectorError::invalid("Invalid OPC UA client configuration"))?;
    let (session, event_loop) =
        tokio::time::timeout_at(deadline.into(), client.connect_to_matching_endpoint(endpoint, identity))
            .await
            .map_err(|_| DeviceConnectorError::timeout(false))?
            .map_err(|_| DeviceConnectorError::transport("OPC UA endpoint connection failed"))?;
    let guard = SessionGuard {
        session: session.clone(),
        task: event_loop.spawn(),
    };
    if !tokio::time::timeout_at(deadline.into(), session.wait_for_connection())
        .await
        .map_err(|_| DeviceConnectorError::timeout(false))?
    {
        return Err(DeviceConnectorError::transport("OPC UA session did not connect"));
    }
    Ok(guard)
}
impl OpcUaClient {
    async fn session(
        &self,
        r: &PreparedDeviceRequest,
        deadline: Instant,
    ) -> Result<Arc<SessionGuard>, DeviceConnectorError> {
        let mut sessions = tokio::time::timeout_at(deadline.into(), self.sessions.lock())
            .await
            .map_err(|_| DeviceConnectorError::timeout(false))?;
        let key = r.connection.session_key();
        if let Some(s) = sessions.get(&key)
            && !s.task.is_finished()
        {
            return Ok(s.clone());
        }
        let s = Arc::new(connect(r, deadline).await?);
        sessions.insert(key, s.clone());
        Ok(s)
    }
}
#[async_trait]
impl DeviceConnectorClient for OpcUaClient {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            read: true,
            write: true,
            subscribe: true,
            transfer: false,
        }
    }
    async fn execute(
        &self,
        r: &PreparedDeviceRequest,
        input: &RequestInput,
        out: &mut RequestOutput,
        deadline: Instant,
    ) -> Result<(), DeviceConnectorError> {
        let nodes = r
            .items
            .iter()
            .map(|i| node_id(&i.address))
            .collect::<Result<Vec<_>, _>>()?;
        if nodes.is_empty() {
            return Err(DeviceConnectorError::invalid("OPC UA request has no items"));
        }
        let writing = r.operation == Operation::Write;
        let writes = if writing {
            r.items
                .iter()
                .zip(&nodes)
                .map(|(i, n)| {
                    Ok(WriteValue {
                        node_id: n.clone(),
                        attribute_id: AttributeId::Value as u32,
                        index_range: NumericRange::None,
                        value: DataValue::value_only(to_variant(
                            input
                                .values
                                .get(&i.parameter_id)
                                .ok_or_else(|| DeviceConnectorError::invalid("Missing OPC UA write value"))?,
                            i.physical_type,
                        )?),
                    })
                })
                .collect::<Result<Vec<_>, DeviceConnectorError>>()?
        } else {
            vec![]
        };
        let guard = self.session(r, deadline).await?;
        let s = &guard.session;
        if writing {
            let statuses = tokio::time::timeout_at(deadline.into(), s.write(&writes))
                .await
                .map_err(|_| DeviceConnectorError::timeout(true))?
                .map_err(|_| DeviceConnectorError::transport("OPC UA write failed").uncertain())?;
            if statuses.len() != r.items.len() {
                return Err(DeviceConnectorError::transport("OPC UA write result count differs").uncertain());
            }
            for (i, status) in r.items.iter().zip(statuses) {
                out.items.push(if status.is_good() {
                    ItemResult::value(&i.parameter_id, input.values[&i.parameter_id].clone())
                } else {
                    ItemResult::error(
                        &i.parameter_id,
                        DeviceConnectorError::transport(format!("OPC UA write rejected: {status}")),
                    )
                });
            }
        } else {
            let nodes = nodes.into_iter().map(ReadValueId::from).collect::<Vec<_>>();
            let values = tokio::time::timeout_at(deadline.into(), s.read(&nodes, TimestampsToReturn::Both, 0.0))
                .await
                .map_err(|_| DeviceConnectorError::timeout(false))?
                .map_err(|_| DeviceConnectorError::transport("OPC UA read failed"))?;
            if values.len() != r.items.len() {
                return Err(DeviceConnectorError::transport("OPC UA read result count differs"));
            }
            for (i, v) in r.items.iter().zip(values) {
                out.items.push(result(&i.parameter_id, v));
            }
        }
        Ok(())
    }
    async fn subscribe(
        &self,
        r: Arc<PreparedDeviceRequest>,
        sink: Arc<dyn SampleSink>,
        cancel: CancellationToken,
        deadline: Instant,
    ) -> Result<tokio::task::JoinHandle<()>, DeviceConnectorError> {
        let nodes = r
            .items
            .iter()
            .map(|i| node_id(&i.address))
            .collect::<Result<Vec<_>, _>>()?;
        if nodes.is_empty() {
            return Err(DeviceConnectorError::invalid("OPC UA subscription has no items"));
        }
        // A subscription owns a dedicated session, so cancellation cannot affect other requests.
        let guard = connect(&r, deadline).await?;
        let req = r.clone();
        let ids = nodes.clone();
        let callback = DataChangeCallback::new(move |value: DataValue, item: &MonitoredItem| {
            let Some(index) = ids.iter().position(|id| id == &item.item_to_monitor().node_id) else {
                return;
            };
            let value = result(&req.items[index].parameter_id, value);
            if let Some(error) = value.error {
                tracing::error!(request=%req.name,error=%error,"OPC UA sample rejected");
                return;
            }
            if let Some(value_data) = value.value
                && let Err(error) = sink.emit(SampleBatch {
                    request_name: req.name.clone(),
                    logging: req.logging,
                    request: Some(req.clone()),
                    samples: vec![Sample {
                        device_id: req.device_id.clone(),
                        log_value_kind: None,
                        parameter_id: Some(value.parameter_id),
                        value: value_data,
                        source_time: value.source_time,
                        received_time: SystemTime::now(),
                        quality: value.quality,
                    }],
                })
            {
                tracing::error!(request=%req.name,error=%error,"OPC UA sample not queued");
            }
        });
        let session = &guard.session;
        let sub = tokio::time::timeout_at(
            deadline.into(),
            session.create_subscription(r.polling_interval, 10, 30, 0, 0, true, callback),
        )
        .await
        .map_err(|_| DeviceConnectorError::timeout(false))?
        .map_err(|_| DeviceConnectorError::transport("Cannot create OPC UA subscription"))?;
        let items = nodes
            .into_iter()
            .map(|n| {
                MonitoredItemCreateRequest::new(
                    ReadValueId::from(n),
                    MonitoringMode::Reporting,
                    MonitoringParameters::default(),
                )
            })
            .collect::<Vec<_>>();
        let results = tokio::time::timeout_at(
            deadline.into(),
            session.create_monitored_items(sub, TimestampsToReturn::Both, items),
        )
        .await
        .map_err(|_| DeviceConnectorError::timeout(false))?
        .map_err(|_| DeviceConnectorError::transport("Cannot create OPC UA monitored items"))?;
        if results.len() != r.items.len() || results.iter().any(|i| !i.result.status_code.is_good()) {
            return Err(DeviceConnectorError::transport("OPC UA rejected monitored items"));
        }
        Ok(tokio::spawn(async move {
            tokio::select! {_=cancel.cancelled()=>{},_ = async {while !guard.task.is_finished(){tokio::time::sleep(Duration::from_millis(100)).await;}}=>{tracing::error!(request=%r.name,"OPC UA subscription session stopped");}}
            let _ = tokio::time::timeout(Duration::from_secs(2), guard.session.delete_subscriptions(&[sub])).await;
            let _ = tokio::time::timeout(Duration::from_secs(2), guard.session.disconnect()).await;
        }))
    }
    async fn close(&self) {
        let sessions = std::mem::take(&mut *self.sessions.lock().await);
        for (_, s) in sessions {
            let _ = tokio::time::timeout(Duration::from_secs(2), s.session.disconnect()).await;
        }
    }
}
