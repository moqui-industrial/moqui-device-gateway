//! Moqui-specific preparation and persistence. No facade is passed into transport code.
use crate::{codec::ValueCodec, model::*, tool::DeviceConnectorToolFactory};
use moqui_core::{
    context::ExecutionContext,
    entity::{EntityFacade, EntityValue},
    native::{Parameters, ServiceInfo},
};
use moqui_lib::{error::Error, object::Value};
use rust_decimal::Decimal;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
fn error(e: impl std::fmt::Display) -> Error {
    Error::new(e.to_string())
}
fn text(v: &EntityValue, key: &str) -> Option<String> {
    v.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(String::from)
}
fn decimal(v: &EntityValue, key: &str) -> Result<Option<Decimal>, Error> {
    v.get(key)
        .filter(|v| !matches!(v, Value::Null))
        .map(|v| match v {
            Value::Decimal(d) => Ok(*d),
            Value::I64(n) => Ok(Decimal::from(*n)),
            Value::I32(n) => Ok(Decimal::from(*n)),
            Value::String(s) => s.parse().map_err(error),
            _ => Err(Error::new(format!("{key} must be an exact decimal"))),
        })
        .transpose()
}
fn positive_ms(v: &EntityValue, key: &str, default: u64) -> Result<Duration, Error> {
    let n = decimal(v, key)?.unwrap_or(Decimal::from(default));
    let ms = u64::try_from(n).map_err(error)?;
    if ms == 0 || ms > 86_400_000 || !n.fract().is_zero() {
        return Err(Error::new(format!("{key} must be 1..86400000 milliseconds")));
    }
    Ok(Duration::from_millis(ms))
}
fn entity(params: &Parameters, key: &str) -> Result<Arc<EntityValue>, Error> {
    match params.get(key) {
        Some(Value::Object(v)) => v
            .downcast::<EntityValue>()
            .ok_or_else(|| Error::new(format!("{key} must be EntityValue"))),
        _ => Err(Error::new(format!("Missing {key}"))),
    }
}
fn facade(ec: &ExecutionContext) -> Result<EntityFacade, Error> {
    ec.factory()
        .entity()
        .ok_or_else(|| Error::new("Entity Engine is not installed"))
}
fn find(ef: &EntityFacade, name: &str, key: &str, value: &str, cache: bool) -> Result<EntityValue, Error> {
    ef.find(name)?
        .eq(key, Value::string(value))
        .use_cache(cache)
        .one()?
        .ok_or_else(|| Error::new(format!("{name} record not found")))
}
fn parse_options(raw: &str) -> Result<BTreeMap<String, String>, Error> {
    if raw.trim().starts_with('{') {
        return serde_json::from_str(raw).map_err(error);
    }
    let mut result = BTreeMap::new();
    for (k, v) in url::form_urlencoded::parse(raw.trim_start_matches('?').as_bytes()) {
        if result.insert(k.into_owned(), v.into_owned()).is_some() {
            return Err(Error::new("Duplicate connection option"));
        }
    }
    Ok(result)
}
fn connection(r: &EntityValue, c: Option<&EntityValue>, device_id: &str) -> Result<Connection, Error> {
    let mut options = if let Some(c) = c {
        if text(c, "deviceId").as_deref() != Some(device_id) {
            return Err(Error::new("Connection belongs to another device"));
        }
        if text(c, "userId").is_some() {
            return Err(Error::new(
                "DeviceConnection.userId needs an explicit credential provider; password hashes cannot authenticate a device",
            ));
        }
        parse_options(&text(c, "options").unwrap_or_default())?
    } else {
        BTreeMap::new()
    };
    let endpoint = text(r, "brokerUri")
        .or_else(|| c.and_then(|c| text(c, "transportConfig")))
        .ok_or_else(|| Error::new("No field connection endpoint"))?;
    let driver = c.and_then(|c| text(c, "driverEnumId"));
    let protocol =
        if endpoint.starts_with("paho-mqtt5:") || endpoint.starts_with("mqtt://") || endpoint.starts_with("mqtts://") {
            Protocol::Mqtt
        } else if endpoint.starts_with("milo-client:")
            || endpoint.starts_with("opc.tcp://")
            || driver.as_deref() == Some("DcdOpcUa")
        {
            Protocol::OpcUa
        } else if driver.as_deref() == Some("DcdModbusTCP") || endpoint.starts_with("modbus-tcp://") {
            Protocol::ModbusTcp
        } else if endpoint.starts_with("file:") {
            Protocol::File
        } else if endpoint.starts_with("sftp:") {
            Protocol::Sftp
        } else {
            return Err(Error::new(
                "Unsupported fieldbus protocol; no default backend is selected",
            ));
        };
    let mut endpoint = endpoint;
    if let Some(rest) = endpoint.strip_prefix("paho-mqtt5:") {
        let (_, query) = rest
            .split_once('?')
            .ok_or_else(|| Error::new("Legacy MQTT endpoint requires brokerUrl"))?;
        for (k, v) in parse_options(query)? {
            if options.insert(k, v).is_some() {
                return Err(Error::new("Duplicate MQTT connection option"));
            }
        }
        endpoint = options
            .remove("brokerUrl")
            .ok_or_else(|| Error::new("Legacy MQTT endpoint requires brokerUrl"))?;
        endpoint = endpoint
            .replacen("tcp://", "mqtt://", 1)
            .replacen("ssl://", "mqtts://", 1);
        if options.remove("qos").is_some_and(|s| s != "1") {
            return Err(Error::new("Only acknowledged MQTT QoS 1 is supported"));
        }
        if let Some(id) = options.remove("clientId") {
            options.insert("client-id-prefix".into(), id);
        }
    }
    if let Some(rest) = endpoint.strip_prefix("milo-client:") {
        endpoint = rest.to_string();
    }
    if protocol == Protocol::OpcUa
        && let Some((url, query)) = endpoint.split_once('?')
    {
        let url = url.to_string();
        for (k, v) in parse_options(query)? {
            if options.insert(k, v).is_some() {
                return Err(Error::new("Duplicate OPC UA option"));
            }
        }
        endpoint = url;
    }
    let username = options.remove("username").or_else(|| options.remove("userName"));
    let password = options.remove("password");
    let resolve = |value: Option<String>, env: Option<String>| -> Result<Option<String>, Error> {
        match (value, env) {
            (Some(_), Some(_)) => Err(Error::new(
                "Specify a credential value or environment variable, not both",
            )),
            (Some(v), None) => Ok(Some(v)),
            (None, Some(k)) => std::env::var(k)
                .map(Some)
                .map_err(|_| Error::new("Configured credential environment variable is missing")),
            _ => Ok(None),
        }
    };
    let username = resolve(username, options.remove("usernameEnv"))?;
    let password = resolve(password, options.remove("passwordEnv"))?;
    Ok(Connection {
        name: c
            .and_then(|c| text(c, "connectionName"))
            .unwrap_or_else(|| "broker".into()),
        protocol,
        endpoint,
        options,
        credentials: Credentials { username, password },
    })
}
fn kind(ef: &EntityFacade, def: &EntityValue) -> Result<ParameterKind, Error> {
    let ty = text(def, "parameterTypeEnumId").ok_or_else(|| Error::new("ParameterDef has no type"))?;
    Ok(match ty.as_str() {
        "PtTextIndicator" => ParameterKind::Indicator,
        "PtByte" | "PtBitSet" | "PtEnumeration" => ParameterKind::Bits,
        _ => {
            let enum_value = find(ef, "moqui.basic.Enumeration", "enumId", &ty, true)?;
            if ty == "PtNumber" || text(&enum_value, "parentEnumId").as_deref() == Some("PtNumber") {
                ParameterKind::Numeric
            } else {
                ParameterKind::Text
            }
        }
    })
}
fn logical_value(parameter: &EntityValue, kind: ParameterKind) -> Result<FieldValue, Error> {
    Ok(match kind {
        ParameterKind::Numeric => FieldValue::Decimal(
            decimal(parameter, "numericValue")?.ok_or_else(|| Error::new("Parameter has no numeric value"))?,
        ),
        ParameterKind::Indicator => match text(parameter, "symbolicValue").as_deref() {
            Some("Y") => FieldValue::Bool(true),
            Some("N") => FieldValue::Bool(false),
            _ => return Err(Error::new("Indicator must be Y or N")),
        },
        ParameterKind::Bits => {
            let raw = text(parameter, "parameterEnumId").ok_or_else(|| Error::new("Parameter has no bit value"))?;
            let bits = raw.strip_prefix("0b").unwrap_or(&raw).replace([' ', '_'], "");
            if bits.is_empty() || !bits.chars().all(|c| c == '0' || c == '1') {
                return Err(Error::new("Bit parameter must contain a binary string"));
            }
            FieldValue::Unsigned(u64::from_str_radix(&bits, 2).map_err(error)?)
        }
        ParameterKind::Text => FieldValue::Text(
            text(parameter, "symbolicValue")
                .or_else(|| text(parameter, "textValue"))
                .ok_or_else(|| Error::new("Parameter has no text value"))?,
        ),
    })
}
pub fn prepare(
    ec: &ExecutionContext,
    params: &Parameters,
) -> Result<(Arc<PreparedDeviceRequest>, RequestInput), Error> {
    let r = entity(params, "request")?;
    let ef = facade(ec)?;
    if ef.sim_active() {
        return Err(Error::new(
            "Device connector network I/O is disabled in simulation mode",
        ));
    }
    let device_id = text(&r, "deviceId").ok_or_else(|| Error::new("Request has no deviceId"))?;
    let device = entity(params, "device")?;
    if text(&device, "deviceId").as_deref() != Some(&device_id) {
        return Err(Error::new("Request and device do not match"));
    }
    let c = params
        .get("connection")
        .filter(|v| !matches!(v, Value::Null))
        .map(|_| entity(params, "connection"))
        .transpose()?;
    let operation = Operation::from_enum(&text(&r, "requestTypeEnumId").unwrap_or_default()).map_err(error)?;
    let mut request = PreparedDeviceRequest {
        name: text(&r, "requestName").ok_or_else(|| Error::new("Missing requestName"))?,
        device_id: device_id.clone(),
        parent_request: text(&r, "parentRequestName"),
        operation,
        connection: if operation == Operation::Unsubscribe {
            let parent =
                text(&r, "parentRequestName").ok_or_else(|| Error::new("Unsubscribe requires parentRequestName"))?;
            let parent = find(&ef, "moqui.device.DeviceRequest", "requestName", &parent, true)?;
            if text(&parent, "deviceId").as_deref() != Some(&device_id) {
                return Err(Error::new("Parent subscription belongs to another device"));
            }
            let parent_connection = text(&parent, "connectionName")
                .map(|name| find(&ef, "moqui.device.DeviceConnection", "connectionName", &name, true))
                .transpose()?;
            connection(&parent, parent_connection.as_ref(), &device_id)?
        } else {
            connection(&r, c.as_deref(), &device_id)?
        },
        query: text(&r, "query"),
        items: vec![],
        timeout: positive_ms(&r, "timeout", 5000)?,
        polling_interval: positive_ms(&r, "pollingInterval", 1000)?,
        logging: text(&r, "purposeEnumId").as_deref() == Some("DrpLogging"),
        plc_log: text(&r, "requestGroup").as_deref() == Some("plc-log")
            || (text(&r, "purposeEnumId").as_deref() == Some("DrpLogging")
                && params
                    .get("requestItemList")
                    .is_none_or(|v| matches!(v, Value::Null) || matches!(v,Value::List(l)if l.is_empty()))
                && params
                    .get("bulkRequestItemList")
                    .is_none_or(|v| matches!(v, Value::Null) || matches!(v,Value::List(l)if l.is_empty()))),
        only_changed: text(&r, "onlyChangedParameters").as_deref() != Some("N"),
    };
    let mut input = RequestInput::default();
    for (list_name, bulk) in [("bulkRequestItemList", true), ("requestItemList", false)] {
        let list = match params.get(list_name) {
            Some(Value::List(l)) => l.as_slice(),
            None | Some(Value::Null) => &[],
            _ => return Err(Error::new("Request items must be lists")),
        };
        for (index, v) in list.iter().enumerate() {
            let Value::Object(v) = v else {
                return Err(Error::new("Item must be EntityValue"));
            };
            let i = v
                .downcast::<EntityValue>()
                .ok_or_else(|| Error::new("Item must be EntityValue"))?;
            if text(&i, "requestName").as_deref() != Some(&request.name) {
                return Err(Error::new("Item belongs to another request"));
            }
            let id = text(&i, "parameterId").ok_or_else(|| Error::new("Item has no parameterId"))?;
            let p = find(&ef, "moqui.math.Parameter", "parameterId", &id, true)?;
            if text(&p, "deviceId").as_deref() != Some(&device_id) {
                return Err(Error::new("Parameter does not belong to the request device"));
            }
            let def = find(
                &ef,
                "moqui.math.ParameterDef",
                "parameterDefId",
                &text(&p, "parameterDefId").ok_or_else(|| Error::new("Parameter has no definition"))?,
                true,
            )?;
            let kind = kind(&ef, &def)?;
            let ty =
                PhysicalType::parse(&text(&i, "itemTypeEnumId").unwrap_or_else(|| "STRING".into())).map_err(error)?;
            let digits = decimal(&i, "significantDigits")?
                .map(|d| u32::try_from(d).map_err(error))
                .transpose()?;
            let codec = ValueCodec {
                physical_min: decimal(&i, "minItemValue")?,
                physical_max: decimal(&i, "maxItemValue")?,
                logical_min: decimal(&def, "minValue")?,
                logical_max: decimal(&def, "maxValue")?,
                scale: decimal(&i, "scalingFactor")?.unwrap_or(Decimal::ONE),
                offset: decimal(&i, "offsetValue")?.unwrap_or(Decimal::ZERO),
                digits,
                tolerance: decimal(&i, "tolerance")?.unwrap_or(Decimal::ZERO),
                reverse_logic: text(&i, "reverseLogic").as_deref() == Some("Y"),
                allow_duplicate: text(&i, "allowDuplicate").as_deref() == Some("Y"),
            };
            if operation == Operation::Write {
                input.values.insert(id.clone(), logical_value(&p, kind)?);
            }
            request.items.push(Item {
                parameter_id: id,
                name: text(&i, "requestItemName").unwrap_or_default(),
                address: text(&i, "query").or_else(|| request.query.clone()).unwrap_or_default(),
                physical_type: ty,
                kind,
                codec,
                bulk_index: if bulk && request.connection.protocol == Protocol::ModbusTcp {
                    Some(index)
                } else {
                    None
                },
            });
        }
    }
    if operation == Operation::Transfer {
        if let Some(Value::List(files)) = params.get("transferFiles") {
            for file in files {
                let Value::Map(file) = file else {
                    return Err(Error::new("Transfer file must be a map"));
                };
                let name = file
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::new("Transfer file requires name"))?;
                let bytes = match file.get("bytes") {
                    Some(Value::Bytes(bytes)) => bytes.clone(),
                    _ => return Err(Error::new("Transfer file requires binary bytes")),
                };
                input.files.push(TransferFile {
                    name: name.into(),
                    bytes,
                });
            }
        }
        if let Some(id) = params.get("deviceContentId").and_then(Value::as_str) {
            let content = find(&ef, "moqui.device.DeviceContent", "deviceContentId", id, false)?;
            if text(&content, "deviceId").as_deref() != Some(&device_id) {
                return Err(Error::new("DeviceContent belongs to a different device"));
            }
            let location = text(&content, "contentLocation")
                .ok_or_else(|| Error::new("DeviceContent has no resource location"))?;
            let name = location.rsplit('/').next().unwrap_or("").to_string();
            crate::backends::file::validate_name(&name).map_err(error)?;
            let bytes = ec.factory().resource().get(&location)?.read_bytes()?;
            input.files.push(TransferFile { name, bytes });
        }
    }
    request.validate().map_err(error)?;
    Ok((Arc::new(request), input))
}
pub fn run_request(ec: &ExecutionContext, _: &ServiceInfo<'_>, params: &Parameters) -> Result<Parameters, Error> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(Error::new(
            "Invoke the Moqui service on a service worker; async Rust callers should use DeviceConnectorFacade directly",
        ));
    }
    let (request, input) = prepare(ec, params)?;
    let factory = ec
        .factory()
        .get_tool_factory_typed::<DeviceConnectorToolFactory>("DeviceConnector")
        .ok_or_else(|| Error::new("Device connector ToolFactory is not installed"))?;
    let state = factory.state().map_err(error)?;
    let mut output = RequestOutput::default();
    let execution = state.runtime.block_on(state.facade.execute(
        request.clone(),
        &input,
        &mut output,
        request.deadline(),
        Some(state.sink.clone()),
    ));
    let mut status = execution.as_ref().copied().unwrap_or_else(|e| {
        if e.may_have_applied {
            ExecutionOutcome::Uncertain
        } else if !output.files.is_empty()
            || output
                .items
                .iter()
                .any(|i| matches!(i.status, ItemStatus::Completed | ItemStatus::Unchanged))
        {
            ExecutionOutcome::Partial
        } else {
            ExecutionOutcome::Failed
        }
    });
    // A finite read is persisted synchronously so success includes the database update.
    let mut persistence_error = None;
    if request.operation == Operation::Read {
        for i in &mut output.items {
            if i.status == ItemStatus::Completed
                && let Some(value) = &i.value
            {
                match persist_value(ec, &request, i, value) {
                    Ok(changed) => {
                        if !changed {
                            i.status = ItemStatus::Unchanged;
                        }
                    }
                    Err(e) => {
                        i.status = ItemStatus::Failed;
                        i.error = Some(e.to_string());
                        persistence_error = Some(e.to_string());
                    }
                }
            }
        }
        status = output.outcome();
    }
    let mut result = Parameters::new();
    result.insert("status".into(), Value::string(status.as_str()));
    result.insert("itemResults".into(),Value::List(output.items.iter().map(|i|Value::Map(indexmap::indexmap!{"parameterId".into()=>Value::string(&i.parameter_id),"status".into()=>Value::string(match i.status{ItemStatus::Completed=>"completed",ItemStatus::Unchanged=>"unchanged",ItemStatus::Failed=>"failed",ItemStatus::Uncertain=>"uncertain"}),"value".into()=>i.value.as_ref().map(to_value).unwrap_or(Value::Null),"error".into()=>i.error.as_ref().map(Value::string).unwrap_or(Value::Null)})).collect()));
    result.insert(
        "transferredFiles".into(),
        Value::List(output.files.iter().map(Value::string).collect()),
    );
    result.insert(
        "subscriptionId".into(),
        output.subscription_id.map(Value::string).unwrap_or(Value::Null),
    );
    result.insert(
        "errorMessage".into(),
        execution
            .err()
            .map(|e| Value::string(e.message))
            .or_else(|| persistence_error.map(Value::string))
            .unwrap_or(Value::Null),
    );
    Ok(result)
}
pub fn to_value(v: &FieldValue) -> Value {
    match v {
        FieldValue::Record(v) => Value::Map(v.iter().map(|(k, v)| (k.clone(), to_value(v))).collect()),
        FieldValue::Null => Value::Null,
        FieldValue::Bool(v) => Value::Bool(*v),
        FieldValue::Signed(v) => Value::I64(*v),
        FieldValue::Unsigned(v) => Value::Decimal(Decimal::from(*v)),
        FieldValue::Decimal(v) => Value::Decimal(*v),
        FieldValue::Float(v) => Value::F64(*v),
        FieldValue::Text(v) => Value::string(v),
        FieldValue::Bytes(v) => Value::Bytes(v.clone()),
        FieldValue::Array(v) => Value::List(v.iter().map(to_value).collect()),
    }
}
fn fields(p: &mut EntityValue, value: &FieldValue, kind: ParameterKind, width: usize) -> Result<(), Error> {
    match kind {
        ParameterKind::Numeric => p.set("numericValue", Value::Decimal(value.decimal().map_err(error)?)),
        ParameterKind::Indicator => {
            let FieldValue::Bool(v) = value else {
                return Err(Error::new("Indicator sample is not boolean"));
            };
            p.set("symbolicValue", Value::string(if *v { "Y" } else { "N" }));
        }
        ParameterKind::Bits => {
            let parsed;
            let v = match value {
                FieldValue::Unsigned(v) => v,
                FieldValue::Text(s) => {
                    let s = s.strip_prefix("0b").unwrap_or(s).replace([' ', '_'], "");
                    parsed = u64::from_str_radix(&s, 2).map_err(error)?;
                    &parsed
                }
                _ => return Err(Error::new("Bit sample is not unsigned or a binary string")),
            };
            if width < 64 && *v >= (1u64 << width) {
                return Err(Error::new("Bit sample exceeds declared width"));
            }
            p.set("parameterEnumId", Value::string(format!("{v:0width$b}")));
        }
        ParameterKind::Text => {
            let FieldValue::Text(v) = value else {
                return Err(Error::new("Text sample is not text"));
            };
            p.set("symbolicValue", Value::string(v));
        }
    }
    Ok(())
}
fn timestamp(time: SystemTime) -> Result<Value, Error> {
    let duration = time.duration_since(UNIX_EPOCH).map_err(error)?;
    Ok(Value::Timestamp(
        jiff::Timestamp::from_nanosecond(duration.as_nanos() as i128).map_err(error)?,
    ))
}
fn persist_value(
    ec: &ExecutionContext,
    r: &PreparedDeviceRequest,
    i: &ItemResult,
    value: &FieldValue,
) -> Result<bool, Error> {
    let item = r
        .items
        .iter()
        .find(|item| item.parameter_id == i.parameter_id)
        .ok_or_else(|| Error::new("Result has no configured request item"))?;
    persist_sample(
        ec,
        r.logging,
        &Sample {
            device_id: r.device_id.clone(),
            log_value_kind: None,
            parameter_id: Some(i.parameter_id.clone()),
            value: value.clone(),
            source_time: i.source_time,
            received_time: SystemTime::now(),
            quality: i.quality.clone(),
        },
        Some(item),
    )
}
fn persist_sample(ec: &ExecutionContext, logging: bool, s: &Sample, item: Option<&Item>) -> Result<bool, Error> {
    let ef = facade(ec)?;
    let transactions = ef.transactions_for(if s.parameter_id.is_some() {
        if logging {
            "moqui.math.ParameterLog"
        } else {
            "moqui.math.Parameter"
        }
    } else {
        "moqui.device.DeviceLog"
    })?;
    let tx = transactions.scope(None)?;
    find(&ef, "moqui.device.Device", "deviceId", &s.device_id, true)?;
    if let Some(id) = &s.parameter_id {
        let mut p = find(&ef, "moqui.math.Parameter", "parameterId", id, false)?;
        if text(&p, "deviceId").as_deref() != Some(&s.device_id) {
            return Err(Error::new("Sample Parameter belongs to another device"));
        }
        let def = find(
            &ef,
            "moqui.math.ParameterDef",
            "parameterDefId",
            &text(&p, "parameterDefId").ok_or_else(|| Error::new("Parameter has no definition"))?,
            true,
        )?;
        let kind = kind(&ef, &def)?;
        let width = item
            .map(|i| match i.physical_type {
                PhysicalType::U8 | PhysicalType::I8 => 8,
                PhysicalType::U32 | PhysicalType::I32 => 32,
                PhysicalType::U64 | PhysicalType::I64 => 64,
                _ => 16,
            })
            .unwrap_or(64);
        if !logging
            && let Some(item) = item
            && !item.codec.allow_duplicate
            && let Ok(old) = logical_value(&p, kind)
        {
            let same = if kind == ParameterKind::Numeric {
                s.value
                    .decimal()
                    .map_err(error)?
                    .checked_sub(old.decimal().map_err(error)?)
                    .ok_or_else(|| Error::new("Tolerance arithmetic overflow"))?
                    .abs()
                    <= item.codec.tolerance
            } else {
                s.value == old
            };
            if same {
                tx.commit()?;
                return Ok(false);
            }
        }
        if logging {
            let last = ef
                .find("moqui.math.ParameterLog")?
                .eq("parameterId", Value::string(id))
                .order_by("sequenceNum", true)
                .one()?;
            let seq = last
                .as_ref()
                .map(|p| decimal(p, "sequenceNum"))
                .transpose()?
                .flatten()
                .unwrap_or(Decimal::ZERO)
                .checked_add(Decimal::ONE)
                .ok_or_else(|| Error::new("Log sequence overflow"))?;
            let mut log = ef.make_value("moqui.math.ParameterLog")?;
            log.set_sequenced_id_primary(&ef)?;
            log.set("parameterId", Value::string(id));
            log.set("sequenceNum", Value::Decimal(seq));
            log.set("observedDate", timestamp(s.source_time.unwrap_or(s.received_time))?);
            match s.log_value_kind {
                Some(LogValueKind::Numeric) => {
                    log.set("numericValue", Value::Decimal(s.value.decimal().map_err(error)?))
                }
                Some(LogValueKind::Enumeration | LogValueKind::Message) => {
                    let FieldValue::Text(v) = &s.value else {
                        return Err(Error::new("PLC enum/message log must be text"));
                    };
                    log.set("symbolicValue", Value::string(v));
                }
                None => fields(&mut log, &s.value, kind, width)?,
            }
            log.create(&ef)?;
        } else {
            fields(&mut p, &s.value, kind, width)?;
            p.update(&ef)?;
        }
    } else {
        let mut log = ef.make_value("moqui.device.DeviceLog")?;
        log.set_sequenced_id_primary(&ef)?;
        log.set("deviceId", Value::string(&s.device_id));
        log.set(
            "payload",
            Value::string(serde_json::to_string(&crate::payload::to_json(&s.value).map_err(error)?).map_err(error)?),
        );
        log.set("observedDate", timestamp(s.source_time.unwrap_or(s.received_time))?);
        log.create(&ef)?;
    }
    tx.commit()?;
    Ok(true)
}
pub(crate) fn persist_batch(ec: &ExecutionContext, batch: &SampleBatch) -> Result<(), Error> {
    // Independent samples continue after an invalid row; every failure is observable.
    let mut failures = 0;
    for sample in &batch.samples {
        if let Err(e) = persist_sample(
            ec,
            batch.logging,
            sample,
            batch.request.as_ref().and_then(|r| {
                r.items
                    .iter()
                    .find(|i| Some(&i.parameter_id) == sample.parameter_id.as_ref())
            }),
        ) {
            failures += 1;
            tracing::error!(request=%batch.request_name,device=%sample.device_id,error=%e,"Field sample persistence failed");
        }
    }
    if failures > 0 {
        return Err(Error::new(format!("{failures} field samples were not saved")));
    }
    Ok(())
}
