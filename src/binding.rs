/*
 * This software is in the public domain under CC0 1.0 Universal plus a
 * Grant of Patent License.
 *
 * To the extent possible under law, the author(s) have dedicated all
 * copyright and related and neighboring rights to this software to the
 * public domain worldwide. This software is distributed without any
 * warranty.
 *
 * You should have received a copy of the CC0 Public Domain Dedication
 * along with this software (see the LICENSE.md file). If not, see
 * <http://creativecommons.org/publicdomain/zero/1.0/>.
 */

//! Moqui-specific preparation and persistence. No facade is passed into transport code.
use crate::{codec::ValueCodec, model::*, tool::DeviceGatewayToolFactory};
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
/// An entity value as a service receives it: the EntityValue of a Rust caller, or the value that a script or XML
/// actions share with their context (a Mutex around it). The request is prepared from a copy.
fn entity_value(v: &Value) -> Option<Arc<EntityValue>> {
    let Value::Object(object) = v else {
        return None;
    };
    object.downcast::<EntityValue>().or_else(|| {
        object
            .downcast::<std::sync::Mutex<EntityValue>>()
            .map(|cell| Arc::new(moqui_lib::sync::lock(&cell).clone()))
    })
}
fn entity(params: &Parameters, key: &str) -> Result<Arc<EntityValue>, Error> {
    match params.get(key) {
        None | Some(Value::Null) => Err(Error::new(format!("Missing {key}"))),
        Some(v) => entity_value(v).ok_or_else(|| Error::new(format!("{key} must be EntityValue"))),
    }
}
/// A list of entity values: a list, the list of a script, or a find result.
fn entity_list(params: &Parameters, key: &str) -> Result<Vec<Arc<EntityValue>>, Error> {
    match params.get(key) {
        None | Some(Value::Null) => Ok(vec![]),
        Some(Value::List(l)) => l
            .iter()
            .map(|v| entity_value(v).ok_or_else(|| Error::new("Item must be EntityValue")))
            .collect(),
        Some(v @ Value::Object(object)) => {
            if let Some(values) = moqui_core::script::script_list_values(v) {
                return Ok(values.into_iter().map(Arc::new).collect());
            }
            object
                .downcast::<moqui_core::entity::EntityList>()
                .map(|list| list.iter().cloned().map(Arc::new).collect())
                .ok_or_else(|| Error::new(format!("{key} must be a list of entity values")))
        }
        Some(_) => Err(Error::new(format!("{key} must be a list of entity values"))),
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
/// The protocol service a request runs through. The endpoint of the request selects the protocol; each service runs
/// only its own, so a DeviceRequest.runServiceName that does not match its endpoint is an error, not a silent switch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceKind {
    Mqtt,
    OpcUa,
    Modbus,
    File,
    PlcLog,
}
impl ServiceKind {
    fn accepts(self, protocol: Protocol) -> bool {
        match self {
            Self::Mqtt | Self::PlcLog => protocol == Protocol::Mqtt,
            Self::OpcUa => protocol == Protocol::OpcUa,
            Self::Modbus => protocol == Protocol::ModbusTcp,
            Self::File => matches!(protocol, Protocol::File | Protocol::Sftp),
        }
    }
    fn service_name(self) -> &'static str {
        match self {
            Self::Mqtt => "MqttServices",
            Self::OpcUa => "OpcUaServices",
            Self::Modbus => "ModbusServices",
            Self::File => "FileServices",
            Self::PlcLog => "PlcLogServices",
        }
    }
}
pub fn prepare(
    ec: &ExecutionContext,
    params: &Parameters,
    service: ServiceKind,
) -> Result<(Arc<PreparedDeviceRequest>, RequestInput), Error> {
    let r = entity(params, "request")?;
    let ef = facade(ec)?;
    if ef.sim_active() {
        return Err(Error::new("Device gateway network I/O is disabled in simulation mode"));
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
    let mut operation = Operation::from_enum(&text(&r, "requestTypeEnumId").unwrap_or_default()).map_err(error)?;
    let name = text(&r, "requestName").ok_or_else(|| Error::new("Missing requestName"))?;
    let connection = if operation == Operation::Unsubscribe {
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
    };
    if !service.accepts(connection.protocol) {
        return Err(Error::new(format!(
            "Request {name} has a {:?} endpoint; moqui.device.gateway.{}.run#DeviceRequest does not run it",
            connection.protocol,
            service.service_name()
        )));
    }
    if service == ServiceKind::PlcLog && !matches!(operation, Operation::Subscribe | Operation::Unsubscribe) {
        return Err(Error::new(
            "A PLC log request is a subscription (DrtSubscribe or DrtEvent) or DrtUnsubscribe",
        ));
    }
    // A recipe export (DrtConfigWrite) to a file or SFTP endpoint writes files.
    if operation == Operation::Write && matches!(connection.protocol, Protocol::File | Protocol::Sftp) {
        operation = Operation::Transfer;
    }
    let mut request = PreparedDeviceRequest {
        name,
        device_id: device_id.clone(),
        parent_request: text(&r, "parentRequestName"),
        operation,
        connection,
        query: text(&r, "query"),
        items: vec![],
        timeout: positive_ms(&r, "timeout", 5000)?,
        polling_interval: positive_ms(&r, "pollingInterval", 1000)?,
        logging: text(&r, "purposeEnumId").as_deref() == Some("DrpLogging"),
        plc_log: service == ServiceKind::PlcLog,
        only_changed: text(&r, "onlyChangedParameters").as_deref() != Some("N"),
    };
    let mut input = RequestInput::default();
    for (list_name, bulk) in [("bulkRequestItemList", true), ("requestItemList", false)] {
        for (index, i) in entity_list(params, list_name)?.into_iter().enumerate() {
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
                let bytes = match (file.get("bytes"), file.get("content")) {
                    (Some(Value::Bytes(bytes)), _) => bytes.clone(),
                    (_, Some(Value::String(content))) => content.as_bytes().to_vec(),
                    _ => return Err(Error::new("Transfer file requires binary bytes or text content")),
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
    match params.get("payload") {
        None | Some(Value::Null) => {}
        Some(payload @ Value::Map(_)) => {
            if request.operation != Operation::Write || request.connection.protocol != Protocol::Mqtt {
                return Err(Error::new("A payload is sent only by an MQTT write (DrtWrite)"));
            }
            if !request.items.is_empty() {
                return Err(Error::new(
                    "A write sends either a payload or its request items, not both",
                ));
            }
            input.payload = Some(serde_json::to_vec(&moqui_lib::json::to_json(payload)).map_err(error)?);
        }
        Some(_) => return Err(Error::new("payload must be a map")),
    }
    request.validate().map_err(error)?;
    Ok((Arc::new(request), input))
}
/// moqui.device.gateway.MqttServices.run#DeviceRequest
pub fn run_mqtt_request(ec: &ExecutionContext, _: &ServiceInfo<'_>, params: &Parameters) -> Result<Parameters, Error> {
    run_request(ec, params, ServiceKind::Mqtt)
}
/// moqui.device.gateway.OpcUaServices.run#DeviceRequest
pub fn run_opcua_request(ec: &ExecutionContext, _: &ServiceInfo<'_>, params: &Parameters) -> Result<Parameters, Error> {
    run_request(ec, params, ServiceKind::OpcUa)
}
/// moqui.device.gateway.ModbusServices.run#DeviceRequest
pub fn run_modbus_request(
    ec: &ExecutionContext,
    _: &ServiceInfo<'_>,
    params: &Parameters,
) -> Result<Parameters, Error> {
    run_request(ec, params, ServiceKind::Modbus)
}
/// moqui.device.gateway.FileServices.run#DeviceRequest, for file and SFTP endpoints.
pub fn run_file_request(ec: &ExecutionContext, _: &ServiceInfo<'_>, params: &Parameters) -> Result<Parameters, Error> {
    run_request(ec, params, ServiceKind::File)
}
/// moqui.device.gateway.PlcLogServices.run#DeviceRequest: an MQTT subscription to PLC log events.
pub fn run_plc_log_request(
    ec: &ExecutionContext,
    _: &ServiceInfo<'_>,
    params: &Parameters,
) -> Result<Parameters, Error> {
    run_request(ec, params, ServiceKind::PlcLog)
}
fn run_request(ec: &ExecutionContext, params: &Parameters, service: ServiceKind) -> Result<Parameters, Error> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(Error::new(
            "Invoke the Moqui service on a service worker; async Rust callers should use DeviceGatewayFacade directly",
        ));
    }
    let (request, input) = prepare(ec, params, service)?;
    let factory = ec
        .factory()
        .get_tool_factory_typed::<DeviceGatewayToolFactory>("DeviceGateway")
        .ok_or_else(|| Error::new("Device gateway ToolFactory is not installed"))?;
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
    // A finite read is stored synchronously so success includes the database update.
    let mut persistence_error = None;
    if request.operation == Operation::Read {
        let read: Vec<usize> = (0..output.items.len())
            .filter(|&n| output.items[n].status == ItemStatus::Completed && output.items[n].value.is_some())
            .collect();
        let samples: Vec<Sample> = read
            .iter()
            .map(|&n| {
                let i = &output.items[n];
                Sample {
                    device_id: request.device_id.clone(),
                    log_value_kind: None,
                    parameter_id: Some(i.parameter_id.clone()),
                    value: i.value.clone().unwrap_or(FieldValue::Null),
                    source_time: i.source_time,
                    received_time: SystemTime::now(),
                    quality: i.quality.clone(),
                }
            })
            .collect();
        if !samples.is_empty() {
            match call_store(ec, STORE_SAMPLES, &request.name, request.logging, &samples) {
                Ok(out) => {
                    let results = match out.get("results") {
                        Some(Value::List(l)) => l.clone(),
                        _ => vec![],
                    };
                    for (n, result) in read.iter().zip(&results) {
                        let item = &mut output.items[*n];
                        match map_text(result, "status").as_deref() {
                            Some("unchanged") => item.status = ItemStatus::Unchanged,
                            Some("failed") => {
                                item.status = ItemStatus::Failed;
                                item.error = map_text(result, "error");
                                persistence_error = item.error.clone();
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    for n in &read {
                        output.items[*n].status = ItemStatus::Failed;
                        output.items[*n].error = Some(e.to_string());
                    }
                    persistence_error = Some(e.to_string());
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
/// The service that stores the samples of a request. A SECA on it runs after each store.
pub const STORE_SAMPLES: &str = "moqui.device.gateway.InboundServices.store#Samples";
/// The service that stores the events of a PLC log subscription.
pub const STORE_PLC_LOG: &str = "moqui.device.gateway.InboundServices.store#PlcLog";
/// How a request item stores its values: duplicates, numeric tolerance and the bit width of bit sets.
#[derive(Clone, Copy)]
struct StorePolicy {
    allow_duplicate: bool,
    tolerance: Decimal,
    width: usize,
}
fn store_policy(ef: &EntityFacade, request_name: &str, parameter_id: &str) -> Result<Option<StorePolicy>, Error> {
    ef.find("moqui.device.DeviceRequestItem")?
        .eq("requestName", Value::string(request_name))
        .eq("parameterId", Value::string(parameter_id))
        .use_cache(true)
        .one()?
        .map(|i| {
            let physical_type =
                PhysicalType::parse(&text(&i, "itemTypeEnumId").unwrap_or_else(|| "STRING".into())).map_err(error)?;
            Ok(StorePolicy {
                allow_duplicate: text(&i, "allowDuplicate").as_deref() == Some("Y"),
                tolerance: decimal(&i, "tolerance")?.unwrap_or(Decimal::ZERO),
                width: match physical_type {
                    PhysicalType::U8 | PhysicalType::I8 => 8,
                    PhysicalType::U32 | PhysicalType::I32 => 32,
                    PhysicalType::U64 | PhysicalType::I64 => 64,
                    _ => 16,
                },
            })
        })
        .transpose()
}
fn persist_sample(
    ec: &ExecutionContext,
    logging: bool,
    s: &Sample,
    policy: Option<StorePolicy>,
) -> Result<bool, Error> {
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
        let width = policy.map_or(64, |p| p.width);
        if !logging
            && let Some(policy) = policy
            && !policy.allow_duplicate
            && let Ok(old) = logical_value(&p, kind)
        {
            let same = if kind == ParameterKind::Numeric {
                s.value
                    .decimal()
                    .map_err(error)?
                    .checked_sub(old.decimal().map_err(error)?)
                    .ok_or_else(|| Error::new("Tolerance arithmetic overflow"))?
                    .abs()
                    <= policy.tolerance
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
fn optional_text(value: Option<&String>) -> Value {
    value.map(Value::string).unwrap_or(Value::Null)
}
fn sample_to_value(s: &Sample) -> Result<Value, Error> {
    Ok(Value::Map(indexmap::indexmap! {
        "deviceId".into() => Value::string(&s.device_id),
        "parameterId".into() => optional_text(s.parameter_id.as_ref()),
        "value".into() => to_value(&s.value),
        "sourceTime".into() => s.source_time.map(timestamp).transpose()?.unwrap_or(Value::Null),
        "receivedTime".into() => timestamp(s.received_time)?,
        "quality".into() => optional_text(s.quality.as_ref()),
        "logValueKind".into() => match s.log_value_kind {
            Some(LogValueKind::Numeric) => Value::string("numeric"),
            Some(LogValueKind::Enumeration) => Value::string("enumeration"),
            Some(LogValueKind::Message) => Value::string("message"),
            None => Value::Null,
        },
    }))
}
/// The inverse of [`to_value`]. A Value has no unsigned integer: a whole non-negative decimal comes back as Unsigned,
/// which every numeric and bit-set conversion accepts.
fn field_value(v: &Value) -> Result<FieldValue, Error> {
    Ok(match v {
        Value::Null => FieldValue::Null,
        Value::Bool(v) => FieldValue::Bool(*v),
        Value::I32(v) => FieldValue::Signed(i64::from(*v)),
        Value::I64(v) => FieldValue::Signed(*v),
        Value::F64(v) => FieldValue::Float(*v),
        Value::Decimal(d) if d.fract().is_zero() && !d.is_sign_negative() && u64::try_from(*d).is_ok() => {
            FieldValue::Unsigned(u64::try_from(*d).map_err(error)?)
        }
        Value::Decimal(d) => FieldValue::Decimal(*d),
        Value::String(v) => FieldValue::Text(v.clone()),
        Value::Bytes(v) => FieldValue::Bytes(v.clone()),
        Value::List(v) => FieldValue::Array(v.iter().map(field_value).collect::<Result<_, _>>()?),
        Value::Map(v) => FieldValue::Record(
            v.iter()
                .map(|(k, v)| Ok((k.clone(), field_value(v)?)))
                .collect::<Result<_, Error>>()?,
        ),
        _ => return Err(Error::new("Sample value has an unsupported type")),
    })
}
fn map_text(v: &Value, key: &str) -> Option<String> {
    match v {
        Value::Map(m) => m
            .get(key)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(String::from),
        _ => None,
    }
}
fn sample_from_value(v: &Value) -> Result<Sample, Error> {
    let Value::Map(m) = v else {
        return Err(Error::new("A sample must be a map"));
    };
    let time = |key: &str| -> Result<Option<SystemTime>, Error> {
        match m.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::Timestamp(t)) => Ok(Some(SystemTime::from(*t))),
            Some(_) => Err(Error::new(format!("{key} must be a timestamp"))),
        }
    };
    Ok(Sample {
        device_id: map_text(v, "deviceId").ok_or_else(|| Error::new("A sample requires deviceId"))?,
        log_value_kind: match map_text(v, "logValueKind").as_deref() {
            None => None,
            Some("numeric") => Some(LogValueKind::Numeric),
            Some("enumeration") => Some(LogValueKind::Enumeration),
            Some("message") => Some(LogValueKind::Message),
            Some(_) => return Err(Error::new("logValueKind must be numeric, enumeration or message")),
        },
        parameter_id: map_text(v, "parameterId"),
        value: field_value(m.get("value").unwrap_or(&Value::Null))?,
        source_time: time("sourceTime")?,
        received_time: time("receivedTime")?.unwrap_or_else(SystemTime::now),
        quality: map_text(v, "quality"),
    })
}
/// moqui.device.gateway.InboundServices.store#Samples and store#PlcLog: stores each sample in its own transaction, so
/// one invalid sample does not lose the others. Results follow the order of the samples.
pub fn store_samples(ec: &ExecutionContext, _: &ServiceInfo<'_>, params: &Parameters) -> Result<Parameters, Error> {
    let request_name = params.get("requestName").and_then(Value::as_str).unwrap_or_default();
    let logging = match params.get("logging") {
        Some(Value::Bool(v)) => *v,
        Some(Value::String(v)) => v == "true" || v == "Y",
        _ => false,
    };
    let samples = match params.get("samples") {
        Some(Value::List(l)) => l.as_slice(),
        None | Some(Value::Null) => &[],
        _ => return Err(Error::new("samples must be a list")),
    };
    let ef = facade(ec)?;
    let (mut stored, mut failed) = (0i64, 0i64);
    let mut results = Vec::with_capacity(samples.len());
    for v in samples {
        let outcome = sample_from_value(v).and_then(|s| {
            let policy = match &s.parameter_id {
                Some(id) if !request_name.is_empty() => store_policy(&ef, request_name, id)?,
                _ => None,
            };
            persist_sample(ec, logging, &s, policy)
        });
        let (status, message) = match outcome {
            Ok(true) => {
                stored += 1;
                ("stored", None)
            }
            Ok(false) => ("unchanged", None),
            Err(e) => {
                failed += 1;
                tracing::error!(request=%request_name,error=%e,"Field sample persistence failed");
                ("failed", Some(e.to_string()))
            }
        };
        results.push(Value::Map(indexmap::indexmap! {
            "parameterId".into() => optional_text(map_text(v, "parameterId").as_ref()),
            "status".into() => Value::string(status),
            "error".into() => optional_text(message.as_ref()),
        }));
    }
    let mut out = Parameters::new();
    out.insert("results".into(), Value::List(results));
    out.insert("storedCount".into(), Value::I64(stored));
    out.insert("failedCount".into(), Value::I64(failed));
    Ok(out)
}
/// Calls a store service with the samples and returns its results; an error the service recorded is an error here.
fn call_store(
    ec: &ExecutionContext,
    service: &str,
    request_name: &str,
    logging: bool,
    samples: &[Sample],
) -> Result<Parameters, Error> {
    let mut params = Parameters::new();
    params.insert("requestName".into(), Value::string(request_name));
    params.insert("logging".into(), Value::Bool(logging));
    params.insert(
        "samples".into(),
        Value::List(samples.iter().map(sample_to_value).collect::<Result<_, _>>()?),
    );
    let mark = ec.message().error_mark();
    let out = moqui_core::service::sync(service, &params)?;
    let errors = ec.message().errors_since(mark);
    if !errors.is_empty() {
        return Err(Error::new(errors.join("\n")));
    }
    Ok(out)
}
/// The persistence worker's path: subscription and polling samples go through the store services.
pub(crate) fn store_batch(ec: &ExecutionContext, batch: &SampleBatch) -> Result<(), Error> {
    let plc_log = batch.request.as_ref().is_some_and(|r| r.plc_log);
    let out = call_store(
        ec,
        if plc_log { STORE_PLC_LOG } else { STORE_SAMPLES },
        &batch.request_name,
        batch.logging,
        &batch.samples,
    )?;
    match out.get("failedCount") {
        Some(Value::I64(n)) if *n > 0 => Err(Error::new(format!("{n} field samples were not saved"))),
        _ => Ok(()),
    }
}
/// moqui.device.gateway.SubscriptionServices.get#SubscriptionState: the requests with a running subscription or polling
/// task, those whose task ended without an unsubscribe and, with claimStartup, whether this call is the first claim of
/// the startup restoration in this process.
pub fn subscription_state(
    ec: &ExecutionContext,
    _: &ServiceInfo<'_>,
    params: &Parameters,
) -> Result<Parameters, Error> {
    let factory = ec
        .factory()
        .get_tool_factory_typed::<DeviceGatewayToolFactory>("DeviceGateway")
        .ok_or_else(|| Error::new("Device gateway ToolFactory is not installed"))?;
    let state = factory.state().map_err(error)?;
    let (active, dropped) = state.runtime.block_on(state.facade.subscription_names());
    let claim = match params.get("claimStartup") {
        Some(Value::Bool(v)) => *v,
        Some(Value::String(v)) => v == "true" || v == "Y",
        _ => false,
    };
    let claimed = claim && !state.startup_claimed.swap(true, std::sync::atomic::Ordering::AcqRel);
    let mut out = Parameters::new();
    out.insert(
        "activeRequestNames".into(),
        Value::List(active.iter().map(Value::string).collect()),
    );
    out.insert(
        "droppedRequestNames".into(),
        Value::List(dropped.iter().map(Value::string).collect()),
    );
    out.insert("startupClaimed".into(), Value::Bool(claimed));
    Ok(out)
}
