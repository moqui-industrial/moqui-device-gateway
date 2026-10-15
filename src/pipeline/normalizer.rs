use crate::model::{InboundRow, PlcLogBatch, PlcLogDeviceRow, RequestContext, RequestItem};
use serde_json::Value;

pub struct Normalizer;

impl Normalizer {
    /// Normalizes raw payload (string, json, binary) into strongly-typed InboundRow list
    pub fn normalize_inbound_payload(
        context: &RequestContext,
        source_query: &str,
        payload_str: &str,
    ) -> Vec<InboundRow> {
        let trimmed = payload_str.trim();
        if trimmed.is_empty() {
            return Vec::new();
        }

        if let Ok(json_val) = serde_json::from_str::<Value>(trimmed) {
            match json_val {
                Value::Array(arr) => {
                    let mut rows = Vec::with_capacity(arr.len());
                    for (i, item) in arr.into_iter().enumerate() {
                        if let Some(row) = Self::normalize_json_element(context, source_query, item, i) {
                            rows.push(row);
                        }
                    }
                    rows
                }
                Value::Object(map) => {
                    if let Some(row) = Self::normalize_json_map(context, source_query, map, 0) {
                        vec![row]
                    } else {
                        Vec::new()
                    }
                }
                scalar => {
                    if let Some(row) = Self::normalize_scalar(context, source_query, &scalar, 0) {
                        vec![row]
                    } else {
                        Vec::new()
                    }
                }
            }
        } else {
            // Raw text scalar
            let scalar_val = Value::String(trimmed.to_string());
            if let Some(row) = Self::normalize_scalar(context, source_query, &scalar_val, 0) {
                vec![row]
            } else {
                Vec::new()
            }
        }
    }

    fn normalize_json_element(
        context: &RequestContext,
        source_query: &str,
        val: Value,
        index: usize,
    ) -> Option<InboundRow> {
        match val {
            Value::Object(map) => Self::normalize_json_map(context, source_query, map, index),
            scalar => Self::normalize_scalar(context, source_query, &scalar, index),
        }
    }

    fn normalize_json_map(
        context: &RequestContext,
        source_query: &str,
        map: serde_json::Map<String, Value>,
        index: usize,
    ) -> Option<InboundRow> {
        let parameter_id = map
            .get("parameterId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| {
                Self::resolve_request_item(context, source_query, index).map(|item| item.parameter_id.clone())
            });

        let parameter_id = parameter_id?;

        let numeric_value = map.get("numericValue").and_then(|v| v.as_f64());
        let symbolic_value = map.get("symbolicValue").and_then(|v| {
            if let Some(s) = v.as_str() {
                Some(s.to_string())
            } else if v.is_boolean() {
                Some(if v.as_bool().unwrap() { "Y".to_string() } else { "N".to_string() })
            } else if !v.is_null() {
                Some(v.to_string())
            } else {
                None
            }
        });
        let parameter_enum_id = map.get("parameterEnumId").and_then(|v| v.as_str()).map(|s| s.to_string());

        let sequence_num = map.get("sequenceNum").and_then(|v| v.as_i64());
        let parameter_log_id = map.get("parameterLogId").and_then(|v| v.as_str()).map(|s| s.to_string());

        Some(InboundRow {
            parameter_id,
            numeric_value,
            symbolic_value,
            parameter_enum_id,
            purpose_enum_id: context.purpose_enum_id.clone(),
            observed_date: Some(jiff::Timestamp::now()),
            sequence_num,
            parameter_log_id,
        })
    }

    fn normalize_scalar(
        context: &RequestContext,
        source_query: &str,
        val: &Value,
        index: usize,
    ) -> Option<InboundRow> {
        let item = Self::resolve_request_item(context, source_query, index)?;

        let mut numeric_value = None;
        let mut symbolic_value = None;

        match val {
            Value::Number(n) => {
                numeric_value = n.as_f64();
            }
            Value::Bool(b) => {
                symbolic_value = Some(if *b { "Y".to_string() } else { "N".to_string() });
            }
            Value::String(s) => {
                symbolic_value = Some(s.clone());
            }
            Value::Null => {}
            other => {
                symbolic_value = Some(other.to_string());
            }
        }

        Some(InboundRow {
            parameter_id: item.parameter_id.clone(),
            numeric_value,
            symbolic_value,
            parameter_enum_id: None,
            purpose_enum_id: context.purpose_enum_id.clone(),
            observed_date: Some(jiff::Timestamp::now()),
            sequence_num: None,
            parameter_log_id: None,
        })
    }

    fn resolve_request_item<'a>(
        context: &'a RequestContext,
        source_query: &str,
        index: usize,
    ) -> Option<&'a RequestItem> {
        if context.request_query.as_deref() == Some(source_query) {
            return context.items.get(index);
        }
        context
            .items
            .iter()
            .find(|item| item.target_query.as_deref() == Some(source_query))
    }

    /// Partitions CODESYS LoggerFacade batch format
    pub fn partition_plc_log_batch(payload_str: &str) -> Result<PlcLogBatch, String> {
        let trimmed = payload_str.trim();
        if trimmed.is_empty() {
            return Ok(PlcLogBatch {
                with_source: Vec::new(),
                without_source: Vec::new(),
            });
        }

        let root: Value = serde_json::from_str(trimmed)
            .map_err(|e| format!("Invalid JSON for PLC log batch: {}", e))?;

        let entries_map = match root {
            Value::Object(map) => map,
            Value::Array(arr) => {
                let mut map = serde_json::Map::new();
                for (i, v) in arr.into_iter().enumerate() {
                    map.insert(i.to_string(), v);
                }
                map
            }
            _ => return Err("PLC log batch payload must be JSON object or array".to_string()),
        };

        let mut with_source = Vec::new();
        let mut without_source = Vec::new();

        for (idx, (_, entry_val)) in entries_map.into_iter().enumerate() {
            let entry = match entry_val {
                Value::Object(map) => map,
                _ => continue,
            };

            let logger_name = entry
                .get("loggerName")
                .or_else(|| entry.get("deviceId"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();

            if logger_name.is_empty() {
                return Err("PLC log entry is missing loggerName/deviceId".to_string());
            }

            let source = entry
                .get("source")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();

            let log_event_date_str = entry.get("logEventDate").and_then(|v| v.as_str()).unwrap_or("");
            let observed_date = parse_plc_timestamp(log_event_date_str);

            let type_id = entry.get("type").and_then(|v| v.as_i64());

            if !source.is_empty() {
                let numeric_value = if type_id == Some(1) || type_id.is_none() {
                    entry.get("numericValue").and_then(|v| v.as_f64())
                } else {
                    None
                };

                let symbolic_value = if type_id == Some(2) {
                    entry.get("enumValue").and_then(|v| v.as_str()).map(|s| s.to_string())
                } else {
                    entry.get("message").and_then(|v| v.as_str()).map(|s| s.to_string())
                };

                with_source.push(InboundRow {
                    parameter_id: source,
                    numeric_value,
                    symbolic_value,
                    parameter_enum_id: None,
                    purpose_enum_id: Some("DrpLogging".to_string()),
                    observed_date,
                    sequence_num: None,
                    parameter_log_id: None,
                });
            } else {
                let sequence_num = entry
                    .get("sequenceNum")
                    .and_then(|v| v.as_i64())
                    .map(|n| n as i32)
                    .unwrap_or((idx + 1) as i32);

                let payload = serde_json::to_string(&entry).unwrap_or_default();

                without_source.push(PlcLogDeviceRow {
                    device_log_id: uuid::Uuid::new_v4().to_string(),
                    device_id: logger_name,
                    payload,
                    sequence_num,
                    observed_date,
                });
            }
        }

        Ok(PlcLogBatch {
            with_source,
            without_source,
        })
    }
}

/// Parses CODESYS timestamp: 'DT#yyyy-MM-dd-HH:mm:ss'
fn parse_plc_timestamp(s: &str) -> Option<jiff::Timestamp> {
    let clean = s.trim();
    if clean.is_empty() {
        return Some(jiff::Timestamp::now());
    }

    let without_prefix = clean.strip_prefix("DT#").unwrap_or(clean);
    // Format is yyyy-MM-dd-HH:mm:ss, change 3rd hyphen to 'T' for ISO8601
    // e.g. 2026-10-14-22:30:00 -> 2026-10-14T22:30:00Z
    if without_prefix.len() >= 19 {
        let mut iso = String::with_capacity(20);
        let _bytes = without_prefix.as_bytes();
        iso.push_str(&without_prefix[..10]);
        iso.push('T');
        iso.push_str(&without_prefix[11..19]);
        iso.push('Z');
        if let Ok(ts) = iso.parse::<jiff::Timestamp>() {
            return Some(ts);
        }
    }
    Some(jiff::Timestamp::now())
}
