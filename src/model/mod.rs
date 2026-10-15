use serde::{Deserialize, Serialize};

/// Status of the gateway logical & physical identity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayIdentityStatus {
    pub gateway_device_id: String,
    pub configured: boolean_alias::Bool,
    pub physical_device_exists: bool,
    pub gateway_member_found: bool,
    pub message: String,
}

impl GatewayIdentityStatus {
    pub fn ready(&self) -> bool {
        self.configured.0 && self.physical_device_exists && self.gateway_member_found
    }
}

pub mod boolean_alias {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Bool(pub bool);
    impl Serialize for Bool {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_bool(self.0)
        }
    }
    impl<'de> Deserialize<'de> for Bool {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let b = bool::deserialize(deserializer)?;
            Ok(Bool(b))
        }
    }
}

/// RequestContext mirrors the Moqui DEVICE_REQUEST + DEVICE_CONNECTION metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestContext {
    pub request_name: String,
    pub device_id: Option<String>,
    pub parent_request_name: Option<String>,
    pub request_type_enum_id: String,
    pub purpose_enum_id: Option<String>,
    pub router_enum_id: Option<String>,
    pub connection_name: Option<String>,
    pub broker_uri: Option<String>,
    pub timeout: Option<i32>,
    pub polling_interval: Option<i32>,
    pub request_query: Option<String>,
    pub only_changed_parameters: Option<String>,
    pub driver_enum_id: Option<String>,
    pub driver_enum_code: Option<String>,
    pub transport_enum_id: Option<String>,
    pub transport_enum_code: Option<String>,
    pub transport_config: Option<String>,
    pub options: Option<String>,
    pub items: Vec<RequestItem>,
}

impl RequestContext {
    pub fn is_opcua(&self) -> bool {
        if let Some(driver) = &self.driver_enum_id {
            if driver == "DcdOpcUa" {
                return true;
            }
        }
        if let Some(broker) = &self.broker_uri {
            if broker.starts_with("opcua:")
                || broker.starts_with("milo-client:")
                || broker.starts_with("opc.tcp://")
            {
                return true;
            }
        }
        false
    }

    pub fn is_modbus(&self) -> bool {
        if let Some(driver) = &self.driver_enum_id {
            if driver == "DcdModbus" {
                return true;
            }
        }
        if let Some(broker) = &self.broker_uri {
            if broker.starts_with("modbus:") || broker.starts_with("modbus.tcp://") {
                return true;
            }
        }
        false
    }

    pub fn is_plc_log(&self) -> bool {
        self.purpose_enum_id.as_deref() == Some("DrpLogging")
    }

    #[allow(dead_code)]
    pub fn is_subscribe_type(&self) -> bool {
        matches!(
            self.request_type_enum_id.as_str(),
            "DrtSubscribe" | "DrtEvent" | "DrtStateChange" | "DrtCyclic"
        )
    }
}

/// RequestItem mirrors DEVICE_REQUEST_ITEM + PARAMETER target query & values
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestItem {
    pub parameter_id: String,
    pub request_item_name: Option<String>,
    pub sequence_num: Option<i32>,
    pub numeric_value: Option<f64>,
    pub symbolic_value: Option<String>,
    pub parameter_enum_id: Option<String>,
    pub target_query: Option<String>,
}

/// Normalized Inbound Row ready to be stored into PARAMETER or PARAMETER_LOG
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboundRow {
    pub parameter_id: String,
    pub numeric_value: Option<f64>,
    pub symbolic_value: Option<String>,
    pub parameter_enum_id: Option<String>,
    pub purpose_enum_id: Option<String>,
    pub observed_date: Option<jiff::Timestamp>,
    pub sequence_num: Option<i64>,
    pub parameter_log_id: Option<String>,
}

/// CODESYS LoggerFacade batch partitioned output
#[derive(Debug, Clone)]
pub struct PlcLogBatch {
    pub with_source: Vec<InboundRow>,
    pub without_source: Vec<PlcLogDeviceRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlcLogDeviceRow {
    pub device_log_id: String,
    pub device_id: String,
    pub payload: String,
    pub sequence_num: i32,
    pub observed_date: Option<jiff::Timestamp>,
}

/// Exported Recipe File Specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportFileSpec {
    pub filename: String,
    pub content: String,
    pub priority: i32,
    pub row_count: usize,
}
