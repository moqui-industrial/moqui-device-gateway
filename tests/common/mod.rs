#![allow(dead_code)]
use moqui_fieldbus::{codec::ValueCodec, model::*};
use std::{collections::BTreeMap, time::Duration};
pub fn request(protocol: Protocol, endpoint: String, operation: Operation) -> PreparedDeviceRequest {
    PreparedDeviceRequest {
        name: "test-request".into(),
        device_id: "test-device".into(),
        parent_request: None,
        operation,
        connection: Connection {
            name: "test-connection".into(),
            protocol,
            endpoint,
            options: BTreeMap::new(),
            credentials: Credentials::default(),
        },
        query: None,
        items: vec![Item {
            parameter_id: "test-param".into(),
            name: "temperature".into(),
            address: "holding-register:2:ULINT".into(),
            physical_type: PhysicalType::U64,
            kind: ParameterKind::Numeric,
            codec: ValueCodec::default(),
            bulk_index: None,
        }],
        timeout: Duration::from_millis(500),
        polling_interval: Duration::from_millis(20),
        logging: false,
        plc_log: false,
        only_changed: false,
    }
}
