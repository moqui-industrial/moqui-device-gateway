#![cfg(feature = "opcua")]
mod common;
use moqui_device_gateway::{
    backends::opcua::{OpcUaClient, from_variant, node_id, to_variant},
    *,
};
use opcua::types::{NodeId, Variant};
#[test]
fn typed_variants_preserve_widths_and_binary() {
    for (ty, v) in [
        (PhysicalType::U64, FieldValue::Unsigned(u64::MAX)),
        (PhysicalType::I64, FieldValue::Signed(i64::MIN)),
        (PhysicalType::F32, FieldValue::Float(1.25)),
        (PhysicalType::Bool, FieldValue::Bool(true)),
        (PhysicalType::Bytes, FieldValue::Bytes(vec![0, 255, 128])),
        (PhysicalType::Text, FieldValue::Text("temperature".into())),
    ] {
        assert_eq!(from_variant(&to_variant(&v, ty).unwrap()).unwrap(), v);
    }
    assert!(matches!(
        to_variant(&FieldValue::Unsigned(1), PhysicalType::U16).unwrap(),
        Variant::UInt16(1)
    ));
    assert!(to_variant(&FieldValue::Unsigned(65536), PhysicalType::U16).is_err());
}
#[test]
fn no_inferred_namespace() {
    assert_eq!(node_id("ns=2;s=Temperature").unwrap(), NodeId::new(2, "Temperature"));
    assert_eq!(node_id("RAW(ns=2;i=10)").unwrap(), NodeId::new(2, 10u32));
    assert!(node_id("Temperature").is_err());
}
#[tokio::test]
async fn credentials_require_encryption_before_connecting() {
    let mut r = common::request(Protocol::OpcUa, "opc.tcp://127.0.0.1:9".into(), Operation::Read);
    r.items[0].address = "ns=2;s=Temperature".into();
    r.connection.credentials.username = Some("operator".into());
    r.connection.credentials.password = Some("secret".into());
    let e = OpcUaClient::default()
        .execute(
            &r,
            &RequestInput::default(),
            &mut RequestOutput::default(),
            r.deadline(),
        )
        .await
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::Invalid);
    assert!(!e.message.contains("secret"));
}
