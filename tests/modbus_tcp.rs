#![cfg(feature = "modbus")]
mod common;
use moqui_fieldbus::{
    backends::modbus::{Address, decode_words, encode_words},
    codec::ValueCodec,
    *,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
#[test]
fn address_bounds_and_register_orders() {
    for bad in [
        "holding-register:65535:UDINT",
        "holding-register:0:UINT[126]",
        "coil:0:UINT",
        "input-register:0:BOOL",
        "holding-register:0:UINT[0]",
    ] {
        assert!(Address::parse(bad).is_err(), "{bad}");
    }
    for ty in [PhysicalType::U64, PhysicalType::I64, PhysicalType::F64] {
        let v = match ty {
            PhysicalType::U64 => FieldValue::Unsigned(u64::MAX),
            PhysicalType::I64 => FieldValue::Signed(i64::MIN),
            _ => FieldValue::Float(-123.5),
        };
        for bytes in [false, true] {
            for words in [false, true] {
                assert_eq!(
                    decode_words(&encode_words(&v, ty, bytes, words).unwrap(), ty, bytes, words).unwrap(),
                    v
                );
            }
        }
    }
}
async fn server(delay: Duration, write: bool) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut header = [0; 7];
        stream.read_exact(&mut header).await.unwrap();
        assert_eq!(&header[2..4], &[0, 0]);
        assert_eq!(header[6], 1);
        let length = u16::from_be_bytes([header[4], header[5]]) as usize - 1;
        let mut pdu = vec![0; length];
        stream.read_exact(&mut pdu).await.unwrap();
        if write {
            assert_eq!(pdu, vec![6, 0, 2, 0, 123]);
        } else {
            assert_eq!(pdu, vec![3, 0, 2, 0, 4]);
        }
        tokio::time::sleep(delay).await;
        let reply = if write {
            pdu
        } else {
            vec![3, 8, 255, 255, 255, 255, 255, 255, 255, 255]
        };
        let len = (reply.len() + 1) as u16;
        header[4..6].copy_from_slice(&len.to_be_bytes());
        let _ = stream.write_all(&header).await;
        let _ = stream.write_all(&reply).await;
    });
    (address, task)
}
#[tokio::test]
async fn real_tcp_read_preserves_u64() {
    let (address, server) = server(Duration::ZERO, false).await;
    let r = Arc::new(common::request(Protocol::ModbusTcp, address, Operation::Read));
    let facade = Arc::new(FieldbusFacade::new());
    let mut out = RequestOutput::default();
    assert_eq!(
        facade
            .execute(r.clone(), &RequestInput::default(), &mut out, r.deadline(), None)
            .await
            .unwrap(),
        ExecutionOutcome::Completed
    );
    assert_eq!(out.items[0].value, Some(FieldValue::Decimal(u64::MAX.into())));
    facade.close().await;
    server.await.unwrap();
}
#[tokio::test]
async fn real_tcp_write_waits_for_echo() {
    let (address, server) = server(Duration::ZERO, true).await;
    let mut r = common::request(Protocol::ModbusTcp, address, Operation::Write);
    r.items[0].address = "holding-register:2:UINT".into();
    r.items[0].physical_type = PhysicalType::U16;
    r.items[0].codec = ValueCodec::default();
    let r = Arc::new(r);
    let input = RequestInput {
        values: std::collections::BTreeMap::from([("test-param".into(), FieldValue::Unsigned(123))]),
        ..Default::default()
    };
    let mut out = RequestOutput::default();
    let facade = Arc::new(FieldbusFacade::new());
    assert_eq!(
        facade
            .execute(r.clone(), &input, &mut out, r.deadline(), None)
            .await
            .unwrap(),
        ExecutionOutcome::Completed
    );
    server.await.unwrap();
    facade.close().await;
}
#[tokio::test]
async fn write_timeout_is_uncertain_and_not_retried() {
    let (address, server) = server(Duration::from_millis(100), true).await;
    let mut r = common::request(Protocol::ModbusTcp, address, Operation::Write);
    r.items[0].address = "holding-register:2:UINT".into();
    r.items[0].physical_type = PhysicalType::U16;
    let facade = Arc::new(FieldbusFacade::new());
    let mut out = RequestOutput::default();
    let input = RequestInput {
        values: std::collections::BTreeMap::from([("test-param".into(), FieldValue::Unsigned(123))]),
        ..Default::default()
    };
    let e = facade
        .execute(
            Arc::new(r),
            &input,
            &mut out,
            Instant::now() + Duration::from_millis(30),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::Timeout);
    assert!(e.may_have_applied);
    server.await.unwrap();
    facade.close().await;
}
#[tokio::test]
async fn bulk_items_share_one_tcp_response() {
    let (address, server) = server(Duration::ZERO, false).await;
    let mut r = common::request(Protocol::ModbusTcp, address, Operation::Read);
    r.items[0].address = "holding-register:2:UINT[4]".into();
    r.items[0].physical_type = PhysicalType::U16;
    r.items[0].bulk_index = Some(0);
    let mut second = r.items[0].clone();
    second.parameter_id = "second-param".into();
    second.bulk_index = Some(3);
    r.items.push(second);
    let r = Arc::new(r);
    let f = Arc::new(FieldbusFacade::new());
    let mut out = RequestOutput::default();
    assert_eq!(
        f.execute(r.clone(), &RequestInput::default(), &mut out, r.deadline(), None)
            .await
            .unwrap(),
        ExecutionOutcome::Completed
    );
    assert_eq!(out.items.len(), 2);
    for i in out.items {
        assert_eq!(i.value, Some(FieldValue::Decimal(65535u32.into())));
    }
    server.await.unwrap();
    f.close().await;
}
