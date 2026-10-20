#![cfg(feature = "mqtt")]
mod common;
use moqui_device_connector::*;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
async fn packet(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let header = stream.read_u8().await.unwrap();
    let mut length = 0usize;
    let mut multiplier = 1;
    loop {
        let n = stream.read_u8().await.unwrap();
        length += (n as usize & 127) * multiplier;
        if n < 128 {
            break;
        }
        multiplier *= 128;
        assert!(multiplier <= 128 * 128 * 128);
    }
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.unwrap();
    (header, body)
}
async fn send(stream: &mut TcpStream, header: u8, body: &[u8]) {
    let mut packet = vec![header];
    let mut n = body.len();
    loop {
        let mut byte = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            byte |= 128;
        }
        packet.push(byte);
        if n == 0 {
            break;
        }
    }
    packet.extend(body);
    stream.write_all(&packet).await.unwrap();
}
async fn broker(write: bool, reject: bool) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ep = format!("mqtt://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        s.set_nodelay(true).unwrap();
        let (header, body) = packet(&mut s).await;
        assert_eq!(header, 0x10);
        assert_eq!(&body[..7], &[0, 4, b'M', b'Q', b'T', b'T', 5]);
        send(&mut s, 0x20, &[0, 0, 0]).await;
        let (header, body) = packet(&mut s).await;
        if write {
            assert_eq!(header & 0xf6, 0x32);
            let len = u16::from_be_bytes([body[0], body[1]]) as usize;
            assert_eq!(&body[2..2 + len], b"/test/topic");
            let id = &body[2 + len..4 + len];
            assert_eq!(body[4 + len], 0);
            let value: serde_json::Value = serde_json::from_slice(&body[5 + len..]).unwrap();
            assert_eq!(value["parameterId"], "test-param");
            assert_eq!(value["numericValue"].as_u64(), Some(u64::MAX));
            assert_eq!(value["temperature"].as_u64(), Some(u64::MAX));
            if reject {
                send(&mut s, 0x40, &[id[0], id[1], 0x87, 0]).await;
            } else {
                send(&mut s, 0x40, id).await;
            }
        } else {
            assert_eq!(header, 0x82);
            let id = &body[..2];
            assert_eq!(body[2], 0);
            let topic = b"/test/topic";
            let mut publish = vec![0, topic.len() as u8];
            publish.extend(topic);
            publish.push(0);
            publish.extend(br#"{"temperature":18446744073709551615}"#);
            // Retained data can arrive while subscription setup is still in progress.
            send(&mut s, 0x30, &publish).await;
            send(&mut s, 0x90, &[id[0], id[1], 0, 1]).await;
        }
        let _ = tokio::time::timeout(Duration::from_millis(500), s.read_u8()).await;
    });
    (ep, task)
}
fn request(endpoint: String, operation: Operation) -> Arc<PreparedDeviceRequest> {
    let mut r = common::request(Protocol::Mqtt, endpoint, operation);
    r.items[0].address = "/test/topic".into();
    Arc::new(r)
}
#[tokio::test]
async fn qos1_publication_preserves_gateway_envelope() {
    let (ep, server) = broker(true, false).await;
    let r = request(ep, Operation::Write);
    let f = Arc::new(DeviceConnectorFacade::new());
    let input = RequestInput {
        values: std::collections::BTreeMap::from([("test-param".into(), FieldValue::Unsigned(u64::MAX))]),
        ..Default::default()
    };
    let mut out = RequestOutput::default();
    assert_eq!(
        f.execute(r.clone(), &input, &mut out, r.deadline(), None)
            .await
            .unwrap(),
        ExecutionOutcome::Completed
    );
    server.await.unwrap();
    f.close().await;
}
#[tokio::test]
async fn rejected_puback_cannot_report_success() {
    let (ep, server) = broker(true, true).await;
    let r = request(ep, Operation::Write);
    let f = Arc::new(DeviceConnectorFacade::new());
    let input = RequestInput {
        values: std::collections::BTreeMap::from([("test-param".into(), FieldValue::Unsigned(u64::MAX))]),
        ..Default::default()
    };
    let mut out = RequestOutput::default();
    assert_eq!(
        f.execute(r.clone(), &input, &mut out, r.deadline(), None)
            .await
            .unwrap(),
        ExecutionOutcome::Failed
    );
    assert_eq!(out.items[0].status, ItemStatus::Failed);
    server.await.unwrap();
    f.close().await;
}
#[tokio::test]
async fn read_handles_retained_message_before_suback() {
    let (ep, server) = broker(false, false).await;
    let r = request(ep, Operation::Read);
    let f = Arc::new(DeviceConnectorFacade::new());
    let mut out = RequestOutput::default();
    assert_eq!(
        f.execute(r.clone(), &RequestInput::default(), &mut out, r.deadline(), None)
            .await
            .unwrap(),
        ExecutionOutcome::Completed
    );
    assert_eq!(out.items[0].value, Some(FieldValue::Decimal(u64::MAX.into())));
    server.await.unwrap();
    f.close().await;
}
