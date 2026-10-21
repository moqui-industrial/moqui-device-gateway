//! MQTT 5: success means PUBACK/SUBACK, not merely enqueueing a command.
use crate::{
    client::{DeviceClient, SampleSink},
    model::*,
    payload,
};
use async_trait::async_trait;
use rumqttc::{
    Transport,
    v5::{
        AsyncClient, Event, EventLoop, MqttOptions,
        mqttbytes::{
            QoS,
            v5::{Packet, PubAckReason, SubscribeReasonCode},
        },
    },
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use tokio_util::sync::CancellationToken;
#[derive(Default)]
pub struct MqttDeviceClient;
static NEXT_CLIENT: AtomicU64 = AtomicU64::new(1);
fn session(request: &PreparedDeviceRequest) -> Result<(AsyncClient, EventLoop), DeviceGatewayError> {
    let c = &request.connection;
    let url = url::Url::parse(&c.endpoint)
        .map_err(|_| DeviceGatewayError::invalid("MQTT endpoint must be mqtt:// or mqtts://"))?;
    if !matches!(url.scheme(), "mqtt" | "mqtts")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || !matches!(url.path(), "" | "/")
    {
        return Err(DeviceGatewayError::invalid(
            "Use a broker URL without credentials, topic, or query; configure options separately",
        ));
    }
    for key in c.options.keys() {
        if !matches!(key.as_str(), "client-id-prefix" | "keep-alive-seconds" | "retain") {
            return Err(DeviceGatewayError::unsupported(format!("Unknown MQTT option: {key}")));
        }
    }
    let id = format!(
        "{}-{}-{}",
        c.options
            .get("client-id-prefix")
            .map(String::as_str)
            .unwrap_or("moqui-device-gateway"),
        std::process::id(),
        NEXT_CLIENT.fetch_add(1, Ordering::Relaxed)
    );
    let mut options = MqttOptions::new(
        id,
        url.host_str()
            .ok_or_else(|| DeviceGatewayError::invalid("Missing MQTT host"))?,
        url.port().unwrap_or(if url.scheme() == "mqtts" { 8883 } else { 1883 }),
    );
    if url.scheme() == "mqtts" {
        options.set_transport(Transport::tls_with_default_config());
    }
    let keep = c
        .options
        .get("keep-alive-seconds")
        .map(|v| v.parse::<u64>())
        .transpose()
        .map_err(|_| DeviceGatewayError::invalid("Invalid MQTT keep-alive"))?
        .unwrap_or(30);
    if !(5..=3600).contains(&keep) {
        return Err(DeviceGatewayError::invalid("MQTT keep-alive must be 5..3600 seconds"));
    }
    options.set_keep_alive(Duration::from_secs(keep));
    if let Some(user) = &c.credentials.username {
        options.set_credentials(user, c.credentials.password.as_deref().unwrap_or(""));
    }
    options.set_clean_start(true);
    Ok(AsyncClient::new(options, 32))
}
fn topic(request: &PreparedDeviceRequest, item: Option<&Item>) -> Result<String, DeviceGatewayError> {
    let topic = item
        .map(|i| i.address.as_str())
        .filter(|s| !s.is_empty())
        .or(request.query.as_deref())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| DeviceGatewayError::invalid("MQTT topic is required"))?;
    Ok(topic.into())
}
async fn poll(events: &mut EventLoop, deadline: Instant, writing: bool) -> Result<Event, DeviceGatewayError> {
    tokio::time::timeout_at(deadline.into(), events.poll())
        .await
        .map_err(|_| DeviceGatewayError::timeout(writing))?
        .map_err(|_| {
            let e = DeviceGatewayError::transport("MQTT connection failed");
            if writing { e.uncertain() } else { e }
        })
}
async fn subscribe_topics(
    client: &AsyncClient,
    events: &mut EventLoop,
    topics: &[String],
    deadline: Instant,
) -> Result<Vec<(String, Vec<u8>)>, DeviceGatewayError> {
    let mut pending = Vec::new();
    for topic in topics {
        client
            .subscribe(topic, QoS::AtLeastOnce)
            .await
            .map_err(|_| DeviceGatewayError::transport("Cannot queue MQTT subscription"))?;
        loop {
            match poll(events, deadline, false).await? {
                Event::Incoming(Packet::SubAck(ack)) => {
                    if ack.return_codes.len() != 1 || !matches!(ack.return_codes[0], SubscribeReasonCode::Success(_)) {
                        return Err(DeviceGatewayError::transport("MQTT broker rejected subscription"));
                    }
                    break;
                }
                Event::Incoming(Packet::Publish(p)) => {
                    pending.push((String::from_utf8_lossy(&p.topic).into_owned(), p.payload.to_vec()))
                }
                _ => {}
            }
        }
    }
    Ok(pending)
}
fn topics(r: &PreparedDeviceRequest) -> Result<Vec<String>, DeviceGatewayError> {
    let mut topics = Vec::new();
    if r.plc_log {
        topics.push(topic(r, None)?);
    } else {
        for i in &r.items {
            let t = topic(r, Some(i))?;
            if !topics.contains(&t) {
                topics.push(t);
            }
        }
    }
    if topics.is_empty() {
        return Err(DeviceGatewayError::invalid("MQTT request has no topics"));
    }
    Ok(topics)
}
fn matches_topic(filter: &str, topic: &str) -> bool {
    if topic.starts_with('$') && !filter.starts_with('$') {
        return false;
    }
    let mut f = filter.split('/');
    let mut t = topic.split('/');
    loop {
        match (f.next(), t.next()) {
            (Some("#"), _) => return f.next().is_none(),
            (Some("+"), Some(_)) => {}
            (Some(a), Some(b)) if a == b => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}
fn values(r: &PreparedDeviceRequest, topic: &str, bytes: &[u8]) -> Result<Vec<ItemResult>, DeviceGatewayError> {
    let mut selected = r.clone();
    selected
        .items
        .retain(|i| self::topic(r, Some(i)).is_ok_and(|t| matches_topic(&t, topic)));
    payload::read_payload(&selected, bytes)
}
fn emit(r: &PreparedDeviceRequest, topic: &str, bytes: &[u8], sink: &dyn SampleSink) -> Result<(), DeviceGatewayError> {
    if r.plc_log {
        return sink.emit(payload::plc_log_batch(r, bytes)?);
    }
    let mut samples = Vec::new();
    for i in values(r, topic, bytes)? {
        if let Some(error) = i.error {
            return Err(DeviceGatewayError::invalid(error));
        }
        if let Some(value) = i.value {
            samples.push(Sample {
                device_id: r.device_id.clone(),
                log_value_kind: None,
                parameter_id: Some(i.parameter_id),
                value,
                source_time: i.source_time,
                received_time: SystemTime::now(),
                quality: i.quality,
            });
        }
    }
    sink.emit(SampleBatch {
        request_name: r.name.clone(),
        logging: r.logging,
        request: Some(Arc::new(r.clone())),
        samples,
    })
}
#[async_trait]
impl DeviceClient for MqttDeviceClient {
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
    ) -> Result<(), DeviceGatewayError> {
        let (client, mut events) = session(r)?;
        if r.operation == Operation::Write {
            let retain = match r
                .connection
                .options
                .get("retain")
                .map(String::as_str)
                .unwrap_or("false")
            {
                "true" => true,
                "false" => false,
                _ => return Err(DeviceGatewayError::invalid("MQTT retain must be true or false")),
            };
            if let Some(message) = &input.payload {
                // One message on the request topic, as the trajectory export sends it.
                let topic = topic(r, None)?;
                if topic.contains(['#', '+']) {
                    return Err(DeviceGatewayError::invalid(
                        "MQTT publication topic cannot contain wildcards",
                    ));
                }
                client
                    .publish(topic, QoS::AtLeastOnce, retain, message.clone())
                    .await
                    .map_err(|_| DeviceGatewayError::transport("Cannot queue MQTT publication"))?;
                loop {
                    if let Event::Incoming(Packet::PubAck(ack)) = poll(&mut events, deadline, true).await? {
                        if !matches!(ack.reason, PubAckReason::Success | PubAckReason::NoMatchingSubscribers) {
                            return Err(DeviceGatewayError::transport("MQTT broker rejected publication"));
                        }
                        return Ok(());
                    }
                }
            }
            // Preflight every payload before sending the first command.
            let plans = r
                .items
                .iter()
                .map(|i| Ok((i, topic(r, Some(i))?, payload::write_item_payload(i, input)?)))
                .collect::<Result<Vec<_>, DeviceGatewayError>>()?;
            if plans.is_empty() {
                return Err(DeviceGatewayError::invalid("MQTT write has no items"));
            }
            for (_, topic, _) in &plans {
                if topic.contains(['#', '+']) {
                    return Err(DeviceGatewayError::invalid(
                        "MQTT publication topic cannot contain wildcards",
                    ));
                }
            }
            for (i, topic, bytes) in plans {
                client
                    .publish(topic, QoS::AtLeastOnce, retain, bytes)
                    .await
                    .map_err(|_| DeviceGatewayError::transport("Cannot queue MQTT publication"))?;
                loop {
                    if let Event::Incoming(Packet::PubAck(ack)) = poll(&mut events, deadline, true).await? {
                        if !matches!(ack.reason, PubAckReason::Success | PubAckReason::NoMatchingSubscribers) {
                            out.items.push(ItemResult::error(
                                &i.parameter_id,
                                DeviceGatewayError::transport("MQTT broker rejected publication"),
                            ));
                            break;
                        }
                        out.items.push(ItemResult::value(
                            &i.parameter_id,
                            input.values[&i.parameter_id].clone(),
                        ));
                        break;
                    }
                }
            }
        } else {
            let pending = subscribe_topics(&client, &mut events, &topics(r)?, deadline).await?;
            let mut received = BTreeMap::new();
            for (t, p) in pending {
                for i in values(r, &t, &p)? {
                    received.insert(i.parameter_id.clone(), i);
                }
            }
            while received.len() < r.items.len() {
                if let Event::Incoming(Packet::Publish(p)) = poll(&mut events, deadline, false).await? {
                    for i in values(r, &String::from_utf8_lossy(&p.topic), &p.payload)? {
                        received.insert(i.parameter_id.clone(), i);
                    }
                }
            }
            out.items.extend(received.into_values());
        }
        // Dropping the event loop closes this finite request session. No write is replayed.
        Ok(())
    }
    async fn subscribe(
        &self,
        r: Arc<PreparedDeviceRequest>,
        sink: Arc<dyn SampleSink>,
        cancel: CancellationToken,
        deadline: Instant,
    ) -> Result<tokio::task::JoinHandle<()>, DeviceGatewayError> {
        let (client, mut events) = session(&r)?;
        let pending = subscribe_topics(&client, &mut events, &topics(&r)?, deadline).await?;
        for (t, p) in pending {
            emit(&r, &t, &p, sink.as_ref())?;
        }
        Ok(tokio::spawn(async move {
            let _client = client;
            loop {
                tokio::select! {biased;_=cancel.cancelled()=>break,event=events.poll()=>match event{
                Ok(Event::Incoming(Packet::Publish(p)))=>if let Err(error)=emit(&r,&String::from_utf8_lossy(&p.topic),&p.payload,sink.as_ref()){tracing::error!(request=%r.name,error=%error,"MQTT sample rejected");},
                Ok(_)=>{},Err(_)=>{tracing::error!(request=%r.name,"MQTT subscription disconnected; request must be restarted");break;}}}
            }
        }))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn topic_matching() {
        assert!(matches_topic("a/+/c", "a/b/c"));
        assert!(matches_topic("a/#", "a"));
        assert!(!matches_topic("a/+", "a"));
        assert!(!matches_topic("#", "$SYS/broker"));
    }
}
