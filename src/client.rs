use crate::model::*;
use async_trait::async_trait;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Instant, SystemTime},
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

pub trait SampleSink: Send + Sync {
    /// Must not block a protocol callback. A full queue is an observable failure.
    fn emit(&self, batch: SampleBatch) -> Result<(), FieldbusError>;
}
#[async_trait]
pub trait FieldbusClient: Send + Sync {
    fn capabilities(&self) -> Capabilities;
    async fn execute(
        &self,
        request: &PreparedDeviceRequest,
        input: &RequestInput,
        output: &mut RequestOutput,
        deadline: Instant,
    ) -> Result<(), FieldbusError>;
    async fn subscribe(
        &self,
        _request: Arc<PreparedDeviceRequest>,
        _sink: Arc<dyn SampleSink>,
        _cancel: CancellationToken,
        _deadline: Instant,
    ) -> Result<tokio::task::JoinHandle<()>, FieldbusError> {
        Err(FieldbusError::unsupported("This backend has no event subscription"))
    }
    async fn close(&self) {}
}
struct Subscription {
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    request: Arc<PreparedDeviceRequest>,
}
pub struct FieldbusFacade {
    clients: HashMap<ProtocolKey, Arc<dyn FieldbusClient>>,
    subscriptions: Mutex<HashMap<String, Subscription>>,
    lifecycle: Mutex<()>,
    closed: AtomicBool,
}
#[derive(Hash, Eq, PartialEq)]
struct ProtocolKey(u8);
impl From<Protocol> for ProtocolKey {
    fn from(p: Protocol) -> Self {
        Self(p as u8)
    }
}
impl Default for FieldbusFacade {
    fn default() -> Self {
        Self::new()
    }
}
impl FieldbusFacade {
    pub fn new() -> Self {
        let mut facade = Self {
            clients: HashMap::new(),
            subscriptions: Mutex::new(HashMap::new()),
            lifecycle: Mutex::new(()),
            closed: AtomicBool::new(false),
        };
        #[cfg(feature = "modbus")]
        facade.install(
            Protocol::ModbusTcp,
            Arc::new(crate::backends::modbus::ModbusClient::default()),
        );
        #[cfg(feature = "mqtt")]
        facade.install(Protocol::Mqtt, Arc::new(crate::backends::mqtt::MqttClient));
        #[cfg(feature = "opcua")]
        facade.install(
            Protocol::OpcUa,
            Arc::new(crate::backends::opcua::OpcUaClient::default()),
        );
        let files = Arc::new(crate::backends::file::FileClient);
        facade.install(Protocol::File, files.clone());
        facade.install(Protocol::Sftp, files);
        facade
    }
    pub fn install(&mut self, protocol: Protocol, client: Arc<dyn FieldbusClient>) {
        self.clients.insert(protocol.into(), client);
    }
    pub async fn execute(
        self: &Arc<Self>,
        request: Arc<PreparedDeviceRequest>,
        input: &RequestInput,
        output: &mut RequestOutput,
        deadline: Instant,
        sink: Option<Arc<dyn SampleSink>>,
    ) -> Result<ExecutionOutcome, FieldbusError> {
        request.validate()?;
        *output = RequestOutput::default();
        if self.closed.load(Ordering::Acquire) {
            return Err(FieldbusError::new(ErrorKind::Closed, "Fieldbus is shutting down"));
        }
        if deadline <= Instant::now() {
            return Err(FieldbusError::timeout(false));
        }
        if request.operation == Operation::Unsubscribe {
            let _guard = tokio::time::timeout_at(deadline.into(), self.lifecycle.lock())
                .await
                .map_err(|_| FieldbusError::timeout(false))?;
            let key = request
                .parent_request
                .as_ref()
                .ok_or_else(|| FieldbusError::invalid("parentRequestName is required"))?;
            let mut subscriptions = self.subscriptions.lock().await;
            if subscriptions
                .get(key)
                .is_some_and(|s| s.request.device_id != request.device_id)
            {
                return Err(FieldbusError::invalid("Subscription belongs to a different device"));
            }
            let sub = subscriptions.remove(key);
            drop(subscriptions);
            if let Some(mut sub) = sub {
                sub.cancel.cancel();
                if tokio::time::timeout_at(deadline.into(), &mut sub.task).await.is_err() {
                    sub.task.abort();
                    let _ = sub.task.await;
                    return Err(FieldbusError::timeout(false));
                }
            }
            return Ok(ExecutionOutcome::Completed);
        }
        let client = self
            .clients
            .get(&request.connection.protocol.into())
            .cloned()
            .ok_or_else(|| FieldbusError::unsupported("Protocol is not compiled into this component"))?;
        let cap = client.capabilities();
        match request.operation {
            Operation::Read if !cap.read => return Err(FieldbusError::unsupported("Backend cannot read")),
            Operation::Write if !cap.write => return Err(FieldbusError::unsupported("Backend cannot write")),
            Operation::Transfer if !cap.transfer => {
                return Err(FieldbusError::unsupported("Backend cannot transfer files"));
            }
            _ => {}
        }
        if matches!(request.operation, Operation::Poll | Operation::Subscribe) {
            let sink = sink.ok_or_else(|| FieldbusError::invalid("Subscription requires a sample sink"))?;
            let _guard = tokio::time::timeout_at(deadline.into(), self.lifecycle.lock())
                .await
                .map_err(|_| FieldbusError::timeout(false))?;
            if let Some(existing) = self.subscriptions.lock().await.get(&request.name)
                && !existing.task.is_finished()
            {
                return Err(FieldbusError::new(
                    ErrorKind::Busy,
                    "Request is already active; unsubscribe before replacing it",
                ));
            }
            let cancel = CancellationToken::new();
            let task = if request.operation == Operation::Subscribe && cap.subscribe {
                client
                    .subscribe(
                        request.clone(),
                        Arc::new(DecodedSink {
                            request: request.clone(),
                            inner: sink,
                        }),
                        cancel.clone(),
                        deadline,
                    )
                    .await?
            } else if request.operation == Operation::Poll && cap.read {
                // Probe before declaring the polling operation active.
                let mut probe = RequestOutput::default();
                self.read_write(&request, input, &mut probe, deadline, client.clone())
                    .await?;
                if matches!(probe.outcome(), ExecutionOutcome::Failed | ExecutionOutcome::Partial) {
                    return Err(FieldbusError::transport("Polling probe returned failed items"));
                }
                sink.emit(batch(&request, probe.items))?;
                let facade = Arc::downgrade(self);
                let req = request.clone();
                let stop = cancel.clone();
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval_at(
                        tokio::time::Instant::now() + req.polling_interval,
                        req.polling_interval,
                    );
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    loop {
                        tokio::select! {_ = stop.cancelled()=>break,_ = interval.tick()=>{}}
                        let Some(facade) = facade.upgrade() else {
                            break;
                        };
                        let mut out = RequestOutput::default();
                        match facade
                            .read_write(&req, &RequestInput::default(), &mut out, req.deadline(), client.clone())
                            .await
                        {
                            Ok(()) => {
                                if let Err(error) = sink.emit(batch(&req, out.items)) {
                                    tracing::error!(request=%req.name,error=%error,"Field sample was not queued");
                                }
                            }
                            Err(error) => tracing::error!(request=%req.name,error=%error,"Field polling failed"),
                        }
                    }
                })
            } else {
                return Err(FieldbusError::unsupported("Backend cannot subscribe or poll"));
            };
            self.subscriptions.lock().await.insert(
                request.name.clone(),
                Subscription {
                    cancel,
                    task,
                    request: request.clone(),
                },
            );
            output.subscription_id = Some(request.name.clone());
            return Ok(ExecutionOutcome::Active);
        }
        self.read_write(&request, input, output, deadline, client).await?;
        Ok(output.outcome())
    }
    async fn read_write(
        &self,
        request: &PreparedDeviceRequest,
        input: &RequestInput,
        output: &mut RequestOutput,
        deadline: Instant,
        client: Arc<dyn FieldbusClient>,
    ) -> Result<(), FieldbusError> {
        let mut physical = input.clone();
        if request.operation == Operation::Write {
            for item in &request.items {
                let value = input
                    .values
                    .get(&item.parameter_id)
                    .ok_or_else(|| FieldbusError::invalid("Write item has no logical value"))?;
                physical.values.insert(
                    item.parameter_id.clone(),
                    item.codec.encode(value, item.kind, item.physical_type)?,
                );
            }
        }
        let writing = matches!(request.operation, Operation::Write | Operation::Transfer);
        let execution = tokio::time::timeout_at(deadline.into(), client.execute(request, &physical, output, deadline))
            .await
            .map_err(|_| FieldbusError::timeout(writing))
            .and_then(|result| result);
        if matches!(
            request.operation,
            Operation::Read | Operation::Poll | Operation::Subscribe
        ) {
            decode_results(request, &mut output.items);
        }
        if let Err(error) = execution {
            for item in &request.items {
                if !output
                    .items
                    .iter()
                    .any(|result| result.parameter_id == item.parameter_id)
                {
                    output.items.push(ItemResult::error(&item.parameter_id, error.clone()));
                }
            }
            return Err(error);
        }

        Ok(())
    }
    pub async fn close(&self) {
        let _guard = self.lifecycle.lock().await;
        self.closed.store(true, Ordering::Release);
        let subs = std::mem::take(&mut *self.subscriptions.lock().await);
        for sub in subs.values() {
            sub.cancel.cancel();
        }
        for (_, mut sub) in subs {
            if tokio::time::timeout(std::time::Duration::from_secs(3), &mut sub.task)
                .await
                .is_err()
            {
                sub.task.abort();
                let _ = sub.task.await;
            }
        }
        for client in self.clients.values() {
            if tokio::time::timeout(std::time::Duration::from_secs(5), client.close())
                .await
                .is_err()
            {
                tracing::error!("Fieldbus backend did not close within five seconds");
            }
        }
    }
}
fn decode_results(request: &PreparedDeviceRequest, results: &mut [ItemResult]) {
    for result in results {
        if result.status != ItemStatus::Completed {
            continue;
        }
        if let (Some(item), Some(value)) = (
            request.items.iter().find(|i| i.parameter_id == result.parameter_id),
            result.value.as_ref(),
        ) {
            match item.codec.decode(value, item.kind) {
                Ok(value) => result.value = Some(value),
                Err(error) => {
                    result.status = ItemStatus::Failed;
                    result.error = Some(error.message);
                    result.value = None;
                }
            }
        }
    }
}
fn batch(request: &PreparedDeviceRequest, items: Vec<ItemResult>) -> SampleBatch {
    SampleBatch {
        request_name: request.name.clone(),
        logging: request.logging,
        request: Some(Arc::new(request.clone())),
        samples: items
            .into_iter()
            .filter(|i| i.status == ItemStatus::Completed)
            .filter_map(|i| {
                i.value.map(|value| Sample {
                    device_id: request.device_id.clone(),
                    log_value_kind: None,
                    parameter_id: Some(i.parameter_id),
                    value,
                    source_time: i.source_time,
                    received_time: SystemTime::now(),
                    quality: i.quality,
                })
            })
            .collect(),
    }
}
struct DecodedSink {
    request: Arc<PreparedDeviceRequest>,
    inner: Arc<dyn SampleSink>,
}
impl SampleSink for DecodedSink {
    fn emit(&self, mut value: SampleBatch) -> Result<(), FieldbusError> {
        value.request = Some(self.request.clone());
        if !self.request.plc_log {
            for sample in &mut value.samples {
                if let Some(item) = self
                    .request
                    .items
                    .iter()
                    .find(|i| Some(&i.parameter_id) == sample.parameter_id.as_ref())
                {
                    sample.value = item.codec.decode(&sample.value, item.kind)?;
                }
            }
        }
        self.inner.emit(value)
    }
}
