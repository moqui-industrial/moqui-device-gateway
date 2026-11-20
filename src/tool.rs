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

//! The ToolFactory of the `moqui` feature, tool `DeviceGateway`: one [`DeviceGatewayFacade`], the Tokio runtime its
//! backends run on, and the bounded queue that subscriptions and polling tasks deliver their samples to. A worker
//! thread hands the queued samples to the store services of moqui-device and tells the users when they fail to store
//! for long. A host without Moqui creates the facade, a runtime and its own `SampleSink` instead.

use crate::{client::DeviceGatewayFacade, model::*, SampleSink};
use moqui_core::{
    context::{Error, ExecutionContextFactory},
    native::{Tool, ToolFactory},
};
use moqui_lib::object::Value;
use std::{
    collections::HashMap,
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, Weak,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};

/// The facade, its I/O runtime and the sample queue.
pub struct Gateway {
    runtime: tokio::runtime::Runtime,
    facade: Arc<DeviceGatewayFacade>,
    sink: Arc<QueueSink>,
    stop: Arc<AtomicBool>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl Gateway {
    pub fn facade(&self) -> &Arc<DeviceGatewayFacade> {
        &self.facade
    }

    /// Runs a facade call from a thread outside the runtime, such as a service worker.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }

    pub fn handle(&self) -> tokio::runtime::Handle {
        self.runtime.handle().clone()
    }

    /// Where subscriptions and polling tasks deliver their samples.
    pub fn sink(&self) -> Arc<dyn SampleSink> {
        self.sink.clone()
    }
}

/// Batches waiting for the persistence worker, unless `moqui-device-gateway@queue-capacity` sets another number.
const QUEUE_CAPACITY: usize = 1024;
/// What the worker stores in one go: the batches already waiting, up to these limits.
const MAX_JOBS: usize = 512;
const MAX_SAMPLES: usize = 5000;
/// The longest wait between attempts while the database does not answer.
const MAX_RETRY_DELAY: Duration = Duration::from_secs(10);
pub(crate) struct Job {
    batch: SampleBatch,
    /// Sent once the batch is stored or rejected for good; dropped when the worker gives it up.
    done: Option<oneshot::Sender<()>>,
}

/// The samples a full queue discarded since the last notice, by request: the device and the number of samples.
type Discarded = Mutex<HashMap<String, (String, u64)>>;

pub(crate) struct QueueSink {
    sender: mpsc::Sender<Job>,
    discarded: Arc<Discarded>,
    capacity: usize,
}

impl QueueSink {
    /// Counts what a full queue discarded, for the worker's notice, and logs the first discard of each request at once.
    fn discard(&self, batch: &SampleBatch) {
        let device = batch
            .request
            .as_ref()
            .map(|r| r.device_id.clone())
            .or_else(|| batch.samples.first().map(|s| s.device_id.clone()))
            .unwrap_or_default();
        let samples = batch.samples.len().max(1) as u64;
        let Ok(mut discarded) = self.discarded.lock() else {
            return;
        };
        let entry = discarded.entry(batch.request_name.clone()).or_insert_with(|| {
            tracing::error!(
                request = %batch.request_name,
                capacity = self.capacity,
                "Device gateway persistence queue is full: samples are discarded; see moqui-device-gateway@queue-capacity"
            );
            (device, 0)
        });
        entry.1 += samples;
    }
}

#[async_trait::async_trait]
impl SampleSink for QueueSink {
    /// Polling and OPC UA samples: nothing upstream keeps them, so a full queue rejects them.
    fn emit(&self, batch: SampleBatch) -> Result<(), DeviceGatewayError> {
        self.sender.try_send(Job { batch, done: None }).map_err(|e| match e {
            mpsc::error::TrySendError::Full(job) => {
                self.discard(&job.batch);
                DeviceGatewayError::new(
                    ErrorKind::Busy,
                    "Device gateway persistence queue is full; sample was not saved",
                )
            }
            mpsc::error::TrySendError::Closed(_) => {
                DeviceGatewayError::new(ErrorKind::Closed, "Device gateway persistence worker is closed")
            }
        })
    }

    /// A durable source's batch waits for room: the source stops taking more, and its broker keeps them.
    async fn deliver(&self, batch: SampleBatch) -> Result<crate::device_facade::Receipt, DeviceGatewayError> {
        let (done, receipt) = crate::device_facade::Receipt::pending();
        self.sender
            .send(Job {
                batch,
                done: Some(done),
            })
            .await
            .map_err(|_| DeviceGatewayError::new(ErrorKind::Closed, "Device gateway persistence worker is closed"))?;
        Ok(receipt)
    }
}

#[derive(Default)]
pub struct DeviceGatewayToolFactory {
    gateway: Mutex<Option<Arc<Gateway>>>,
}

impl DeviceGatewayToolFactory {
    /// The facade, runtime and sample queue of this factory, once initialized.
    pub fn gateway(&self) -> Result<Arc<Gateway>, Error> {
        self.gateway
            .lock()
            .map_err(|_| Error::new("Device gateway factory lock poisoned"))?
            .clone()
            .ok_or_else(|| Error::new("Device gateway factory is not initialized"))
    }

    pub fn facade(&self) -> Result<Arc<DeviceGatewayFacade>, Error> {
        Ok(self.gateway()?.facade.clone())
    }
}

struct DeviceGatewayTool;
impl Tool for DeviceGatewayTool {}
impl ToolFactory for DeviceGatewayToolFactory {
    fn name(&self) -> &str {
        "DeviceGateway"
    }

    fn init(&self, factory: &Arc<ExecutionContextFactory>) -> Result<(), Error> {
        let mut gateway = self
            .gateway
            .lock()
            .map_err(|_| Error::new("Device gateway factory lock poisoned"))?;
        if gateway.is_some() {
            return Ok(());
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("device-gateway-io")
            .build()
            .map_err(|e| Error::new(e.to_string()))?;
        let capacity = queue_capacity();
        let (sender, receiver) = mpsc::channel::<Job>(capacity);
        let discarded = Arc::new(Discarded::default());
        let reported = discarded.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let factory = Arc::downgrade(factory);
        let handle = runtime.handle().clone();
        let worker = std::thread::Builder::new()
            .name("device-gateway-store".into())
            .spawn(move || persist_loop(factory, handle, receiver, stopped, reported, capacity))
            .map_err(|e| Error::new(e.to_string()))?;
        *gateway = Some(Arc::new(Gateway {
            runtime,
            facade: Arc::new(DeviceGatewayFacade::new()),
            sink: Arc::new(QueueSink {
                sender,
                discarded,
                capacity,
            }),
            stop,
            worker: Mutex::new(Some(worker)),
        }));
        Ok(())
    }

    fn get_instance(&self, _: &[Value]) -> Result<Arc<dyn Tool>, Error> {
        self.gateway()?;
        Ok(Arc::new(DeviceGatewayTool))
    }

    /// Ends the subscriptions (a durable broker session keeps collecting until the next start), then stops the worker:
    /// the samples stored are acknowledged, the others are delivered again by their broker.
    fn destroy(&self) {
        let gateway = self.gateway.lock().ok().and_then(|mut g| g.take());
        if let Some(gateway) = gateway {
            gateway.runtime.block_on(gateway.facade.close());
            gateway.stop.store(true, Ordering::Release);
            let worker = gateway.worker.lock().ok().and_then(|mut w| w.take());
            if worker.is_some_and(|w| w.join().is_err()) {
                tracing::error!("Device gateway store worker panicked");
            }
        }
    }
}

/// `moqui-device-gateway@queue-capacity`, in batches: a number above zero, else the default.
fn queue_capacity() -> usize {
    let value = moqui_core::context::conf_env::setting("moqui-device-gateway@queue-capacity");
    if value.trim().is_empty() {
        return QUEUE_CAPACITY;
    }
    match value.trim().parse::<usize>() {
        Ok(capacity) if capacity > 0 => capacity,
        _ => {
            tracing::error!(value = %value, default = QUEUE_CAPACITY, "Invalid moqui-device-gateway@queue-capacity; using the default");
            QUEUE_CAPACITY
        }
    }
}

/// Persistence failure notices: `moqui-device-gateway@error-notification` false turns them off, and
/// `@error-notification-seconds` (default 60) is how long the failures of a request last before the
/// user is told, and the shortest time between two notices of discarded samples.
fn notification_seconds() -> Option<Duration> {
    if moqui_core::context::conf_env::setting("moqui-device-gateway@error-notification") == "false" {
        return None;
    }
    let seconds = moqui_core::context::conf_env::setting("moqui-device-gateway@error-notification-seconds")
        .parse::<u64>()
        .unwrap_or(60);
    Some(Duration::from_secs(seconds))
}

/// Tells the users what a full queue discarded since the last notice, at most once every `interval`. A notice needs the
/// database: during an outage the log has the discards, and the notice follows once the worker stores again.
struct DiscardNotices {
    discarded: Arc<Discarded>,
    capacity: usize,
    interval: Option<Duration>,
    last: Option<Instant>,
}

impl DiscardNotices {
    fn send(&mut self, factory: &Weak<ExecutionContextFactory>) {
        let Some(interval) = self.interval else {
            return;
        };
        if self.last.is_some_and(|last| last.elapsed() < interval) {
            return;
        }
        let taken = match self.discarded.lock() {
            Ok(mut discarded) if !discarded.is_empty() => std::mem::take(&mut *discarded),
            _ => return,
        };
        self.last = Some(Instant::now());
        let Some(factory) = factory.upgrade() else {
            return;
        };
        let ec = factory.new_context();
        let _entered = ec.enter();
        let _auth = ec.disable_authz();
        for (request, (device, samples)) in taken {
            if let Err(error) = crate::binding::notify_discarded(&ec, &request, &device, samples, self.capacity) {
                tracing::error!(request = %request, error = %error, "Discarded samples notice not sent");
            }
        }
    }
}

/// The persistence worker: takes the batches waiting, stores them a request at a time, and while the database does not
/// answer keeps them and tries again, so the queue fills and durable sources stop taking more from their broker.
fn persist_loop(
    factory: Weak<ExecutionContextFactory>,
    handle: tokio::runtime::Handle,
    mut receiver: mpsc::Receiver<Job>,
    stop: Arc<AtomicBool>,
    discarded: Arc<Discarded>,
    capacity: usize,
) {
    let interval = notification_seconds();
    let mut errors = interval.map(crate::notifier::InboundErrors::new);
    let mut notices = DiscardNotices {
        discarded,
        capacity,
        interval,
        last: None,
    };
    loop {
        notices.send(&factory);
        let first =
            match handle.block_on(async { tokio::time::timeout(Duration::from_millis(100), receiver.recv()).await }) {
                Ok(Some(job)) => job,
                Ok(None) => break,
                Err(_) if stop.load(Ordering::Acquire) => break,
                Err(_) => continue,
            };
        let mut count = first.batch.samples.len();
        let mut jobs = vec![first];
        while count < MAX_SAMPLES && jobs.len() < MAX_JOBS {
            match receiver.try_recv() {
                Ok(job) => {
                    count += job.batch.samples.len();
                    jobs.push(job);
                }
                Err(_) => break,
            }
        }
        let Some(factory) = factory.upgrade() else {
            break;
        };
        let mut groups: Vec<Vec<Job>> = Vec::new();
        for job in jobs {
            match groups
                .iter_mut()
                .find(|g| g[0].batch.request_name == job.batch.request_name)
            {
                Some(group) => group.push(job),
                None => groups.push(vec![job]),
            }
        }
        for group in groups {
            store_group(&factory, group, &mut errors, &stop);
        }
    }
}

/// The batches of one request as one.
fn merge(jobs: &[Job]) -> SampleBatch {
    let first = &jobs[0].batch;
    SampleBatch {
        request_name: first.request_name.clone(),
        errors: jobs.iter().flat_map(|j| j.batch.errors.iter().cloned()).collect(),
        logging: first.logging,
        request: first.request.clone(),
        samples: jobs.iter().flat_map(|j| j.batch.samples.iter().cloned()).collect(),
    }
}

fn store_group(
    factory: &Arc<ExecutionContextFactory>,
    jobs: Vec<Job>,
    errors: &mut Option<crate::notifier::InboundErrors>,
    stop: &AtomicBool,
) {
    let mut batch = merge(&jobs);
    let mut delay = Duration::from_millis(200);
    loop {
        let finished = {
            let ec = factory.new_context();
            let _entered = ec.enter();
            let _auth = ec.disable_authz();
            let (finished, failure) = match crate::binding::store_batch(&ec, &batch) {
                Ok(crate::binding::Stored::Done) => (true, None),
                Ok(crate::binding::Stored::Failed(message)) => (true, Some(message)),
                Ok(crate::binding::Stored::Retry(from)) => {
                    batch.samples.drain(..from.min(batch.samples.len()));
                    (
                        false,
                        Some("The database does not answer; the samples are kept and stored again".to_string()),
                    )
                }
                Err(error) => (true, Some(error.to_string())),
            };
            match (&failure, finished) {
                (Some(error), true) => {
                    tracing::error!(
                        request = %batch.request_name,
                        error = %error,
                        "Device gateway samples were not saved"
                    )
                }
                (Some(error), false) => {
                    tracing::warn!(
                        request = %batch.request_name,
                        samples = batch.samples.len(),
                        error = %error,
                        "Device gateway samples wait for the database"
                    )
                }
                _ => {}
            }
            if let Some(errors) = errors.as_mut() {
                let now = std::time::Instant::now();
                let notice = match &failure {
                    None => errors.clear(&batch.request_name, now),
                    Some(_) => errors.record_error(&batch.request_name, now),
                };
                if let Some(notice) = notice {
                    if let Err(error) = crate::binding::notify_inbound(&ec, &batch, &notice, failure.clone()) {
                        tracing::error!(request = %batch.request_name, error = %error, "Persistence notice not sent");
                    }
                }
            }
            finished
        };
        if finished {
            for job in jobs {
                if let Some(done) = job.done {
                    let _ = done.send(());
                }
            }
            return;
        }
        if stop.load(Ordering::Acquire) {
            // Dropping the receipts leaves durable messages unacknowledged: the broker delivers them again.
            tracing::warn!(
                request = %batch.request_name,
                samples = batch.samples.len(),
                "Device gateway stopped before the database answered; durable sources deliver the samples again"
            );
            return;
        }
        let until = std::time::Instant::now() + delay;
        while std::time::Instant::now() < until && !stop.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(100));
        }
        delay = (delay * 2).min(MAX_RETRY_DELAY);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(request: &str, values: usize) -> SampleBatch {
        SampleBatch {
            request_name: request.into(),
            errors: Vec::new(),
            logging: false,
            request: None,
            samples: (0..values)
                .map(|_| Sample {
                    device_id: "PLC_1".into(),
                    log_value_kind: None,
                    parameter_id: Some("P".into()),
                    value: FieldValue::Bool(true),
                    source_time: None,
                    received_time: std::time::SystemTime::now(),
                    quality: None,
                })
                .collect(),
        }
    }

    #[test]
    fn a_full_queue_counts_the_discarded_samples_by_request() {
        let (sender, _receiver) = mpsc::channel::<Job>(1);
        let sink = QueueSink {
            sender,
            discarded: Arc::new(Discarded::default()),
            capacity: 1,
        };
        assert!(sink.emit(batch("A", 3)).is_ok());
        assert_eq!(sink.emit(batch("A", 2)).unwrap_err().kind, ErrorKind::Busy);
        assert!(sink.emit(batch("A", 4)).is_err());
        assert!(sink.emit(batch("B", 1)).is_err());
        let discarded = sink.discarded.lock().unwrap();
        assert_eq!(discarded.get("A"), Some(&("PLC_1".to_string(), 6)));
        assert_eq!(discarded.get("B"), Some(&("PLC_1".to_string(), 1)));
    }
}
