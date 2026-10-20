//! Factory lifecycle owns the I/O runtime and a bounded persistence queue.
use crate::{
    client::{DeviceConnectorFacade, SampleSink},
    model::*,
};
use moqui_core::{
    context::{Error, ExecutionContextFactory},
    native::{Tool, ToolFactory},
};
use moqui_lib::object::Value;
use std::{
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread::JoinHandle,
    time::Duration,
};
#[derive(Default)]
pub struct DeviceConnectorToolFactory {
    state: Mutex<Option<Arc<State>>>,
}
pub(crate) struct State {
    pub runtime: tokio::runtime::Runtime,
    pub facade: Arc<DeviceConnectorFacade>,
    pub sink: Arc<QueueSink>,
    stop: Arc<AtomicBool>,
    worker: Mutex<Option<JoinHandle<()>>>,
}
pub(crate) struct QueueSink {
    sender: mpsc::SyncSender<SampleBatch>,
}
impl SampleSink for QueueSink {
    fn emit(&self, batch: SampleBatch) -> Result<(), DeviceConnectorError> {
        self.sender.try_send(batch).map_err(|e| match e {
            mpsc::TrySendError::Full(_) => DeviceConnectorError::new(
                ErrorKind::Busy,
                "Device connector persistence queue is full; sample was not saved",
            ),
            mpsc::TrySendError::Disconnected(_) => {
                DeviceConnectorError::new(ErrorKind::Closed, "Device connector persistence worker is closed")
            }
        })
    }
}
impl DeviceConnectorToolFactory {
    pub(crate) fn state(&self) -> Result<Arc<State>, Error> {
        self.state
            .lock()
            .map_err(|_| Error::new("Device connector factory lock poisoned"))?
            .clone()
            .ok_or_else(|| Error::new("Device connector factory is not initialized"))
    }
    /// Resolve once during setup. The returned API needs no ExecutionContext.
    pub fn facade(&self) -> Result<Arc<DeviceConnectorFacade>, Error> {
        Ok(self.state()?.facade.clone())
    }
}
struct DeviceConnectorTool {
    pub _facade: Arc<DeviceConnectorFacade>,
}
impl Tool for DeviceConnectorTool {}
impl ToolFactory for DeviceConnectorToolFactory {
    fn name(&self) -> &str {
        "DeviceConnector"
    }
    fn init(&self, factory: &Arc<ExecutionContextFactory>) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::new("Device connector factory lock poisoned"))?;
        if state.is_some() {
            return Ok(());
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("device-connector-io")
            .build()
            .map_err(|e| Error::new(e.to_string()))?;
        let (sender, receiver) = mpsc::sync_channel::<SampleBatch>(1024);
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let factory = Arc::downgrade(factory);
        let worker = std::thread::Builder::new()
            .name("device-connector-store".into())
            .spawn(move || persist_loop(factory, receiver, stopped))
            .map_err(|e| Error::new(e.to_string()))?;
        *state = Some(Arc::new(State {
            runtime,
            facade: Arc::new(DeviceConnectorFacade::new()),
            sink: Arc::new(QueueSink { sender }),
            stop,
            worker: Mutex::new(Some(worker)),
        }));
        Ok(())
    }
    fn get_instance(&self, _: &[Value]) -> Result<Arc<dyn Tool>, Error> {
        Ok(Arc::new(DeviceConnectorTool {
            _facade: self.facade()?,
        }))
    }
    fn destroy(&self) {
        let state = self.state.lock().ok().and_then(|mut s| s.take());
        if let Some(state) = state {
            state.runtime.block_on(state.facade.close());
            state.stop.store(true, Ordering::Release);
            if let Ok(mut w) = state.worker.lock()
                && let Some(w) = w.take()
                && w.join().is_err()
            {
                tracing::error!("Device connector persistence worker panicked");
            }
        }
    }
}
fn persist_loop(factory: Weak<ExecutionContextFactory>, receiver: mpsc::Receiver<SampleBatch>, stop: Arc<AtomicBool>) {
    loop {
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(batch) => {
                let Some(factory) = factory.upgrade() else {
                    break;
                };
                let ec = factory.new_context();
                let _entered = ec.enter();
                let _auth = ec.disable_authz();
                if let Err(error) = crate::binding::persist_batch(&ec, &batch) {
                    tracing::error!(request=%batch.request_name,error=%error,"Device connector samples were not saved");
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) if stop.load(Ordering::Acquire) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
}
