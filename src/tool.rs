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

//! The registry of connections and subscriptions: the factory owns the I/O runtime, the protocol clients with their
//! sessions and the active subscriptions (through DeviceGatewayFacade), and a bounded persistence queue.
use crate::{
    client::{DeviceGatewayFacade, SampleSink},
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
pub struct DeviceGatewayToolFactory {
    state: Mutex<Option<Arc<State>>>,
}
pub(crate) struct State {
    pub runtime: tokio::runtime::Runtime,
    pub facade: Arc<DeviceGatewayFacade>,
    pub sink: Arc<QueueSink>,
    stop: Arc<AtomicBool>,
    worker: Mutex<Option<JoinHandle<()>>>,
    /// Set by the first start#Subscriptions of this process, so the subscriptions are restored once per start.
    pub startup_claimed: AtomicBool,
}
pub(crate) struct QueueSink {
    sender: mpsc::SyncSender<SampleBatch>,
}
impl SampleSink for QueueSink {
    fn emit(&self, batch: SampleBatch) -> Result<(), DeviceGatewayError> {
        self.sender.try_send(batch).map_err(|e| match e {
            mpsc::TrySendError::Full(_) => DeviceGatewayError::new(
                ErrorKind::Busy,
                "Device gateway persistence queue is full; sample was not saved",
            ),
            mpsc::TrySendError::Disconnected(_) => {
                DeviceGatewayError::new(ErrorKind::Closed, "Device gateway persistence worker is closed")
            }
        })
    }
}
impl DeviceGatewayToolFactory {
    pub(crate) fn state(&self) -> Result<Arc<State>, Error> {
        self.state
            .lock()
            .map_err(|_| Error::new("Device gateway factory lock poisoned"))?
            .clone()
            .ok_or_else(|| Error::new("Device gateway factory is not initialized"))
    }
    /// Resolve once during setup. The returned API needs no ExecutionContext.
    pub fn facade(&self) -> Result<Arc<DeviceGatewayFacade>, Error> {
        Ok(self.state()?.facade.clone())
    }
}
struct DeviceGatewayTool {
    pub _facade: Arc<DeviceGatewayFacade>,
}
impl Tool for DeviceGatewayTool {}
impl ToolFactory for DeviceGatewayToolFactory {
    fn name(&self) -> &str {
        "DeviceGateway"
    }
    fn init(&self, factory: &Arc<ExecutionContextFactory>) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::new("Device gateway factory lock poisoned"))?;
        if state.is_some() {
            return Ok(());
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("device-gateway-io")
            .build()
            .map_err(|e| Error::new(e.to_string()))?;
        let (sender, receiver) = mpsc::sync_channel::<SampleBatch>(1024);
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let factory = Arc::downgrade(factory);
        let worker = std::thread::Builder::new()
            .name("device-gateway-store".into())
            .spawn(move || persist_loop(factory, receiver, stopped))
            .map_err(|e| Error::new(e.to_string()))?;
        *state = Some(Arc::new(State {
            runtime,
            facade: Arc::new(DeviceGatewayFacade::new()),
            sink: Arc::new(QueueSink { sender }),
            stop,
            worker: Mutex::new(Some(worker)),
            startup_claimed: AtomicBool::new(false),
        }));
        Ok(())
    }
    fn get_instance(&self, _: &[Value]) -> Result<Arc<dyn Tool>, Error> {
        Ok(Arc::new(DeviceGatewayTool {
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
                tracing::error!("Device gateway persistence worker panicked");
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
                if let Err(error) = crate::binding::store_batch(&ec, &batch) {
                    tracing::error!(request=%batch.request_name,error=%error,"Device gateway samples were not saved");
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) if stop.load(Ordering::Acquire) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
}
