//! Native field communication with a transport API independent of Moqui facades.
pub mod backends;
#[cfg(feature = "moqui")]
pub mod binding;
pub mod client;
pub mod codec;
pub mod model;
pub mod payload;
#[cfg(feature = "moqui")]
pub mod tool;

pub use client::{DeviceConnectorClient, DeviceConnectorFacade, SampleSink};
pub use model::*;
#[cfg(feature = "moqui")]
pub use tool::DeviceConnectorToolFactory;

#[cfg(feature = "moqui")]
mod native {
    use moqui_core::{
        context::Error,
        native::{NativeComponent, NativeRegistry, submit_component},
    };
    submit_component! { NativeComponent { name: "moqui-device-connector", register } }
    fn register(registry: &mut NativeRegistry) -> Result<(), Error> {
        registry.tool("moqui_device_connector::DeviceConnectorToolFactory", || {
            std::sync::Arc::new(crate::tool::DeviceConnectorToolFactory::default())
        })?;
        registry.service_function(
            "moqui_device_connector::binding::run_request",
            crate::binding::run_request,
        )
    }
}
