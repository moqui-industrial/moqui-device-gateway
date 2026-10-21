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

pub use client::{DeviceGatewayClient, DeviceGatewayFacade, SampleSink};
pub use model::*;
#[cfg(feature = "moqui")]
pub use tool::DeviceGatewayToolFactory;

#[cfg(feature = "moqui")]
mod native {
    use moqui_core::{
        context::Error,
        native::{NativeComponent, NativeRegistry, submit_component},
    };
    submit_component! { NativeComponent { name: "moqui-device-gateway", register } }
    fn register(registry: &mut NativeRegistry) -> Result<(), Error> {
        registry.tool("moqui_device_gateway::DeviceGatewayToolFactory", || {
            std::sync::Arc::new(crate::tool::DeviceGatewayToolFactory::default())
        })?;
        registry.service_function(
            "moqui_device_gateway::binding::run_request",
            crate::binding::run_request,
        )
    }
}
