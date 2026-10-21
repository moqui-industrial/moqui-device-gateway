//! Native field communication with a transport API independent of Moqui facades.
//!
//! Moqui services (service/moqui/device/gateway): one run#DeviceRequest per protocol (MqttServices, OpcUaServices,
//! ModbusServices, FileServices, PlcLogServices), each an implementation of
//! moqui.device.DeviceServices.run#DeviceRequestInternal; InboundServices stores what the devices send;
//! SubscriptionServices restores the subscriptions after a start. DeviceGatewayToolFactory is the registry of
//! connections and subscriptions; each protocol is a DeviceClient (MqttDeviceClient, OpcUaDeviceClient,
//! ModbusDeviceClient, FileDeviceClient for file and SFTP).
pub mod backends;
#[cfg(feature = "moqui")]
pub mod binding;
pub mod client;
pub mod codec;
pub mod model;
pub mod payload;
#[cfg(feature = "moqui")]
pub mod tool;

pub use client::{DeviceClient, DeviceGatewayFacade, SampleSink};
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
        use crate::binding::*;
        registry.service_function("moqui_device_gateway::binding::run_mqtt_request", run_mqtt_request)?;
        registry.service_function("moqui_device_gateway::binding::run_opcua_request", run_opcua_request)?;
        registry.service_function("moqui_device_gateway::binding::run_modbus_request", run_modbus_request)?;
        registry.service_function("moqui_device_gateway::binding::run_file_request", run_file_request)?;
        registry.service_function(
            "moqui_device_gateway::binding::run_plc_log_request",
            run_plc_log_request,
        )?;
        registry.service_function("moqui_device_gateway::binding::store_samples", store_samples)?;
        registry.service_function("moqui_device_gateway::binding::subscription_state", subscription_state)
    }
}
