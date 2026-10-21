#![cfg(feature = "moqui")]
use moqui_core::{
    context::{BootOptions, ExecutionContextFactory, Executor},
    driver::{DbConnection, DbDriver},
    entity::{EntityFacade, EntityGroup},
    transaction::{PoolConfig, TransactionManager},
};
use moqui_device_connector::DeviceConnectorToolFactory;
use moqui_lib::error::Error;
use std::{path::Path, sync::Arc};
struct NoDb;
impl DbDriver for NoDb {
    fn connect(&self) -> Result<Box<dyn DbConnection>, Error> {
        Err(Error::new("Metadata test must not query the database"))
    }
}
#[test]
fn native_registration_interface_and_factory_lifecycle() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let framework = root.join("framework");
    let component = root.join("component/moqui-device-connector");
    let device = root.join("component/moqui-device");
    std::fs::create_dir_all(framework.join("conf")).unwrap();
    std::fs::create_dir_all(component.join("service/moqui/device/connector")).unwrap();
    std::fs::create_dir_all(device.join("service/moqui/device")).unwrap();
    std::fs::write(framework.join("component.xml"), "<component name=\"framework\"/>").unwrap();
    std::fs::write(framework.join("conf/MoquiDefaultConf.xml"),"<moqui-conf><component-list><component-dir location=\"component\"/></component-list><service-facade><service-file location=\"component://moqui-device/service/moqui/device/DeviceServices.xml\"/><service-file location=\"component://moqui-device-connector/service/moqui/device/connector/DeviceConnectorServices.xml\"/></service-facade></moqui-conf>").unwrap();
    std::fs::write(device.join("component.xml"), "<component name=\"moqui-device\"/>").unwrap();
    std::fs::write(component.join("component.xml"), include_str!("../component.xml")).unwrap();
    std::fs::write(component.join("MoquiConf.xml"), include_str!("../MoquiConf.xml")).unwrap();
    let device_services = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../moqui-device/service/moqui/device/DeviceServices.xml"),
    )
    .unwrap();
    let start = device_services
        .find("    <service verb=\"run\" noun=\"DeviceRequestInternal\"")
        .unwrap();
    let end = device_services[start..].find("</service>").unwrap() + start + "</service>".len();
    std::fs::write(
        device.join("service/moqui/device/DeviceServices.xml"),
        format!("<services>{}</services>", &device_services[start..end]),
    )
    .unwrap();
    std::fs::write(
        component.join("service/moqui/device/connector/DeviceConnectorServices.xml"),
        include_str!("../service/moqui/device/connector/DeviceConnectorServices.xml"),
    )
    .unwrap();
    let factory = ExecutionContextFactory::boot_with_options(
        root,
        &framework,
        BootOptions {
            executor: Executor::Inline,
        },
    )
    .unwrap();
    let real_framework = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../runtime/base-component/framework");
    let tx = TransactionManager::new(
        Arc::new(NoDb),
        PoolConfig {
            min: 0,
            ..Default::default()
        },
    )
    .unwrap();
    let ef = EntityFacade::open(
        &real_framework,
        "transactional",
        vec![EntityGroup::new("transactional", "postgres", tx)],
    )
    .unwrap();
    factory.set_entity(ef);
    factory.init_tools().unwrap();
    factory.init_tools().unwrap();
    let tool = factory
        .get_tool_factory_typed::<DeviceConnectorToolFactory>("DeviceConnector")
        .expect("native factory");
    assert!(tool.facade().is_ok());
    let ec = factory.new_context();
    let _entered = ec.enter();
    let _authz = ec.disable_authz();
    let errors = moqui_core::service::lint(&[]).unwrap();
    assert!(errors.is_empty(), "{errors:?}");
    factory.shutdown_tools();
    assert!(tool.facade().is_err());
    factory.shutdown_tools();
}
