mod common;
use moqui_device_gateway::*;
use std::sync::Arc;
#[tokio::test]
async fn binary_file_transfer_and_path_rejection() {
    let dir = tempfile::tempdir().unwrap();
    let r = Arc::new(common::request(
        Protocol::File,
        format!("file:{}", dir.path().display()),
        Operation::Transfer,
    ));
    let f = Arc::new(DeviceGatewayFacade::new());
    let mut out = RequestOutput::default();
    let mut input = RequestInput {
        files: vec![TransferFile {
            name: "program.bin".into(),
            bytes: vec![0, 255, 128, 10],
        }],
        ..Default::default()
    };
    assert_eq!(
        f.execute(r.clone(), &input, &mut out, r.deadline(), None)
            .await
            .unwrap(),
        ExecutionOutcome::Completed
    );
    assert_eq!(
        std::fs::read(dir.path().join("program.bin")).unwrap(),
        input.files[0].bytes
    );
    input.files[0].name = "../escape".into();
    assert_eq!(
        f.execute(r.clone(), &input, &mut out, r.deadline(), None)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Invalid
    );
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    f.close().await;
}
