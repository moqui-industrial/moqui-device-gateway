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
