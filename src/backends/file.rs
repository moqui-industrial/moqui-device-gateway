//! Finite file transfers. Destinations are directories, never arbitrary item paths.
use crate::{client::DeviceGatewayClient, model::*};
use async_trait::async_trait;
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};
use tokio::io::AsyncWriteExt;
#[derive(Default)]
pub struct FileClient;
static NEXT: AtomicU64 = AtomicU64::new(1);
pub fn validate_name(name: &str) -> Result<(), DeviceGatewayError> {
    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\\', '\0']) {
        return Err(DeviceGatewayError::invalid(
            "Transfer filename must be a single safe path component",
        ));
    }
    Ok(())
}
struct TempPath(PathBuf);
impl Drop for TempPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
#[async_trait]
impl DeviceGatewayClient for FileClient {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            read: false,
            write: false,
            subscribe: false,
            transfer: true,
        }
    }
    async fn execute(
        &self,
        r: &PreparedDeviceRequest,
        input: &RequestInput,
        out: &mut RequestOutput,
        deadline: Instant,
    ) -> Result<(), DeviceGatewayError> {
        if input.files.is_empty() {
            return Err(DeviceGatewayError::invalid("Transfer has no files"));
        }
        let mut names = std::collections::HashSet::new();
        for f in &input.files {
            validate_name(&f.name)?;
            if !names.insert(&f.name) {
                return Err(DeviceGatewayError::invalid("Duplicate transfer filename"));
            }
        }
        if r.connection.protocol == Protocol::Sftp {
            #[cfg(feature = "sftp")]
            return super::sftp::transfer(r, input, out, deadline).await;
            #[cfg(not(feature = "sftp"))]
            return Err(DeviceGatewayError::unsupported(
                "SFTP is not compiled into this component",
            ));
        }
        if r.connection.protocol != Protocol::File {
            return Err(DeviceGatewayError::unsupported("Unsupported transfer transport"));
        }
        if !r.connection.options.is_empty() {
            return Err(DeviceGatewayError::unsupported("Unknown local transfer options"));
        }
        let directory = r
            .connection
            .endpoint
            .strip_prefix("file:")
            .ok_or_else(|| DeviceGatewayError::invalid("File destination must start with file:"))?;
        if directory.is_empty() || directory.starts_with("//") {
            return Err(DeviceGatewayError::invalid(
                "Use file:/absolute/directory or file:relative/directory",
            ));
        }
        let directory = Path::new(directory);
        tokio::fs::create_dir_all(directory)
            .await
            .map_err(|_| DeviceGatewayError::transport("Cannot create transfer destination"))?;
        for f in &input.files {
            if Instant::now() >= deadline {
                return Err(DeviceGatewayError::timeout(!out.files.is_empty()));
            }
            let temp = TempPath(directory.join(format!(
                ".device-gateway-{}-{}.part",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            )));
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp.0)
                .await
                .map_err(|_| DeviceGatewayError::transport("Cannot create transfer temporary file"))?;
            file.write_all(&f.bytes)
                .await
                .map_err(|_| DeviceGatewayError::transport("Cannot write transfer temporary file"))?;
            file.sync_all()
                .await
                .map_err(|_| DeviceGatewayError::transport("Cannot sync transfer file"))?;
            drop(file);
            let destination = directory.join(&f.name);
            tokio::fs::rename(&temp.0, &destination)
                .await
                .map_err(|_| DeviceGatewayError::transport("Cannot publish transfer file").uncertain())?;
            out.files.push(destination.to_string_lossy().into_owned());
        }
        Ok(())
    }
}
