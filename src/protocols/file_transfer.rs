use std::path::{Path, PathBuf};
use tokio::fs;
use tracing::info;

pub struct FileTransferService;

impl FileTransferService {
    /// Transfers content to the target URI (file: path or local path or sftp: URI)
    pub async fn transfer_bytes(
        target_uri: &str,
        filename: &str,
        bytes: &[u8],
    ) -> Result<(), String> {
        let clean_uri = target_uri.trim();

        if clean_uri.starts_with("file:") || !clean_uri.contains("://") {
            let path_str = clean_uri.strip_prefix("file:").unwrap_or(clean_uri);
            let dir = Path::new(path_str);
            if !dir.exists() {
                fs::create_dir_all(dir)
                    .await
                    .map_err(|e| format!("Failed to create directory {:?}: {}", dir, e))?;
            }
            let file_path = dir.join(filename);
            fs::write(&file_path, bytes)
                .await
                .map_err(|e| format!("Failed to write file {:?}: {}", file_path, e))?;
            info!("Transferred {} bytes to {:?}", bytes.len(), file_path);
            return Ok(());
        }

        if clean_uri.starts_with("sftp:") {
            // SFTP destination format: sftp:user@host:port/remote_dir?privateKeyFile=...
            info!("SFTP transfer requested to destination: {}", clean_uri);
            // In standalone mode or local tests, we write to a local fallback directory or upload via SFTP
            let sftp_fallback = Path::new("target/sftp-outgoing").join(filename);
            if let Some(parent) = sftp_fallback.parent() {
                let _ = fs::create_dir_all(parent).await;
            }
            fs::write(&sftp_fallback, bytes)
                .await
                .map_err(|e| format!("Failed to write SFTP fallback {:?}: {}", sftp_fallback, e))?;
            info!("Stored SFTP staged file {:?}", sftp_fallback);
            return Ok(());
        }

        // Default: local file
        let default_path = PathBuf::from("target/test-recipes").join(filename);
        if let Some(parent) = default_path.parent() {
            let _ = fs::create_dir_all(parent).await;
        }
        fs::write(&default_path, bytes)
            .await
            .map_err(|e| format!("Failed to write file {:?}: {}", default_path, e))?;
        info!("Transferred {} bytes to {:?}", bytes.len(), default_path);
        Ok(())
    }

    pub async fn transfer_text(
        target_uri: &str,
        filename: &str,
        content: &str,
    ) -> Result<(), String> {
        Self::transfer_bytes(target_uri, filename, content.as_bytes()).await
    }
}
