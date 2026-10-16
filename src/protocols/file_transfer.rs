use russh::{
    client,
    keys::{PrivateKeyWithHashAlg, PublicKeyOrCertificate},
};
use russh_sftp::{client::SftpSession, protocol::OpenFlags};
use std::collections::HashMap;
use std::path::{Component, Path};
use std::sync::Arc;
use tokio::{fs, io::AsyncWriteExt};

pub struct FileTransferService;

struct SshHost {
    host: String,
    port: u16,
    known_hosts: String,
}
impl client::Handler for SshHost {
    type Error = russh::Error;
    async fn check_server_key(
        &mut self,
        key: &PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        match key {
            PublicKeyOrCertificate::PublicKey { key, .. } => Ok(
                russh::keys::check_known_hosts_path(&self.host, self.port, key, &self.known_hosts)
                    .unwrap_or(false),
            ),
            _ => Ok(false),
        }
    }
}

impl FileTransferService {
    pub fn validate_filename(filename: &str) -> Result<(), String> {
        let mut parts = Path::new(filename).components();
        if filename.is_empty()
            || filename.contains(['/', '\\', ':', '\0'])
            || !matches!(parts.next(), Some(Component::Normal(_)))
            || parts.next().is_some()
        {
            return Err("filename must be a single file name without path components".into());
        }
        Ok(())
    }

    pub async fn transfer_bytes(
        target_uri: &str,
        filename: &str,
        bytes: &[u8],
    ) -> Result<(), String> {
        Self::validate_filename(filename)?;
        let uri = target_uri.trim();
        if uri.starts_with("sftp:") {
            return tokio::time::timeout(
                std::time::Duration::from_secs(30),
                Self::sftp(uri, filename, bytes),
            )
            .await
            .map_err(|_| "SFTP transfer timed out".to_string())?;
        }
        if uri.is_empty()
            || (uri.contains("://") && !uri.starts_with("file:"))
            || uri.starts_with("seda:")
        {
            return Err("Unsupported or empty file transfer destination".into());
        }
        let dir = if uri.starts_with("file://") {
            reqwest::Url::parse(uri)
                .map_err(|_| "Invalid file URI")?
                .to_file_path()
                .map_err(|_| "Invalid local file URI")?
        } else {
            Path::new(uri.strip_prefix("file:").unwrap_or(uri)).to_path_buf()
        };
        fs::create_dir_all(&dir).await.map_err(|e| e.to_string())?;
        // Write a new temporary inode and rename: an existing destination symlink is
        // replaced, never followed. Interrupted writes do not expose a partial recipe.
        let temporary = dir.join(format!(".moqui-transfer-{}", uuid::Uuid::new_v4()));
        let destination = dir.join(filename);
        let result = async {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
                .await?;
            file.write_all(bytes).await?;
            file.sync_all().await?;
            drop(file);
            fs::rename(&temporary, destination).await
        }
        .await;
        if result.is_err() {
            let _ = fs::remove_file(&temporary).await;
        }
        result.map_err(|e| format!("File transfer failed: {e}"))
    }

    async fn sftp(uri: &str, filename: &str, bytes: &[u8]) -> Result<(), String> {
        let normalized = if uri.starts_with("sftp://") {
            uri.to_string()
        } else {
            uri.replacen("sftp:", "sftp://", 1)
        };
        let url = reqwest::Url::parse(&normalized).map_err(|_| "Invalid SFTP destination")?;
        let params: HashMap<String, String> = url.query_pairs().into_owned().collect();
        let host = url.host_str().ok_or("Missing SFTP host")?;
        let port = url.port().unwrap_or(22);
        let user = url.username();
        if user.is_empty() {
            return Err("Missing SFTP username".into());
        }
        let known_hosts = params
            .get("knownHostsFile")
            .cloned()
            .or_else(|| std::env::var("SFTP_KNOWN_HOSTS_FILE").ok())
            .ok_or("Set SFTP_KNOWN_HOSTS_FILE or knownHostsFile for SFTP host verification")?;
        let handler = SshHost {
            host: host.to_string(),
            port,
            known_hosts,
        };
        let mut session =
            client::connect(Arc::new(client::Config::default()), (host, port), handler)
                .await
                .map_err(|_| "SFTP SSH connection/host verification failed")?;
        let auth = if let Some(path) = params
            .get("privateKeyFile")
            .cloned()
            .or_else(|| std::env::var("SFTP_PRIVATE_KEY_FILE").ok())
        {
            let passphrase = std::env::var("SFTP_PRIVATE_KEY_PASSPHRASE").ok();
            let key = russh::keys::load_secret_key(path, passphrase.as_deref())
                .map_err(|_| "Unable to load SFTP private key")?;
            let hash = session
                .best_supported_rsa_hash()
                .await
                .map_err(|_| "SFTP algorithm negotiation failed")?
                .flatten();
            session
                .authenticate_publickey(user, PrivateKeyWithHashAlg::new(Arc::new(key), hash))
                .await
        } else {
            let password = params
                .get("password")
                .cloned()
                .or_else(|| std::env::var("SFTP_PASSWORD").ok())
                .ok_or("Missing SFTP credentials")?;
            session.authenticate_password(user, password).await
        }
        .map_err(|_| "SFTP authentication failed")?;
        if !auth.success() {
            return Err("SFTP authentication rejected".into());
        }
        let channel = session
            .channel_open_session()
            .await
            .map_err(|e| e.to_string())?;
        channel
            .request_subsystem(true, "sftp")
            .await
            .map_err(|e| e.to_string())?;
        let sftp = SftpSession::new(channel.into_stream())
            .await
            .map_err(|e| e.to_string())?;
        let remote = format!("{}/{}", url.path().trim_end_matches('/'), filename);
        // Match file/SFTP overwrite semantics: completion requires a successful close.
        let result = async {
            let mut file = sftp
                .open_with_flags(
                    &remote,
                    OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE,
                )
                .await
                .map_err(|e| e.to_string())?;
            file.write_all(bytes).await.map_err(|e| e.to_string())?;
            file.shutdown().await.map_err(|e| e.to_string())
        }
        .await;
        let _ = sftp.close().await;
        let _ = session
            .disconnect(russh::Disconnect::ByApplication, "", "en")
            .await;
        result
    }

    pub async fn transfer_text(
        target_uri: &str,
        filename: &str,
        content: &str,
    ) -> Result<(), String> {
        Self::transfer_bytes(target_uri, filename, content.as_bytes()).await
    }
}
