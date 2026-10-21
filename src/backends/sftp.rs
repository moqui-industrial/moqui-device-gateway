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

//! SFTP with an explicitly pinned host key and no implicit trust-on-first-use.
use crate::model::*;
use russh::{
    client,
    keys::{HashAlg, PrivateKeyWithHashAlg, PublicKeyOrCertificate},
};
use std::{sync::Arc, time::Instant};
use tokio::io::AsyncWriteExt;
struct KeyCheck {
    fingerprint: String,
}
impl client::Handler for KeyCheck {
    type Error = russh::Error;
    async fn check_server_key(&mut self, key: &PublicKeyOrCertificate) -> Result<bool, Self::Error> {
        Ok(key
            .public_key()
            .fingerprint(HashAlg::Sha256)
            .to_string()
            .trim_end_matches('=')
            == self.fingerprint.trim_end_matches('='))
    }
}
pub async fn transfer(
    r: &PreparedDeviceRequest,
    input: &RequestInput,
    out: &mut RequestOutput,
    deadline: Instant,
) -> Result<(), DeviceGatewayError> {
    let c = &r.connection;
    for key in c.options.keys() {
        if !matches!(key.as_str(), "host-key-fingerprint" | "private-key-file") {
            return Err(DeviceGatewayError::unsupported(format!("Unknown SFTP option: {key}")));
        }
    }
    let fingerprint = c
        .options
        .get("host-key-fingerprint")
        .filter(|v| v.starts_with("SHA256:"))
        .ok_or_else(|| DeviceGatewayError::invalid("SFTP requires a pinned SHA256: host-key-fingerprint"))?
        .clone();
    let url = url::Url::parse(&c.endpoint).map_err(|_| DeviceGatewayError::invalid("Invalid SFTP endpoint"))?;
    if url.scheme() != "sftp"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(DeviceGatewayError::invalid(
            "Use sftp://host:port/directory without credentials or query",
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| DeviceGatewayError::invalid("Missing SFTP host"))?;
    let user = c
        .credentials
        .username
        .as_deref()
        .ok_or_else(|| DeviceGatewayError::invalid("SFTP requires username"))?;
    let mut handle = tokio::time::timeout_at(
        deadline.into(),
        client::connect(
            Arc::new(client::Config::default()),
            (host, url.port().unwrap_or(22)),
            KeyCheck { fingerprint },
        ),
    )
    .await
    .map_err(|_| DeviceGatewayError::timeout(false))?
    .map_err(|_| DeviceGatewayError::transport("SFTP connection or host-key verification failed"))?;
    let auth = if let Some(password) = &c.credentials.password {
        handle.authenticate_password(user, password).await
    } else {
        let path = c
            .options
            .get("private-key-file")
            .ok_or_else(|| DeviceGatewayError::invalid("SFTP requires password or private-key-file"))?;
        let data = tokio::fs::read_to_string(path)
            .await
            .map_err(|_| DeviceGatewayError::invalid("Cannot read SFTP private key"))?;
        let key = russh::keys::decode_secret_key(&data, None)
            .map_err(|_| DeviceGatewayError::invalid("Cannot decode SFTP private key"))?;
        let hash = handle
            .best_supported_rsa_hash()
            .await
            .map_err(|_| DeviceGatewayError::transport("Cannot negotiate SSH key algorithm"))?
            .flatten();
        handle
            .authenticate_publickey(user, PrivateKeyWithHashAlg::new(Arc::new(key), hash))
            .await
    };
    if !auth
        .map_err(|_| DeviceGatewayError::transport("SFTP authentication failed"))?
        .success()
    {
        return Err(DeviceGatewayError::transport("SFTP authentication rejected"));
    }
    let channel = handle
        .channel_open_session()
        .await
        .map_err(|_| DeviceGatewayError::transport("Cannot open SSH session"))?;
    channel
        .request_subsystem(true, "sftp")
        .await
        .map_err(|_| DeviceGatewayError::transport("SFTP subsystem rejected"))?;
    let sftp = russh_sftp::client::SftpSession::new(channel.into_stream())
        .await
        .map_err(|_| DeviceGatewayError::transport("Cannot initialize SFTP"))?;
    let directory = url.path().trim_end_matches('/');
    for f in &input.files {
        if Instant::now() >= deadline {
            return Err(DeviceGatewayError::timeout(!out.files.is_empty()));
        }
        let destination = format!("{directory}/{}", f.name);
        // The file is written under a temporary name and renamed at the end, so the PLC never reads a partial
        // recipe; a failed transfer can leave the temporary file, never a partial destination. It is not replayed.
        let partial = format!("{directory}/.{}.part", f.name);
        let mut file = sftp
            .create(&partial)
            .await
            .map_err(|_| DeviceGatewayError::transport("Cannot create SFTP file").uncertain())?;
        file.write_all(&f.bytes)
            .await
            .map_err(|_| DeviceGatewayError::transport("Cannot write SFTP file").uncertain())?;
        file.shutdown()
            .await
            .map_err(|_| DeviceGatewayError::transport("Cannot close SFTP file").uncertain())?;
        // SFTP v3 rename does not replace an existing file: remove the previous version first
        if sftp.rename(partial.as_str(), destination.as_str()).await.is_err() {
            let _ = sftp.remove_file(destination.as_str()).await;
            sftp.rename(partial.as_str(), destination.as_str())
                .await
                .map_err(|_| DeviceGatewayError::transport("Cannot rename the SFTP file").uncertain())?;
        }
        out.files.push(destination);
    }
    let _ = sftp.close().await;
    let _ = handle.disconnect(russh::Disconnect::ByApplication, "", "").await;
    Ok(())
}
