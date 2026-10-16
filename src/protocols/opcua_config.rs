use crate::model::RequestContext;
use opcua::client::{ClientBuilder, IdentityToken, Session};
use opcua::types::{EndpointDescription, MessageSecurityMode, UserTokenPolicy, UserTokenType};
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

pub struct Settings {
    pub endpoint: String,
    values: HashMap<String, String>,
}

impl Settings {
    pub fn parse(context: &RequestContext) -> Result<Self, String> {
        let raw = if let Some(transport) = &context.transport_config {
            if transport.starts_with("opc.") {
                transport.clone()
            } else {
                format!("opc.tcp://{transport}")
            }
        } else {
            context
                .broker_uri
                .as_deref()
                .and_then(|uri| {
                    uri.strip_prefix("milo-client:")
                        .or_else(|| uri.strip_prefix("opcua:"))
                        .or(Some(uri))
                })
                .ok_or("Missing OPC UA endpoint")?
                .to_string()
        };
        let mut url = reqwest::Url::parse(&raw).map_err(|_| "Invalid OPC UA endpoint URL")?;
        if url.scheme() != "opc.tcp" || !url.username().is_empty() || url.password().is_some() {
            return Err(
                "OPC UA requires opc.tcp and credentials in options/environment, not URL userinfo"
                    .into(),
            );
        }
        let mut values: HashMap<String, String> = url
            .query_pairs()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        url.set_query(None);
        url.set_fragment(None);
        if let Some(options) = context.options.as_deref().filter(|s| !s.trim().is_empty()) {
            if options.trim().starts_with('{') {
                let map: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_str(options).map_err(|_| "Invalid OPC UA JSON options")?;
                for (key, value) in map {
                    values.insert(
                        key,
                        value
                            .as_str()
                            .map(str::to_owned)
                            .unwrap_or_else(|| value.to_string()),
                    );
                }
            } else {
                for option in options.split(',') {
                    let (key, value) = option
                        .split_once('=')
                        .ok_or("OPC UA options require key=value")?;
                    values.insert(key.trim().into(), value.trim().into());
                }
            }
        }
        const ALLOWED: &[&str] = &[
            "securityPolicy",
            "securityMode",
            "messageSecurityMode",
            "applicationUri",
            "applicationName",
            "pkiDir",
            "certificateFile",
            "privateKeyFile",
            "username",
            "userName",
            "password",
            "usernameEnv",
            "passwordEnv",
            "identityCertificateFile",
            "identityPrivateKeyFile",
            "trustServerCerts",
            "verifyServerCerts",
        ];
        if values.keys().any(|key| !ALLOWED.contains(&key.as_str())) {
            return Err(
                "Unsupported OPC UA option; see the supported security/identity options".into(),
            );
        }
        Ok(Self {
            endpoint: url.into(),
            values,
        })
    }
    fn value(&self, key: &str, default: &str) -> String {
        self.values
            .get(key)
            .cloned()
            .unwrap_or_else(|| default.into())
    }
    fn flag(&self, key: &str, default: bool) -> Result<bool, String> {
        self.values
            .get(key)
            .map(|value| {
                value
                    .parse()
                    .map_err(|_| format!("{key} must be true or false"))
            })
            .unwrap_or(Ok(default))
    }
    fn secret(&self, key: &str, alias: &str, default_env: &str) -> Result<Option<String>, String> {
        if let Some(name) = self.values.get(&format!("{key}Env")) {
            return std::env::var(name)
                .map(Some)
                .map_err(|_| format!("Configured {key} environment variable is missing"));
        }
        Ok(self
            .values
            .get(key)
            .or_else(|| self.values.get(alias))
            .cloned()
            .or_else(|| std::env::var(default_env).ok()))
    }
}

pub async fn connect(
    context: &RequestContext,
) -> Result<(Arc<Session>, super::SessionLoop), String> {
    let settings = Settings::parse(context)?;
    let policy = settings.value("securityPolicy", "None");
    let policy = policy.rsplit('#').next().unwrap_or(&policy);
    if ![
        "None",
        "Basic256Sha256",
        "Aes128_Sha256_RsaOaep",
        "Aes256_Sha256_RsaPss",
    ]
    .contains(&policy)
    {
        return Err("Unsupported/deprecated OPC UA securityPolicy".into());
    }
    let mode = match settings
        .values
        .get("securityMode")
        .or_else(|| settings.values.get("messageSecurityMode"))
        .map(String::as_str)
        .unwrap_or("None")
    {
        "None" => MessageSecurityMode::None,
        "Sign" => MessageSecurityMode::Sign,
        "SignAndEncrypt" => MessageSecurityMode::SignAndEncrypt,
        _ => return Err("securityMode must be None, Sign or SignAndEncrypt".into()),
    };
    if (policy == "None") != (mode == MessageSecurityMode::None) {
        return Err("securityPolicy and securityMode must both enable or disable security".into());
    }
    let timeout = Duration::from_secs(context.timeout.unwrap_or(30).max(1) as u64);
    let pki = PathBuf::from(settings.value("pkiDir", "runtime/opcua-pki"));
    let mut builder = ClientBuilder::new()
        .application_name(settings.value("applicationName", "moqui-device-gateway"))
        .application_uri(settings.value("applicationUri", "urn:moqui:device:gateway"))
        .pki_dir(&pki)
        .trust_server_certs(settings.flag("trustServerCerts", false)?)
        .verify_server_certs(settings.flag("verifyServerCerts", true)?)
        .create_sample_keypair(false)
        .request_timeout(timeout)
        .session_retry_limit(3);
    match (
        settings.values.get("certificateFile"),
        settings.values.get("privateKeyFile"),
    ) {
        (Some(cert), Some(key)) => {
            if !pki.join(cert).is_file() || !pki.join(key).is_file() {
                return Err("OPC UA client certificate/key file is missing".into());
            }
            builder = builder.certificate_path(cert).private_key_path(key);
        }
        (None, None) if mode == MessageSecurityMode::None => {}
        _ => return Err("Secure OPC UA requires certificateFile and privateKeyFile".into()),
    }
    let username = settings.secret("username", "userName", "OPCUA_USERNAME")?;
    let password = settings.secret("password", "password", "OPCUA_PASSWORD")?;
    let (identity, token_type) = match (
        username,
        password,
        settings.values.get("identityCertificateFile"),
        settings.values.get("identityPrivateKeyFile"),
    ) {
        (None, None, None, None) => (IdentityToken::Anonymous, UserTokenType::Anonymous),
        (Some(user), Some(password), None, None) => {
            if mode != MessageSecurityMode::SignAndEncrypt {
                return Err("Username authentication requires SignAndEncrypt".into());
            }
            (
                IdentityToken::new_user_name(user, password),
                UserTokenType::UserName,
            )
        }
        (None, None, Some(cert), Some(key)) => (
            IdentityToken::new_x509_path(cert, key)
                .map_err(|_| "Cannot load OPC UA user identity certificate/key")?,
            UserTokenType::Certificate,
        ),
        _ => return Err("Incomplete or conflicting OPC UA identity configuration".into()),
    };
    let mut token = UserTokenPolicy::anonymous();
    token.token_type = token_type;
    let policy_uri = format!("http://opcfoundation.org/UA/SecurityPolicy#{policy}");
    let endpoint: EndpointDescription =
        (settings.endpoint.as_str(), policy_uri.as_str(), mode, token).into();
    let mut client = builder
        .client()
        .map_err(|_| "Invalid OPC UA client configuration")?;
    let (session, event_loop) = tokio::time::timeout(
        timeout,
        client.connect_to_matching_endpoint(endpoint, identity),
    )
    .await
    .map_err(|_| "OPC UA endpoint discovery timed out")?
    .map_err(|status| format!("OPC UA endpoint discovery failed: {status:?}"))?;
    let guard = super::SessionLoop {
        session: session.clone(),
        task: Some(event_loop.spawn()),
    };
    if !tokio::time::timeout(timeout, session.wait_for_connection())
        .await
        .map_err(|_| "OPC UA session connection timed out")?
    {
        return Err("OPC UA session authentication/connection failed".into());
    }
    Ok((session, guard))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn removes_options_from_endpoint_and_rejects_unknown_settings() {
        let mut context:RequestContext=serde_json::from_value(serde_json::json!({"request_name":"test","request_type_enum_id":"DrtRead","broker_uri":"milo-client:opc.tcp://localhost:4840/test?securityPolicy=Basic256Sha256&securityMode=SignAndEncrypt","items":[]})).unwrap();
        let parsed = Settings::parse(&context).unwrap();
        assert_eq!(parsed.endpoint, "opc.tcp://localhost:4840/test");
        assert_eq!(parsed.value("securityMode", ""), "SignAndEncrypt");
        context.options = Some("{\"silentlyIgnoredSecurity\":true}".into());
        assert!(Settings::parse(&context).is_err());
    }
}
