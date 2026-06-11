//! HashiCorp Vault dynamic database credentials.
//!
//! Pools configured with `server_auth = "vault"` fetch their username and
//! password from the Vault path configured on the user
//! (e.g. `database/creds/my-role`). Credentials are cached in the global
//! [`TokenCache`](crate::backend::pool::token_cache::TokenCache) and
//! refreshed by the pool monitor after a configured percentage of the
//! lease has elapsed.

use std::time::{Duration, SystemTime};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::json;
use tracing::{debug, info};

use crate::backend::pool::token_cache::{Credentials, FetchedCredentials};
use crate::backend::{Error, pool::Address};
use crate::config::config;
use pgdog_config::vault::{DEFAULT_REFRESH_PERCENT, Vault, VaultAuthMethod};

/// Re-login this long before the Vault client token expires.
const TOKEN_EXPIRY_BUFFER: Duration = Duration::from_secs(60);

#[derive(Clone)]
struct VaultToken {
    token: String,
    expires_at: SystemTime,
}

/// Cached Vault client token, shared by all pools.
static VAULT_TOKEN: Lazy<Mutex<Option<VaultToken>>> = Lazy::new(|| Mutex::new(None));

#[derive(Deserialize)]
struct AuthResponse {
    auth: AuthData,
}

#[derive(Deserialize)]
struct AuthData {
    client_token: String,
    lease_duration: u64,
}

#[derive(Deserialize)]
struct SecretResponse {
    lease_duration: u64,
    data: SecretData,
}

#[derive(Deserialize)]
struct SecretData {
    username: String,
    password: String,
}

fn error(message: impl std::fmt::Display) -> Error {
    Error::VaultCredentials(message.to_string())
}

fn client(vault: &Vault) -> Result<reqwest::Client, Error> {
    let mut builder = reqwest::Client::builder();

    if let Some(namespace) = vault.namespace.as_deref() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "X-Vault-Namespace",
            namespace
                .parse()
                .map_err(|_| error("invalid Vault namespace"))?,
        );
        builder = builder.default_headers(headers);
    }

    builder.build().map_err(error)
}

async fn login(vault: &Vault) -> Result<VaultToken, Error> {
    let mount = vault.auth_mount();
    let url = format!(
        "{}/v1/auth/{}/login",
        vault.url.trim_end_matches('/'),
        mount
    );

    let payload = match vault.auth_method {
        VaultAuthMethod::Kubernetes => {
            let role = vault.kubernetes_role.as_deref().ok_or_else(|| {
                error(r#""kubernetes_role" is required for Vault Kubernetes auth"#)
            })?;
            let jwt = tokio::fs::read_to_string(vault.kubernetes_jwt_path())
                .await
                .map_err(|err| {
                    error(format!(
                        "failed to read service account JWT from \"{}\": {}",
                        vault.kubernetes_jwt_path(),
                        err
                    ))
                })?;
            json!({ "jwt": jwt.trim(), "role": role })
        }

        VaultAuthMethod::Approle => {
            let role_id = vault
                .approle_role_id
                .as_deref()
                .ok_or_else(|| error(r#""approle_role_id" is required for Vault AppRole auth"#))?;
            let secret_id = match vault.approle_secret_id_file.as_deref() {
                Some(path) => tokio::fs::read_to_string(path)
                    .await
                    .map(|secret| secret.trim().to_owned())
                    .map_err(|err| {
                        error(format!(
                            "failed to read AppRole secret ID from \"{}\": {}",
                            path, err
                        ))
                    })?,
                None => std::env::var("VAULT_SECRET_ID").map_err(|_| {
                    error(
                        r#"set "approle_secret_id_file" or the VAULT_SECRET_ID environment variable"#,
                    )
                })?,
            };
            json!({ "role_id": role_id, "secret_id": secret_id })
        }
    };

    let response = client(vault)?
        .post(&url)
        .json(&payload)
        .send()
        .await
        .map_err(|err| {
            error(format!(
                "Vault login request to \"{}\" failed: {}",
                url, err
            ))
        })?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(error(format!(
            "Vault login at \"{}\" returned {}: {}",
            url, status, body
        )));
    }

    let auth: AuthResponse = response
        .json()
        .await
        .map_err(|err| error(format!("invalid Vault login response: {}", err)))?;

    Ok(VaultToken {
        token: auth.auth.client_token,
        expires_at: SystemTime::now() + Duration::from_secs(auth.auth.lease_duration),
    })
}

/// Get a valid Vault client token, logging in if the cached one is
/// missing or about to expire.
async fn vault_token(vault: &Vault) -> Result<String, Error> {
    if let Some(cached) = VAULT_TOKEN.lock().clone()
        && SystemTime::now() + TOKEN_EXPIRY_BUFFER < cached.expires_at
    {
        return Ok(cached.token);
    }

    let token = login(vault).await?;
    let secret = token.token.clone();
    *VAULT_TOKEN.lock() = Some(token);
    debug!("logged into Vault");

    Ok(secret)
}

/// Fetch fresh dynamic database credentials for `addr` from Vault.
///
/// This is the raw fetcher passed to [`TokenCache::credentials_or_fetch`]
/// and called by the monitor's refresh loop. Callers should never invoke
/// it directly — go through
/// [`TokenCache::global`](crate::backend::pool::token_cache::TokenCache::global)
/// instead.
pub(crate) async fn credentials(addr: Address) -> Result<FetchedCredentials, Error> {
    let vault = config()
        .config
        .vault
        .clone()
        .ok_or_else(|| error("[vault] section is missing from pgdog.toml"))?;

    let path = addr.vault_path.as_deref().ok_or_else(|| {
        error(format!(
            r#""vault_path" is not configured for {}@{}:{}"#,
            addr.user, addr.host, addr.port
        ))
    })?;

    let token = vault_token(&vault).await?;
    let url = format!(
        "{}/v1/{}",
        vault.url.trim_end_matches('/'),
        path.trim_start_matches('/')
    );

    let response = client(&vault)?
        .get(&url)
        .header("X-Vault-Token", token)
        .send()
        .await
        .map_err(|err| {
            error(format!(
                "Vault credentials request to \"{}\" failed: {}",
                url, err
            ))
        })?;

    let status = response.status();

    // The cached Vault token may have been revoked — drop it so the next
    // attempt logs in again.
    if status == reqwest::StatusCode::FORBIDDEN {
        *VAULT_TOKEN.lock() = None;
    }

    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(error(format!(
            "Vault credentials read at \"{}\" returned {}: {}",
            url, status, body
        )));
    }

    let secret: SecretResponse = response
        .json()
        .await
        .map_err(|err| error(format!("invalid Vault credentials response: {}", err)))?;

    let lease = Duration::from_secs(secret.lease_duration);

    info!(
        user = %addr.user,
        vault_user = %secret.data.username,
        ttl_secs = secret.lease_duration,
        "fetched Vault credentials"
    );

    let refresh_percent = addr
        .vault_refresh_percent
        .unwrap_or(DEFAULT_REFRESH_PERCENT)
        .clamp(1, 80);
    let now = SystemTime::now();
    let refresh_at = now + lease.mul_f64(refresh_percent as f64 / 100.0);

    Ok(FetchedCredentials {
        credentials: Credentials {
            username: Some(secret.data.username),
            secret: secret.data.password,
        },
        expires_at: now + lease,
        refresh_at: Some(refresh_at),
    })
}
