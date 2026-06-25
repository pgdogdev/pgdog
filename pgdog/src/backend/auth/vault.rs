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

#[derive(Clone, Debug)]
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
        && SystemTime::now() + vault.token_expiry_buffer() < cached.expires_at
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

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use pgdog_config::Role;
    use pgdog_config::vault::{Vault, VaultAuthMethod};
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::config::ConfigAndUsers;

    fn setup() {
        let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
    }

    fn approle_vault(url: &str) -> Vault {
        Vault {
            url: url.to_string(),
            namespace: None,
            auth_method: VaultAuthMethod::Approle,
            auth_mount: None,
            kubernetes_role: None,
            kubernetes_jwt_path: None,
            approle_role_id: Some("test-role-id".into()),
            approle_secret_id_file: None,
            client_token_ttl: None,
        }
    }

    fn set_vault_config(vault: Vault) {
        let mut config = ConfigAndUsers::default();
        config.config.vault = Some(vault);
        crate::config::set(config).unwrap();
    }

    fn make_addr(vault_path: Option<&str>) -> Address {
        Address {
            host: "127.0.0.1".into(),
            port: 5432,
            database_name: "testdb".into(),
            user: "testuser".into(),
            passwords: vec![],
            server_auth: Default::default(),
            server_iam_region: None,
            vault_path: vault_path.map(Into::into),
            vault_refresh_percent: None,
            database_number: 0,
            configured_role: Role::Primary,
        }
    }

    // ── credentials(): config-level error cases ───────────────────────────────

    #[tokio::test]
    async fn test_credentials_no_vault_config() {
        crate::config::set(ConfigAndUsers::default()).unwrap();

        let err = credentials(make_addr(Some("database/creds/role")))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("[vault] section is missing"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_credentials_no_vault_path() {
        set_vault_config(approle_vault("http://127.0.0.1:8200"));

        let err = credentials(make_addr(None)).await.unwrap_err();
        assert!(
            err.to_string().contains("vault_path"),
            "unexpected error: {err}"
        );
    }

    // ── login(): parameter validation ─────────────────────────────────────────

    #[tokio::test]
    async fn test_login_approle_missing_role_id() {
        let vault = Vault {
            url: "http://127.0.0.1:8200".into(),
            namespace: None,
            auth_method: VaultAuthMethod::Approle,
            auth_mount: None,
            kubernetes_role: None,
            kubernetes_jwt_path: None,
            approle_role_id: None,
            approle_secret_id_file: None,
            client_token_ttl: None,
        };

        let err = login(&vault).await.unwrap_err();
        assert!(
            err.to_string().contains("approle_role_id"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_login_approle_missing_secret_id() {
        let _guard = crate::test_utils::remove_env_var("VAULT_SECRET_ID");

        let vault = Vault {
            url: "http://127.0.0.1:8200".into(),
            namespace: None,
            auth_method: VaultAuthMethod::Approle,
            auth_mount: None,
            kubernetes_role: None,
            kubernetes_jwt_path: None,
            approle_role_id: Some("my-role".into()),
            approle_secret_id_file: None,
            client_token_ttl: None,
        };

        let err = login(&vault).await.unwrap_err();
        assert!(
            err.to_string().contains("VAULT_SECRET_ID"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_login_kubernetes_missing_role() {
        let vault = Vault {
            url: "http://127.0.0.1:8200".into(),
            namespace: None,
            auth_method: VaultAuthMethod::Kubernetes,
            auth_mount: None,
            kubernetes_role: None,
            kubernetes_jwt_path: None,
            approle_role_id: None,
            approle_secret_id_file: None,
            client_token_ttl: None,
        };

        let err = login(&vault).await.unwrap_err();
        assert!(
            err.to_string().contains("kubernetes_role"),
            "unexpected error: {err}"
        );
    }

    // ── login(): HTTP responses ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_login_approle_success() {
        setup();
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1/auth/approle/login"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "auth": { "client_token": "s.abc123", "lease_duration": 3600 }
            })))
            .mount(&server)
            .await;

        let _guard = crate::test_utils::set_env_var("VAULT_SECRET_ID", "my-secret");
        let vault = approle_vault(&server.uri());

        let token = login(&vault).await.unwrap();
        assert_eq!(token.token, "s.abc123");
        assert!(token.expires_at > SystemTime::now());
    }

    #[tokio::test]
    async fn test_login_non_success_response() {
        setup();
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1/auth/approle/login"))
            .respond_with(ResponseTemplate::new(403).set_body_string("permission denied"))
            .mount(&server)
            .await;

        let _guard = crate::test_utils::set_env_var("VAULT_SECRET_ID", "bad-secret");
        let vault = approle_vault(&server.uri());

        let err = login(&vault).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("403"), "expected 403 in: {msg}");
        assert!(msg.contains("permission denied"), "expected body in: {msg}");
    }

    // ── vault_token(): cache behaviour ────────────────────────────────────────

    #[tokio::test]
    async fn test_vault_token_uses_cached() {
        *VAULT_TOKEN.lock() = Some(VaultToken {
            token: "s.cached".into(),
            expires_at: SystemTime::now() + Duration::from_secs(3600),
        });

        // Port 1 is unreachable; proves no HTTP call was made.
        let vault = approle_vault("http://127.0.0.1:1");
        let tok = vault_token(&vault).await.unwrap();
        assert_eq!(tok, "s.cached");
    }

    // ── credentials(): HTTP responses ─────────────────────────────────────────

    #[tokio::test]
    async fn test_credentials_success() {
        setup();
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1/auth/approle/login"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "auth": { "client_token": "s.tok", "lease_duration": 3600 }
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/v1/database/creds/pgdog-role"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "lease_duration": 600,
                "data": { "username": "v-pgdog-abc", "password": "super-secret" }
            })))
            .mount(&server)
            .await;

        let _guard = crate::test_utils::set_env_var("VAULT_SECRET_ID", "my-secret");
        *VAULT_TOKEN.lock() = None;
        set_vault_config(approle_vault(&server.uri()));

        let fetched = credentials(make_addr(Some("database/creds/pgdog-role")))
            .await
            .unwrap();
        assert_eq!(fetched.credentials.username.as_deref(), Some("v-pgdog-abc"));
        assert_eq!(fetched.credentials.secret, "super-secret");
        assert!(fetched.expires_at > SystemTime::now());
        assert!(fetched.refresh_at.is_some());
    }

    #[tokio::test]
    async fn test_credentials_forbidden_clears_token_cache() {
        setup();
        let server = MockServer::start().await;

        *VAULT_TOKEN.lock() = Some(VaultToken {
            token: "s.stale".into(),
            expires_at: SystemTime::now() + Duration::from_secs(3600),
        });

        Mock::given(method("GET"))
            .and(path("/v1/database/creds/pgdog-role"))
            .respond_with(ResponseTemplate::new(403).set_body_string("token revoked"))
            .mount(&server)
            .await;

        set_vault_config(approle_vault(&server.uri()));

        let err = credentials(make_addr(Some("database/creds/pgdog-role")))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("403"), "unexpected error: {err}");
        assert!(
            VAULT_TOKEN.lock().is_none(),
            "token cache should be cleared after 403"
        );
    }

    #[tokio::test]
    async fn test_credentials_error_response() {
        setup();
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1/auth/approle/login"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "auth": { "client_token": "s.tok2", "lease_duration": 3600 }
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/v1/database/creds/pgdog-role"))
            .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
            .mount(&server)
            .await;

        let _guard = crate::test_utils::set_env_var("VAULT_SECRET_ID", "my-secret");
        *VAULT_TOKEN.lock() = None;
        set_vault_config(approle_vault(&server.uri()));

        let err = credentials(make_addr(Some("database/creds/pgdog-role")))
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("500"), "expected 500 in: {msg}");
        assert!(msg.contains("internal error"), "expected body in: {msg}");
    }

    // ── client(): namespace header ─────────────────────────────────────────────

    #[test]
    fn test_client_with_namespace() {
        setup();
        let vault = Vault {
            url: "http://127.0.0.1:8200".into(),
            namespace: Some("ns1/ns2".into()),
            auth_method: VaultAuthMethod::Approle,
            auth_mount: None,
            kubernetes_role: None,
            kubernetes_jwt_path: None,
            approle_role_id: None,
            approle_secret_id_file: None,
            client_token_ttl: None,
        };
        assert!(client(&vault).is_ok());
    }

    #[test]
    fn test_client_without_namespace() {
        setup();
        assert!(client(&approle_vault("http://127.0.0.1:8200")).is_ok());
    }
}
