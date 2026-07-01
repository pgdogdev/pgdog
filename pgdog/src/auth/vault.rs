//! HashiCorp Vault client and static role password verification.
//!
//! This module owns the Vault login/token machinery shared by every Vault
//! consumer in the codebase:
//!
//! - **Client authentication** — users configured with `client_vault_path`
//!   (e.g. `database/static-creds/my-role`) have their client-supplied
//!   password verified against the current password held by Vault for that
//!   static role. Results are cached for the role's rotation TTL.
//! - **Backend pools** (`src/backend/auth/vault.rs`) reuse the login/token
//!   cache here to fetch dynamic database credentials.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::json;
use tracing::debug;

use crate::backend::Error;
use crate::config::config;
use pgdog_config::vault::{Vault, VaultAuthMethod};

#[derive(Clone, Debug)]
pub(crate) struct VaultToken {
    pub(crate) token: String,
    pub(crate) expires_at: SystemTime,
}

/// Cached Vault client token, shared by all pools.
pub(crate) static VAULT_TOKEN: Lazy<Mutex<Option<VaultToken>>> = Lazy::new(|| Mutex::new(None));

// ── Static role client-auth cache ────────────────────────────────────────────

/// How early to evict a static role password before its rotation TTL expires,
/// to reduce the chance of serving a stale password right at the rotation boundary.
const STATIC_CACHE_BUFFER: Duration = Duration::from_secs(10);

struct CachedStaticPassword {
    password: String,
    expires_at: SystemTime,
}

static CLIENT_PASSWORD_CACHE: Lazy<Mutex<HashMap<String, CachedStaticPassword>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Deserialize)]
struct AuthResponse {
    auth: AuthData,
}

#[derive(Deserialize)]
struct AuthData {
    client_token: String,
    lease_duration: u64,
}

/// Response from a Vault static database role (`database/static-creds/<role>`).
///
/// Unlike dynamic leases, `lease_duration` is always 0; `data.ttl` is the
/// seconds remaining until Vault rotates the password.
#[derive(Deserialize)]
struct StaticSecretResponse {
    data: StaticSecretData,
}

#[derive(Deserialize)]
struct StaticSecretData {
    password: String,
    /// Seconds until Vault rotates the password.
    ttl: u64,
}

pub(crate) fn error(message: impl std::fmt::Display) -> Error {
    Error::VaultCredentials(message.to_string())
}

pub(crate) fn client(vault: &Vault) -> Result<reqwest::Client, Error> {
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

pub(crate) async fn login(vault: &Vault) -> Result<VaultToken, Error> {
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
pub(crate) async fn vault_token(vault: &Vault) -> Result<String, Error> {
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

/// Return the current password for a Vault static database role, using a
/// per-path cache keyed by `vault_path`.
///
/// The cached value is evicted `STATIC_CACHE_BUFFER` before the role's
/// rotation TTL expires so the next connection after a rotation picks up
/// the new password.
pub(crate) async fn static_client_password(vault_path: &str) -> Result<String, Error> {
    if let Some(cached) = CLIENT_PASSWORD_CACHE.lock().get(vault_path) {
        if SystemTime::now() < cached.expires_at {
            return Ok(cached.password.clone());
        }
    }

    let vault = config()
        .config
        .vault
        .clone()
        .ok_or_else(|| error("[vault] section is missing from pgdog.toml"))?;

    let token = vault_token(&vault).await?;
    let url = format!(
        "{}/v1/{}",
        vault.url.trim_end_matches('/'),
        vault_path.trim_start_matches('/')
    );

    let response = client(&vault)?
        .get(&url)
        .header("X-Vault-Token", token)
        .send()
        .await
        .map_err(|err| {
            error(format!(
                "Vault static credentials request to \"{}\" failed: {}",
                url, err
            ))
        })?;

    let status = response.status();

    if status == reqwest::StatusCode::FORBIDDEN {
        *VAULT_TOKEN.lock() = None;
    }

    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(error(format!(
            "Vault static credentials read at \"{}\" returned {}: {}",
            url, status, body
        )));
    }

    let secret: StaticSecretResponse = response.json().await.map_err(|err| {
        error(format!(
            "invalid Vault static credentials response: {}",
            err
        ))
    })?;

    let ttl = Duration::from_secs(secret.data.ttl);
    let expires_at = SystemTime::now() + ttl.saturating_sub(STATIC_CACHE_BUFFER);

    debug!(
        vault_path,
        ttl_secs = secret.data.ttl,
        "fetched Vault static role password"
    );

    CLIENT_PASSWORD_CACHE.lock().insert(
        vault_path.to_owned(),
        CachedStaticPassword {
            password: secret.data.password.clone(),
            expires_at,
        },
    );

    Ok(secret.data.password)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

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

    // ── static_client_password(): config-level error cases ─────────────────────

    #[tokio::test]
    async fn test_static_client_password_no_vault_config() {
        crate::config::set(ConfigAndUsers::default()).unwrap();

        let err = static_client_password("database/static-creds/no-such-role")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("[vault] section is missing"),
            "unexpected error: {err}"
        );
    }

    // ── static_client_password(): cache behaviour ───────────────────────────────

    #[tokio::test]
    async fn test_static_client_password_uses_cache() {
        CLIENT_PASSWORD_CACHE.lock().insert(
            "database/static-creds/cached-role".into(),
            CachedStaticPassword {
                password: "cached-pw".into(),
                expires_at: SystemTime::now() + Duration::from_secs(3600),
            },
        );

        // No Vault config is set; proves the cache hit short-circuits
        // before any config lookup or HTTP call.
        let pw = static_client_password("database/static-creds/cached-role")
            .await
            .unwrap();
        assert_eq!(pw, "cached-pw");
    }

    #[tokio::test]
    async fn test_static_client_password_expired_cache_refetches() {
        setup();
        let server = MockServer::start().await;

        CLIENT_PASSWORD_CACHE.lock().insert(
            "database/static-creds/expired-role".into(),
            CachedStaticPassword {
                password: "stale-pw".into(),
                expires_at: SystemTime::now() - Duration::from_secs(1),
            },
        );

        Mock::given(method("POST"))
            .and(path("/v1/auth/approle/login"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "auth": { "client_token": "s.tok3", "lease_duration": 3600 }
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/v1/database/static-creds/expired-role"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": { "password": "fresh-pw", "ttl": 3600 }
            })))
            .mount(&server)
            .await;

        let _guard = crate::test_utils::set_env_var("VAULT_SECRET_ID", "my-secret");
        *VAULT_TOKEN.lock() = None;
        set_vault_config(approle_vault(&server.uri()));

        let pw = static_client_password("database/static-creds/expired-role")
            .await
            .unwrap();
        assert_eq!(pw, "fresh-pw");
    }

    // ── static_client_password(): HTTP responses ────────────────────────────────

    #[tokio::test]
    async fn test_static_client_password_success() {
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
            .and(path("/v1/database/static-creds/pgdog-static-role"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": { "password": "rotated-secret", "ttl": 3600 }
            })))
            .mount(&server)
            .await;

        let _guard = crate::test_utils::set_env_var("VAULT_SECRET_ID", "my-secret");
        *VAULT_TOKEN.lock() = None;
        set_vault_config(approle_vault(&server.uri()));

        let pw = static_client_password("database/static-creds/pgdog-static-role")
            .await
            .unwrap();
        assert_eq!(pw, "rotated-secret");
        assert_eq!(
            CLIENT_PASSWORD_CACHE
                .lock()
                .get("database/static-creds/pgdog-static-role")
                .unwrap()
                .password,
            "rotated-secret"
        );
    }

    #[tokio::test]
    async fn test_static_client_password_forbidden_clears_token_cache() {
        setup();
        let server = MockServer::start().await;

        *VAULT_TOKEN.lock() = Some(VaultToken {
            token: "s.stale".into(),
            expires_at: SystemTime::now() + Duration::from_secs(3600),
        });

        Mock::given(method("GET"))
            .and(path("/v1/database/static-creds/forbidden-role"))
            .respond_with(ResponseTemplate::new(403).set_body_string("token revoked"))
            .mount(&server)
            .await;

        set_vault_config(approle_vault(&server.uri()));

        let err = static_client_password("database/static-creds/forbidden-role")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("403"), "unexpected error: {err}");
        assert!(
            VAULT_TOKEN.lock().is_none(),
            "token cache should be cleared after 403"
        );
    }

    #[tokio::test]
    async fn test_static_client_password_error_response() {
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
            .and(path("/v1/database/static-creds/error-role"))
            .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
            .mount(&server)
            .await;

        let _guard = crate::test_utils::set_env_var("VAULT_SECRET_ID", "my-secret");
        *VAULT_TOKEN.lock() = None;
        set_vault_config(approle_vault(&server.uri()));

        let err = static_client_password("database/static-creds/error-role")
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("500"), "expected 500 in: {msg}");
        assert!(msg.contains("internal error"), "expected body in: {msg}");
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
