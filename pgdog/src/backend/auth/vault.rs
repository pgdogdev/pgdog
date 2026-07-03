//! HashiCorp Vault dynamic and static database credentials for backend pools.
//!
//! Pools configured with `server_auth = "vault_dynamic"` fetch their
//! username and password from a Vault database secrets engine path.
//! Vault generates the username, so credentials are cached in the global
//! [`TokenCache`](crate::backend::pool::token_cache::TokenCache) via
//! [`TokenCache::credentials_or_fetch`] and proactively refreshed by the pool
//! monitor after a configured percentage of the lease.
//!
//! Pools configured with `server_auth = "vault_static"` fetch the current
//! password for a Vault static database role instead.
//! The username is operator-supplied (like RDS IAM and Azure Workload
//! Identity) so credentials are cached via
//! [`TokenCache::get_or_fetch_with_refresh`]. Unlike RDS/Azure tokens, Vault
//! only issues a new static-role password once the old one's TTL actually
//! expires. That's why [`static_backend_credentials`] schedules its refresh
//! at expiry rather than ahead of it.
//!
//! The Vault login/token cache itself lives in
//! [`crate::auth::vault`], shared with static role client-auth verification.

use std::time::{Duration, SystemTime};

use serde::Deserialize;
use tracing::info;

use crate::auth::vault::{StaticSecretResponse, error, fetch_secret};
use crate::backend::pool::token_cache::{Credentials, FetchedCredentials};
use crate::backend::{Error, pool::Address};
use crate::config::config;
use pgdog_config::vault::DEFAULT_REFRESH_PERCENT;

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

/// Vault path and config lookup shared by every backend credential fetcher.
fn vault_and_path(addr: &Address) -> Result<(pgdog_config::vault::Vault, &str), Error> {
    let vault = config()
        .config
        .vault
        .clone()
        .ok_or_else(|| error("[vault] section is missing from pgdog.toml"))?;

    let path = addr.vault_path.as_deref().ok_or_else(|| {
        error(format!(
            r#""server_vault_path" is not configured for {}@{}:{}"#,
            addr.user, addr.host, addr.port
        ))
    })?;

    Ok((vault, path))
}

/// Compute when to refresh a credential given its total lifetime, clamping
/// `refresh_percent` to the 1-80% range.
fn scheduled_refresh(
    now: SystemTime,
    lifetime: Duration,
    refresh_percent: Option<u8>,
) -> SystemTime {
    let refresh_percent = refresh_percent
        .unwrap_or(DEFAULT_REFRESH_PERCENT)
        .clamp(1, 80);
    now + lifetime.mul_f64(refresh_percent as f64 / 100.0)
}

/// Fetch fresh dynamic database credentials for `addr` from Vault.
///
/// This is the raw fetcher passed to [`TokenCache::credentials_or_fetch`]
/// and called by the monitor's refresh loop. Callers should never invoke
/// it directly — go through
/// [`TokenCache::global`](crate::backend::pool::token_cache::TokenCache::global)
/// instead.
pub(crate) async fn credentials(addr: Address) -> Result<FetchedCredentials, Error> {
    let (vault, path) = vault_and_path(&addr)?;

    let secret: SecretResponse = fetch_secret(&vault, path).await?;

    let lease = Duration::from_secs(secret.lease_duration);

    info!(
        user = %addr.user,
        vault_user = %secret.data.username,
        ttl_secs = secret.lease_duration,
        "fetched Vault credentials"
    );

    let now = SystemTime::now();
    let refresh_at = scheduled_refresh(now, lease, addr.vault_refresh_percent);

    Ok(FetchedCredentials {
        credentials: Credentials {
            username: Some(secret.data.username),
            secret: secret.data.password,
        },
        expires_at: now + lease,
        refresh_at: Some(refresh_at),
    })
}

/// Minimum time between refresh attempts for a Vault static role.
///
/// This floor guards against hammering Vault when the
/// observed TTL is very small — e.g. read close to the rotation boundary,
/// or Vault's actual rotation lagging slightly behind the TTL it reported.
const STATIC_ROLE_MIN_REFRESH_BACKOFF: Duration = Duration::from_secs(1);

/// Fetch the current password for a Vault static database role, for use as
/// a backend (server-side) connection password.
///
/// Unlike dynamic credentials, the username is fixed and supplied in the
/// config. This is the raw fetcher passed to
/// [`TokenCache::get_or_fetch_with_refresh`] and called by the monitor's
/// refresh loop. Callers should never invoke it directly — go through
/// [`TokenCache::global`](crate::backend::pool::token_cache::TokenCache::global)
/// instead.
///
/// Returns `(password, expires_at, refresh_at)`. `refresh_at` is `expires_at`
/// itself (clamped to at least [`STATIC_ROLE_MIN_REFRESH_BACKOFF`] from now)
/// rather than an early buffer, since Vault won't have a fresh password to
/// give out before then.
pub(crate) async fn static_backend_credentials(
    addr: Address,
) -> Result<(String, SystemTime, SystemTime), Error> {
    let (vault, path) = vault_and_path(&addr)?;

    let secret: StaticSecretResponse = fetch_secret(&vault, path).await?;

    info!(
        user = %addr.user,
        ttl_secs = secret.data.ttl,
        "fetched Vault static backend credentials"
    );

    let now = SystemTime::now();
    let expires_at = now + Duration::from_secs(secret.data.ttl);
    let refresh_at = expires_at.max(now + STATIC_ROLE_MIN_REFRESH_BACKOFF);

    Ok((secret.data.password, expires_at, refresh_at))
}

#[cfg(test)]
mod tests {
    use pgdog_config::Role;
    use pgdog_config::vault::{Vault, VaultAuthMethod};
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::auth::vault::{VAULT_TOKEN, VaultToken};
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

    // ── static_backend_credentials(): config-level error cases ────────────────

    #[tokio::test]
    async fn test_static_backend_credentials_no_vault_config() {
        crate::config::set(ConfigAndUsers::default()).unwrap();

        let err = static_backend_credentials(make_addr(Some("database/static-creds/role")))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("[vault] section is missing"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_static_backend_credentials_no_vault_path() {
        set_vault_config(approle_vault("http://127.0.0.1:8200"));

        let err = static_backend_credentials(make_addr(None))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("vault_path"),
            "unexpected error: {err}"
        );
    }

    // ── static_backend_credentials(): HTTP responses ───────────────────────────

    #[tokio::test]
    async fn test_static_backend_credentials_success() {
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
                "data": { "username": "testuser", "password": "vault-rotated-pass", "ttl": 600 }
            })))
            .mount(&server)
            .await;

        let _guard = crate::test_utils::set_env_var("VAULT_SECRET_ID", "my-secret");
        *VAULT_TOKEN.lock() = None;
        set_vault_config(approle_vault(&server.uri()));

        let (password, expires_at, refresh_at) =
            static_backend_credentials(make_addr(Some("database/static-creds/pgdog-static-role")))
                .await
                .unwrap();
        assert_eq!(password, "vault-rotated-pass");
        assert!(expires_at > SystemTime::now());
        assert!(refresh_at > SystemTime::now());
    }

    #[tokio::test]
    async fn test_static_backend_credentials_forbidden_clears_token_cache() {
        setup();
        let server = MockServer::start().await;

        *VAULT_TOKEN.lock() = Some(VaultToken {
            token: "s.stale".into(),
            expires_at: SystemTime::now() + Duration::from_secs(3600),
        });

        Mock::given(method("GET"))
            .and(path("/v1/database/static-creds/pgdog-static-role"))
            .respond_with(ResponseTemplate::new(403).set_body_string("token revoked"))
            .mount(&server)
            .await;

        set_vault_config(approle_vault(&server.uri()));

        let err =
            static_backend_credentials(make_addr(Some("database/static-creds/pgdog-static-role")))
                .await
                .unwrap_err();
        assert!(err.to_string().contains("403"), "unexpected error: {err}");
        assert!(
            VAULT_TOKEN.lock().is_none(),
            "token cache should be cleared after 403"
        );
    }

    #[tokio::test]
    async fn test_static_backend_credentials_error_response() {
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
            .and(path("/v1/database/static-creds/pgdog-static-role"))
            .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
            .mount(&server)
            .await;

        let _guard = crate::test_utils::set_env_var("VAULT_SECRET_ID", "my-secret");
        *VAULT_TOKEN.lock() = None;
        set_vault_config(approle_vault(&server.uri()));

        let err =
            static_backend_credentials(make_addr(Some("database/static-creds/pgdog-static-role")))
                .await
                .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("500"), "expected 500 in: {msg}");
        assert!(msg.contains("internal error"), "expected body in: {msg}");
    }
}
