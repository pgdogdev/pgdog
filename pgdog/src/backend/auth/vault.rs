//! HashiCorp Vault dynamic database credentials for backend pools.
//!
//! Pools configured with `server_auth = "vault"` fetch their username and
//! password from a Vault database secrets engine path (e.g.
//! `database/creds/my-role`). Credentials are cached in the global
//! [`TokenCache`](crate::backend::pool::token_cache::TokenCache) and
//! refreshed by the pool monitor after a configured percentage of the
//! lease.
//!
//! The Vault login/token cache itself lives in
//! [`crate::auth::vault`], shared with static role client-auth
//! verification.

use std::time::{Duration, SystemTime};

use serde::Deserialize;
use tracing::info;

use crate::auth::vault::{VAULT_TOKEN, client, error, vault_token};
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
    use pgdog_config::Role;
    use pgdog_config::vault::{Vault, VaultAuthMethod};
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::auth::vault::VaultToken;
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
}
