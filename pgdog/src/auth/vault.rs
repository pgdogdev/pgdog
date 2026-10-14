//! HashiCorp Vault client and static role password verification.
//!
//! This module owns the Vault login/token machinery shared by every Vault
//! consumer in the codebase:
//!
//! - **Client authentication** — users configured with `vault_path`
//!   retrieve their password from Vault and results are cached for the role's rotation TTL.
//! - **Backend pools** (`src/backend/auth/vault.rs`) reuse the login/token
//!   cache here to fetch dynamic database credentials.

use std::collections::HashMap;
use std::sync::{Arc, Once};
use std::time::{Duration, Instant};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use tracing::{debug, warn};

use crate::backend::auth::vault::evict_static_backend_credentials;
use crate::backend::databases::databases;
use crate::backend::{Error, reload_signal};
use crate::config::config;
use pgdog_config::users::PasswordKind;
use pgdog_config::vault::{Vault, VaultAuthMethod};

/// Cached Vault client token, shared by all pools.
pub(crate) static VAULT_TOKEN: Lazy<Mutex<Option<VaultToken>>> = Lazy::new(|| Mutex::new(None));

#[derive(Clone, Debug)]
pub(crate) struct VaultToken {
    pub(crate) token: String,
    pub(crate) expires_at: Instant,
}

#[derive(Deserialize)]
struct AuthResponse {
    auth: AuthData,
}

#[derive(Deserialize)]
struct AuthData {
    client_token: String,
    lease_duration: u64,
}

/// Static role client-auth cache
struct CachedStaticPassword {
    password: String,
    fetched_at: Instant,
    expires_at: Instant,
}

/// Safety margin subtracted from the static role TTL, so the cache expires
/// slightly before Vault rotates the password.
const STATIC_TTL_MARGIN: Duration = Duration::from_secs(1);

/// Minimum age of a cached static password before a failed client auth may
/// evict it. Bounds the Vault request rate a client sending wrong passwords
/// can cause.
const MIN_REFETCH_INTERVAL: Duration = Duration::from_secs(5);

/// Per-path async mutex — serialises Vault fetches to prevent thundering herd on cache expiry.
static FETCH_LOCKS: Lazy<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

static CLIENT_PASSWORD_CACHE: Lazy<Mutex<HashMap<String, CachedStaticPassword>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Response from a Vault static database role (`database/static-creds/<role>`).
///
/// Unlike dynamic leases, `lease_duration` is always 0; `data.ttl` is the
/// seconds remaining until Vault rotates the password.
///
/// `pub(crate)` because [`crate::backend::auth::vault::static_backend_credentials`] deserializes into it directly.
#[derive(Deserialize)]
pub(crate) struct StaticSecretResponse {
    pub(crate) data: StaticSecretData,
}

#[derive(Deserialize)]
pub(crate) struct StaticSecretData {
    pub(crate) password: String,
    /// Seconds until Vault rotates the password.
    pub(crate) ttl: u64,
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
        expires_at: Instant::now() + Duration::from_secs(auth.auth.lease_duration),
    })
}

/// Get a valid Vault client token, logging in if the cached one is
/// missing or about to expire.
pub(crate) async fn vault_token(vault: &Vault) -> Result<String, Error> {
    if let Some(cached) = VAULT_TOKEN.lock().clone()
        && Instant::now() + vault.token_expiry_buffer() < cached.expires_at
    {
        return Ok(cached.token);
    }

    let token = login(vault).await?;
    let secret = token.token.clone();
    *VAULT_TOKEN.lock() = Some(token);
    debug!("logged into Vault");

    Ok(secret)
}

/// Fetch and parse a JSON secret from `path` in Vault.
///
/// Handles token acquisition, a `403` response by dropping the cached
/// client token (so the next attempt logs in again), and non-success responses. `context`
pub(crate) async fn fetch_secret<T: DeserializeOwned>(
    vault: &Vault,
    path: &str,
) -> Result<T, Error> {
    let token = vault_token(vault).await?;
    let url = format!(
        "{}/v1/{}",
        vault.url.trim_end_matches('/'),
        path.trim_start_matches('/')
    );

    let response = client(vault)?
        .get(&url)
        .header("X-Vault-Token", token)
        .send()
        .await
        .map_err(|err| error(format!("request to \"{}\" failed: {}", url, err)))?;

    let status = response.status();

    if status == reqwest::StatusCode::FORBIDDEN {
        *VAULT_TOKEN.lock() = None;
    }

    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(error(format!(
            "read at \"{}\" returned {}: {}",
            url, status, body
        )));
    }

    response
        .json()
        .await
        .map_err(|err| error(format!("{} invalid response: {}", url, err)))
}

/// Return the current password for a Vault static database role, using a
/// per-path cache keyed by `vault_path`.
pub(crate) async fn static_client_password(vault_path: &str) -> Result<String, Error> {
    if let Some(cached) = CLIENT_PASSWORD_CACHE.lock().get(vault_path)
        && Instant::now() < cached.expires_at
    {
        return Ok(cached.password.clone());
    }

    let fetch_lock = {
        let mut locks = FETCH_LOCKS.lock();
        Arc::clone(
            locks
                .entry(vault_path.to_owned())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    };
    let _fetch_guard = fetch_lock.lock().await;

    // Double-check: another task may have fetched while we waited for the lock.
    if let Some(cached) = CLIENT_PASSWORD_CACHE.lock().get(vault_path)
        && Instant::now() < cached.expires_at
    {
        return Ok(cached.password.clone());
    }

    let vault = config()
        .config
        .vault
        .clone()
        .ok_or_else(|| error("[vault] section is missing from pgdog.toml"))?;

    watch_reloads();
    let secret: StaticSecretResponse = fetch_secret(&vault, vault_path).await?;
    let fetched_at = Instant::now();
    let expires_at =
        fetched_at + Duration::from_secs(secret.data.ttl).saturating_sub(STATIC_TTL_MARGIN);

    debug!(
        vault_path,
        ttl_secs = secret.data.ttl,
        "fetched Vault static role password"
    );

    CLIENT_PASSWORD_CACHE.lock().insert(
        vault_path.to_owned(),
        CachedStaticPassword {
            password: secret.data.password.clone(),
            fetched_at,
            expires_at,
        },
    );

    Ok(secret.data.password)
}

/// Evict cached static role passwords referenced by `passwords` after a failed
/// client auth, so the next attempt refetches from Vault. Handles the password
/// being rotated before its TTL ran out (e.g. a manual `rotate-role`).
///
/// Entries younger than [`MIN_REFETCH_INTERVAL`] are kept, so repeated bad
/// passwords can't turn into a flood of Vault requests.
pub(crate) fn evict_static_client_passwords(passwords: &[PasswordKind]) {
    let mut cache = CLIENT_PASSWORD_CACHE.lock();
    for p in passwords {
        if let PasswordKind::VaultStaticRole(path) = p
            && cache
                .get(path)
                .is_some_and(|cached| cached.fetched_at.elapsed() >= MIN_REFETCH_INTERVAL)
        {
            cache.remove(path);
            debug!(
                vault_path = path,
                "evicted cached Vault static role password"
            );
        }
    }
}

/// Drop cached static role passwords, client and backend, whenever the
/// databases are reloaded (`RELOAD`, `RECONNECT`, SIGHUP, ...).
///
/// Vault only rotates static role passwords on schedule, so this is how
/// operators make pgdog pick up an out-of-schedule rotation (e.g.
/// `vault write -f database/rotate-role/<role>`) without waiting for the next
/// scheduled one. Started by the first static role fetch.
pub(crate) fn watch_reloads() {
    static STARTED: Once = Once::new();
    STARTED.call_once(|| {
        let mut reloads = reload_signal::watch();
        tokio::spawn(async move {
            loop {
                reloads.changed().await;
                CLIENT_PASSWORD_CACHE.lock().clear();
                evict_static_backend_credentials(&databases());
                debug!("evicted cached Vault static role passwords after reload");
            }
        });
    });
}

/// Resolve any [`PasswordKind::VaultStaticRole`] entries in `passwords` to
/// [`PasswordKind::Plain`] by fetching them from Vault.  Non-Vault entries are
/// passed through unchanged.  Failed Vault fetches are logged and skipped so
/// that any remaining plain/hashed passwords can still authenticate the client.
pub(crate) async fn resolve_passwords(passwords: &[PasswordKind]) -> Vec<PasswordKind> {
    let mut buf = Vec::with_capacity(passwords.len());
    for p in passwords {
        match p {
            PasswordKind::VaultStaticRole(path) => match static_client_password(path).await {
                Ok(pw) => buf.push(PasswordKind::Plain(pw)),
                Err(err) => {
                    warn!(vault_path = path, %err, "failed to resolve Vault password, skipping")
                }
            },
            other => buf.push(other.clone()),
        }
    }
    buf
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

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
                fetched_at: Instant::now(),
                expires_at: Instant::now() + Duration::from_secs(3600),
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
                fetched_at: Instant::now() - Duration::from_secs(3600),
                expires_at: Instant::now() - Duration::from_secs(1),
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
                "data": { "username": "pgdog-static", "password": "fresh-pw", "ttl": 3600 }
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

    #[tokio::test]
    async fn test_static_client_password_concurrent_fetches_once() {
        setup();
        let server = MockServer::start().await;
        let vault_path = "database/static-creds/concurrent-role";

        Mock::given(method("POST"))
            .and(path("/v1/auth/approle/login"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "auth": { "client_token": "s.tok4", "lease_duration": 3600 }
            })))
            .mount(&server)
            .await;

        // Fails on drop if Vault is hit more than once.
        Mock::given(method("GET"))
            .and(path(format!("/v1/{vault_path}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({
                        "data": { "username": "pgdog-static", "password": "shared-pw", "ttl": 3600 }
                    }))
                    .set_delay(Duration::from_millis(100)),
            )
            .expect(1)
            .mount(&server)
            .await;

        let _guard = crate::test_utils::set_env_var("VAULT_SECRET_ID", "my-secret");
        *VAULT_TOKEN.lock() = None;
        CLIENT_PASSWORD_CACHE.lock().remove(vault_path);
        set_vault_config(approle_vault(&server.uri()));

        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..10 {
            tasks.spawn(static_client_password(vault_path));
        }
        while let Some(pw) = tasks.join_next().await {
            assert_eq!(pw.unwrap().unwrap(), "shared-pw");
        }
    }

    #[tokio::test]
    async fn test_static_client_password_ttl_below_margin_not_cached() {
        setup();
        let server = MockServer::start().await;
        let vault_path = "database/static-creds/rotating-role";

        Mock::given(method("POST"))
            .and(path("/v1/auth/approle/login"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "auth": { "client_token": "s.tok5", "lease_duration": 3600 }
            })))
            .mount(&server)
            .await;

        // Rotation is imminent: every call must go back to Vault.
        Mock::given(method("GET"))
            .and(path(format!("/v1/{vault_path}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": { "username": "pgdog-static", "password": "about-to-rotate", "ttl": 0 }
            })))
            .expect(2)
            .mount(&server)
            .await;

        let _guard = crate::test_utils::set_env_var("VAULT_SECRET_ID", "my-secret");
        *VAULT_TOKEN.lock() = None;
        CLIENT_PASSWORD_CACHE.lock().remove(vault_path);
        set_vault_config(approle_vault(&server.uri()));

        for _ in 0..2 {
            let pw = static_client_password(vault_path).await.unwrap();
            assert_eq!(pw, "about-to-rotate");
        }
    }

    // ── evict_static_client_passwords() ─────────────────────────────────────────

    #[test]
    fn test_evict_static_client_passwords() {
        let old = "database/static-creds/evict-old";
        let fresh = "database/static-creds/evict-fresh";
        let untouched = "database/static-creds/evict-untouched";
        let expires_at = Instant::now() + Duration::from_secs(3600);

        {
            let mut cache = CLIENT_PASSWORD_CACHE.lock();
            for (vault_path, age) in [
                (old, MIN_REFETCH_INTERVAL),
                (fresh, Duration::ZERO),
                (untouched, MIN_REFETCH_INTERVAL),
            ] {
                cache.insert(
                    vault_path.into(),
                    CachedStaticPassword {
                        password: "pw".into(),
                        fetched_at: Instant::now() - age,
                        expires_at,
                    },
                );
            }
        }

        evict_static_client_passwords(&[
            PasswordKind::VaultStaticRole(old.into()),
            PasswordKind::VaultStaticRole(fresh.into()),
            PasswordKind::Plain(untouched.into()),
        ]);

        let cache = CLIENT_PASSWORD_CACHE.lock();
        assert!(!cache.contains_key(old), "old entry should be evicted");
        assert!(
            cache.contains_key(fresh),
            "entry younger than MIN_REFETCH_INTERVAL should be kept"
        );
        assert!(
            cache.contains_key(untouched),
            "paths not referenced by a Vault password should be kept"
        );
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
                "data": { "username": "pgdog-static", "password": "rotated-secret", "ttl": 3600 }
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
            expires_at: Instant::now() + Duration::from_secs(3600),
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
        assert!(token.expires_at > Instant::now());
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
            expires_at: Instant::now() + Duration::from_secs(3600),
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

    // ── resolve_passwords() ────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_resolve_passwords_no_vault_roles_passthrough() {
        let passwords = vec![
            PasswordKind::Plain("secret".into()),
            PasswordKind::Hashed("$2b$12$hash".into()),
        ];
        let result = resolve_passwords(&passwords).await;
        assert_eq!(result, passwords);
    }

    #[tokio::test]
    async fn test_resolve_passwords_empty() {
        assert!(resolve_passwords(&[]).await.is_empty());
    }

    #[tokio::test]
    async fn test_resolve_passwords_resolves_vault_role() {
        setup();
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1/auth/approle/login"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "auth": { "client_token": "s.resolve1", "lease_duration": 3600 }
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/v1/database/static-creds/my-role"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": { "username": "pgdog", "password": "vault-pw", "ttl": 3600 }
            })))
            .mount(&server)
            .await;

        let _guard = crate::test_utils::set_env_var("VAULT_SECRET_ID", "my-secret");
        *VAULT_TOKEN.lock() = None;
        CLIENT_PASSWORD_CACHE
            .lock()
            .remove("database/static-creds/my-role");
        set_vault_config(approle_vault(&server.uri()));

        let passwords = vec![PasswordKind::VaultStaticRole(
            "database/static-creds/my-role".into(),
        )];
        let result = resolve_passwords(&passwords).await;
        assert_eq!(result, vec![PasswordKind::Plain("vault-pw".into())]);
    }

    #[tokio::test]
    async fn test_resolve_passwords_mixed_resolves_only_vault() {
        setup();
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1/auth/approle/login"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "auth": { "client_token": "s.resolve2", "lease_duration": 3600 }
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/v1/database/static-creds/mixed-role"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": { "username": "pgdog", "password": "mixed-pw", "ttl": 3600 }
            })))
            .mount(&server)
            .await;

        let _guard = crate::test_utils::set_env_var("VAULT_SECRET_ID", "my-secret");
        *VAULT_TOKEN.lock() = None;
        CLIENT_PASSWORD_CACHE
            .lock()
            .remove("database/static-creds/mixed-role");
        set_vault_config(approle_vault(&server.uri()));

        let passwords = vec![
            PasswordKind::Plain("static-pw".into()),
            PasswordKind::VaultStaticRole("database/static-creds/mixed-role".into()),
        ];
        let result = resolve_passwords(&passwords).await;
        assert_eq!(
            result,
            vec![
                PasswordKind::Plain("static-pw".into()),
                PasswordKind::Plain("mixed-pw".into()),
            ]
        );
    }

    #[tokio::test]
    async fn test_resolve_passwords_vault_error_skips_entry() {
        setup();
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1/auth/approle/login"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "auth": { "client_token": "s.resolve3", "lease_duration": 3600 }
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/v1/database/static-creds/bad-role"))
            .respond_with(ResponseTemplate::new(500).set_body_string("vault internal error"))
            .mount(&server)
            .await;

        let _guard = crate::test_utils::set_env_var("VAULT_SECRET_ID", "my-secret");
        *VAULT_TOKEN.lock() = None;
        CLIENT_PASSWORD_CACHE
            .lock()
            .remove("database/static-creds/bad-role");
        set_vault_config(approle_vault(&server.uri()));

        // Vault entry is skipped; the plain fallback survives.
        let passwords = vec![
            PasswordKind::VaultStaticRole("database/static-creds/bad-role".into()),
            PasswordKind::Plain("fallback-pw".into()),
        ];
        let result = resolve_passwords(&passwords).await;
        assert_eq!(result, vec![PasswordKind::Plain("fallback-pw".into())]);
    }

    #[tokio::test]
    async fn test_resolve_passwords_all_vault_errors_yields_empty() {
        crate::config::set(ConfigAndUsers::default()).unwrap();

        // No vault config → static_client_password errors for every entry.
        let passwords = vec![PasswordKind::VaultStaticRole(
            "database/static-creds/no-config-role".into(),
        )];
        let result = resolve_passwords(&passwords).await;
        assert!(
            result.is_empty(),
            "all vault entries failed, expected empty vec"
        );
    }
}
