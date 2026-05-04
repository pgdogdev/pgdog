//! Vault dynamic credential lifecycle management for pgdog backend pools.
//!
//! ## Startup
//!
//! Call [`init`] **before** `databases::init()`. It authenticates to Vault, fetches
//! credentials for every user with `vault_path`, and writes them into the live
//! config via [`update_config`] (no pool reload). This ensures pools are created
//! with the correct credentials on the very first connection.
//!
//! ## Background renewal
//!
//! After `databases::init()`, call [`VaultManager::start`] with the `initial_delay`
//! returned by [`init`]. The single background task:
//!
//! 1. Sleeps for `initial_delay` (skips redundant re-fetch of credentials just obtained at startup).
//! 2. Authenticates to Vault, fetches fresh credentials for all pools, and calls
//!    [`apply_credential`] which writes the config and triggers `reload_from_existing()`.
//! 3. Sleeps until `pre_rotation_pct`% of the shortest lease TTL has elapsed, then repeats.
//!
//! If any step fails the task retries with exponential backoff (capped at 60 s).

pub mod api;
pub mod error;

use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::backend::databases::{lock, reload_from_existing};
use crate::config::{config, set};

pub use api::{VaultCredential, VaultToken};
pub use error::Error;

use pgdog_config::{User, VaultAuthMethod, VaultConfig};

// ── Vault HTTP client ─────────────────────────────────────────────────────────

/// Holds the `reqwest` client and config for a single Vault connection.
/// Built once — at startup via [`init`] and once inside the renewal task —
/// so TLS handshakes and connection pool state are reused across all API calls.
struct VaultClient {
    client: reqwest::Client,
    cfg: VaultConfig,
}

impl VaultClient {
    fn new(cfg: VaultConfig) -> Result<Self, Error> {
        let client = api::build_client(&cfg)?;
        Ok(Self { client, cfg })
    }

    /// Authenticate to Vault and return a short-lived token.
    async fn login(&self) -> Result<VaultToken, Error> {
        match self.cfg.auth_method {
            VaultAuthMethod::AppRole => {
                let role_id = self.cfg.role_id.as_deref().ok_or_else(|| {
                    Error::SecretId("vault: role_id is required for AppRole auth".into())
                })?;
                let secret_id = self
                    .cfg
                    .secret_id()
                    .map_err(|e| Error::SecretId(e.to_string()))?;
                api::approle_login(&self.client, &self.cfg.address, role_id, &secret_id).await
            }
            VaultAuthMethod::Kubernetes => {
                let role = self.cfg.kubernetes_role.as_deref().ok_or_else(|| {
                    Error::SecretId(
                        "vault: kubernetes_role is required for Kubernetes auth".into(),
                    )
                })?;
                let jwt = tokio::fs::read_to_string(self.cfg.jwt_path())
                    .await
                    .map(|s| s.trim().to_string())
                    .map_err(|e| {
                        Error::SecretId(format!(
                            "vault: failed to read JWT from {}: {e}",
                            self.cfg.jwt_path()
                        ))
                    })?;
                api::kubernetes_login(
                    &self.client,
                    &self.cfg.address,
                    &self.cfg.kubernetes_mount_path,
                    role,
                    &jwt,
                )
                .await
            }
        }
    }

    /// Authenticate once, fetch credentials for every pool, call `on_credential` for
    /// each. Returns the minimum lease duration seen across all pools.
    async fn fetch_credentials(
        &self,
        pools: &[(String, String)],
        mut on_credential: impl FnMut(&str, &VaultCredential) -> Result<(), Error>,
    ) -> Result<u64, Error> {
        let token = self.login().await?;
        let mut min_lease = u64::MAX;

        for (pool_name, vault_path) in pools {
            let cred = api::fetch_credential(
                &self.client,
                &self.cfg.address,
                &token.client_token,
                vault_path,
            )
            .await?;
            if cred.lease_duration == 0 {
                tracing::warn!(
                    pool = %pool_name,
                    "vault: lease_duration is 0 — credentials may not be renewable; check Vault backend config"
                );
            }
            on_credential(pool_name, &cred)?;
            min_lease = min_lease.min(cred.lease_duration);
        }

        Ok(if min_lease == u64::MAX { 0 } else { min_lease })
    }
}

// ── startup init ──────────────────────────────────────────────────────────────

/// Fetch Vault credentials for all users with `vault_path` and write them into
/// the live config without triggering pool reload. Call this **before**
/// `databases::init()` so pools start with the correct credentials.
///
/// Returns the rotation interval to pass to [`VaultManager::start`] as
/// `initial_delay`. Returns `Duration::ZERO` on error so the background task
/// retries immediately.
pub async fn init(vault_config: &VaultConfig, users: &[User]) -> Duration {
    let pools = vault_pools(users);
    if pools.is_empty() {
        return Duration::ZERO;
    }

    let client = match VaultClient::new(vault_config.clone()) {
        Ok(c) => c,
        Err(err) => {
            error!("vault: failed to build HTTP client: {err}");
            return Duration::ZERO;
        }
    };

    match client.fetch_credentials(&pools, update_config).await {
        Ok(min_lease) => {
            info!(pools = pools.len(), "vault: initial credentials fetched");
            rotation_interval(min_lease, vault_config.pre_rotation_pct)
        }
        Err(err) => {
            error!("vault: initial credential fetch failed: {err}");
            Duration::ZERO
        }
    }
}

// ── public interface ──────────────────────────────────────────────────────────

/// Owns the single background renewal task.
pub struct VaultManager {
    handle: JoinHandle<()>,
}

impl VaultManager {
    /// Spawn a single renewal task covering all pools with `vault_path` configured.
    ///
    /// `initial_delay` is the value returned by [`init`]. The task sleeps for that
    /// duration before its first renewal so it does not redundantly re-fetch
    /// credentials that were just obtained at startup. Pass `Duration::ZERO` to
    /// start immediately (e.g. when [`init`] failed).
    ///
    /// Returns `None` when no users have `vault_path` set.
    pub fn start(
        vault_config: &VaultConfig,
        users: &[User],
        initial_delay: Duration,
    ) -> Option<Self> {
        let pools = vault_pools(users);
        if pools.is_empty() {
            return None;
        }

        let cfg = vault_config.clone();
        let handle = tokio::spawn(async move {
            let client = match VaultClient::new(cfg) {
                Ok(c) => c,
                Err(err) => {
                    error!("vault: failed to build HTTP client: {err}");
                    return;
                }
            };
            renewal_task(client, pools, initial_delay).await;
        });

        Some(Self { handle })
    }
}

impl Drop for VaultManager {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

// ── background task ───────────────────────────────────────────────────────────

/// A single task that sleeps for `initial_delay`, then repeatedly authenticates
/// to Vault, rotates credentials for all configured pools, and sleeps until the
/// next rotation is due.
async fn renewal_task(
    client: VaultClient,
    pools: Vec<(String, String)>,
    initial_delay: Duration,
) {
    // Skip initial sleep when init() failed (initial_delay == ZERO) to retry promptly.
    if !initial_delay.is_zero() {
        tokio::time::sleep(initial_delay).await;
    }

    let mut backoff = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(60);

    loop {
        match client.fetch_credentials(&pools, apply_credential).await {
            Ok(min_lease) => {
                backoff = Duration::from_secs(1);
                let next = rotation_interval(min_lease, client.cfg.pre_rotation_pct);
                info!(
                    pools = pools.len(),
                    min_lease_secs = min_lease,
                    next_refresh_secs = next.as_secs(),
                    "vault: credentials rotated"
                );
                tokio::time::sleep(next).await;
            }
            Err(err) => {
                error!(
                    backoff_secs = backoff.as_secs(),
                    "vault: rotation failed: {err}"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

// ── config update ─────────────────────────────────────────────────────────────

/// Update `server_user` / `server_password` for `pool_name` in the live config,
/// then trigger `reload_from_existing()` so pgdog reconnects with the new credentials.
fn apply_credential(pool_name: &str, cred: &VaultCredential) -> Result<(), Error> {
    // update_config acquires and releases databases::lock() internally.
    // reload_from_existing() must be called AFTER the lock is released — it
    // re-acquires the same lock, so holding it across the call would deadlock.
    update_config(pool_name, cred)?;
    reload_from_existing().map_err(|e| Error::ConfigUpdate(e.to_string()))
}

/// Write `server_user` / `server_password` for `pool_name` into the live config.
/// Does **not** trigger pool reload; used by [`init`] before pools exist.
fn update_config(pool_name: &str, cred: &VaultCredential) -> Result<(), Error> {
    let _lock = lock();
    let mut cfg = (*config()).clone();

    let found = cfg.users.users.iter_mut().any(|u| {
        if u.name == pool_name {
            u.server_user = Some(cred.username.clone());
            u.server_password = Some(cred.password.clone());
            true
        } else {
            false
        }
    });

    if !found {
        return Err(Error::PoolNotFound(pool_name.to_string()));
    }

    set(cfg)
        .map(|_| ())
        .map_err(|e| Error::ConfigUpdate(e.to_string()))
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn vault_pools(users: &[User]) -> Vec<(String, String)> {
    users
        .iter()
        .filter_map(|u| {
            u.vault_path
                .as_ref()
                .map(|path| (u.name.clone(), path.clone()))
        })
        .collect()
}

/// Minimum rotation interval regardless of TTL, to prevent a busy-loop when
/// Vault returns `lease_duration = 0` (non-renewable credentials, misconfigured backend).
const MIN_ROTATION_INTERVAL: Duration = Duration::from_secs(10);

/// How long to wait before fetching the next credential generation.
/// Clamps `pre_rotation_pct` to [1, 99] and enforces a 10-second minimum
/// so a zero-TTL response never causes a tight loop hammering Vault.
pub fn rotation_interval(lease_duration_secs: u64, pre_rotation_pct: u8) -> Duration {
    let pct = pre_rotation_pct.clamp(1, 99) as f64 / 100.0;
    let computed = Duration::from_secs_f64(lease_duration_secs as f64 * pct);
    computed.max(MIN_ROTATION_INTERVAL)
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::api::test_support;
    use super::*;
    use crate::config::{load_test, set};
    use once_cell::sync::Lazy;
    use pgdog_config::VaultConfig;

    // reqwest with rustls-tls needs a process-level CryptoProvider.
    static RING: Lazy<()> = Lazy::new(|| {
        let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
    });

    fn set_config_only() {
        let mut cfg = pgdog_config::ConfigAndUsers::default();
        cfg.config.databases = vec![pgdog_config::Database {
            name: "pgdog".into(),
            host: "127.0.0.1".into(),
            port: 5432,
            ..Default::default()
        }];
        cfg.users.users = vec![pgdog_config::User {
            name: "pgdog".into(),
            database: "pgdog".into(),
            password: Some("pgdog".into()),
            ..Default::default()
        }];
        set(cfg).unwrap();
    }

    fn vault_cfg() -> VaultConfig {
        let _ = *RING;
        VaultConfig {
            address: "http://127.0.0.1:8200".into(),
            auth_method: pgdog_config::VaultAuthMethod::AppRole,
            pre_rotation_pct: 75,
            role_id: Some("test-role-id".into()),
            secret_id: Some("test-secret-id".into()),
            secret_id_file: None,
            kubernetes_role: None,
            kubernetes_jwt_path: VaultConfig::default_kubernetes_jwt_path(),
            kubernetes_mount_path: VaultConfig::default_kubernetes_mount_path(),
            tls_verify: pgdog_config::VaultTlsVerify::VerifyFull,
            tls_server_ca_certificate: None,
        }
    }

    fn k8s_cfg() -> VaultConfig {
        VaultConfig {
            address: "http://127.0.0.1:8200".into(),
            auth_method: pgdog_config::VaultAuthMethod::Kubernetes,
            pre_rotation_pct: 75,
            role_id: None,
            secret_id: None,
            secret_id_file: None,
            kubernetes_role: Some("pgdog".into()),
            kubernetes_jwt_path: VaultConfig::default_kubernetes_jwt_path(),
            kubernetes_mount_path: VaultConfig::default_kubernetes_mount_path(),
            tls_verify: pgdog_config::VaultTlsVerify::VerifyFull,
            tls_server_ca_certificate: None,
        }
    }

    fn vault_client() -> VaultClient {
        VaultClient::new(vault_cfg()).expect("failed to build VaultClient in test")
    }

    fn k8s_client() -> VaultClient {
        VaultClient::new(k8s_cfg()).expect("failed to build VaultClient in test")
    }

    fn test_cred(username: &str) -> VaultCredential {
        test_cred_with_lease(username, 3600)
    }

    fn test_cred_with_lease(username: &str, lease_duration: u64) -> VaultCredential {
        VaultCredential {
            username: username.into(),
            password: "s3cr3t".into(),
            lease_duration,
        }
    }

    // ── rotation_interval ────────────────────────────────────────────────────

    #[test]
    fn test_rotation_interval_75pct_of_one_hour() {
        assert_eq!(rotation_interval(3600, 75), Duration::from_secs(2700));
    }

    #[test]
    fn test_rotation_interval_50pct_of_one_day() {
        assert_eq!(rotation_interval(86400, 50), Duration::from_secs(43200));
    }

    #[test]
    fn test_rotation_interval_clamps_100_to_99() {
        assert_eq!(rotation_interval(100, 100), rotation_interval(100, 99));
        assert_eq!(rotation_interval(100, 99), Duration::from_secs(99));
    }

    #[test]
    fn test_rotation_interval_clamps_0_to_1() {
        assert_eq!(rotation_interval(1000, 0), Duration::from_secs(10));
    }

    #[test]
    fn test_rotation_interval_zero_lease_uses_minimum() {
        assert_eq!(rotation_interval(0, 75), MIN_ROTATION_INTERVAL);
        assert_eq!(rotation_interval(0, 0), MIN_ROTATION_INTERVAL);
    }

    #[test]
    fn test_rotation_interval_short_lease_uses_minimum() {
        assert_eq!(rotation_interval(1, 75), MIN_ROTATION_INTERVAL);
    }

    // ── apply_credential ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_apply_credential_errors_on_unknown_pool() {
        set_config_only();
        let cred = test_cred("v-approle-dml-AbCdEf");
        let err = apply_credential("nonexistent_vault_pool", &cred).unwrap_err();
        assert!(matches!(err, Error::PoolNotFound(_)));
    }

    #[tokio::test]
    async fn test_apply_credential_updates_known_pool() {
        load_test();
        let cred = test_cred("v-approle-pgdog-AbCdEf");
        assert!(apply_credential("pgdog", &cred).is_ok());
    }

    // ── fetch_credentials (mocked) ───────────────────────────────────────────

    #[tokio::test]
    async fn test_fetch_credentials_apply_rotation() {
        load_test();
        test_support::set_login(Some(Ok(VaultToken {
            client_token: "tok".into(),
            lease_duration: 3600,
            renewable: true,
        })));
        test_support::set_credential(Some(Ok(test_cred("v-approle-dml-mock"))));

        let pools = vec![("pgdog".into(), "database/creds/dml-role".into())];
        let lease = vault_client()
            .fetch_credentials(&pools, apply_credential)
            .await
            .expect("fetch_credentials should succeed with mocked API");
        assert_eq!(lease, 3600);
    }

    #[tokio::test]
    async fn test_fetch_credentials_propagates_login_error() {
        test_support::set_login(Some(Err(Error::VaultStatus {
            status: 403,
            body: "".into(),
        })));
        let pools = vec![("pool".into(), "database/creds/role".into())];
        let err = vault_client()
            .fetch_credentials(&pools, apply_credential)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::VaultStatus { status: 403, .. }));
    }

    #[tokio::test]
    async fn test_fetch_credentials_propagates_credential_error() {
        test_support::set_login(Some(Ok(VaultToken {
            client_token: "tok".into(),
            lease_duration: 3600,
            renewable: true,
        })));
        test_support::set_credential(Some(Err(Error::VaultStatus {
            status: 500,
            body: "".into(),
        })));
        let pools = vec![("pool".into(), "database/creds/role".into())];
        let err = vault_client()
            .fetch_credentials(&pools, apply_credential)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::VaultStatus { status: 500, .. }));
    }

    // ── VaultManager ─────────────────────────────────────────────────────────

    #[test]
    fn test_manager_returns_none_when_no_vault_users() {
        let users: Vec<pgdog_config::User> = vec![pgdog_config::User {
            name: "plain_user".into(),
            vault_path: None,
            ..Default::default()
        }];
        assert!(VaultManager::start(&vault_cfg(), &users, Duration::ZERO).is_none());
    }

    #[tokio::test]
    async fn test_manager_spawns_tasks_for_vault_users() {
        let users = vec![
            pgdog_config::User {
                name: "dml_role".into(),
                vault_path: Some("database/creds/dml-role".into()),
                ..Default::default()
            },
            pgdog_config::User {
                name: "ro_role".into(),
                vault_path: Some("database/creds/ro-role".into()),
                ..Default::default()
            },
            pgdog_config::User {
                name: "plain".into(),
                vault_path: None,
                ..Default::default()
            },
        ];
        // Large initial_delay so the renewal_task never fires during the test.
        assert!(
            VaultManager::start(&vault_cfg(), &users, Duration::from_secs(3600)).is_some()
        );
    }

    // ── init ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_init_populates_config_and_returns_rotation_interval() {
        set_config_only();
        test_support::set_login(Some(Ok(VaultToken {
            client_token: "tok".into(),
            lease_duration: 3600,
            renewable: true,
        })));
        test_support::set_credential(Some(Ok(test_cred("v-approle-init-AbCdEf"))));

        let users = vec![pgdog_config::User {
            name: "pgdog".into(),
            vault_path: Some("database/creds/dml-role".into()),
            ..Default::default()
        }];

        let delay = init(&vault_cfg(), &users).await;
        assert_eq!(delay, Duration::from_secs(2700)); // 75% of 3600s

        let cfg = config();
        let user = cfg.users.users.iter().find(|u| u.name == "pgdog").unwrap();
        assert_eq!(user.server_user.as_deref(), Some("v-approle-init-AbCdEf"));
        assert_eq!(user.server_password.as_deref(), Some("s3cr3t"));
    }

    #[tokio::test]
    async fn test_init_returns_zero_on_login_failure() {
        test_support::set_login(Some(Err(Error::VaultStatus {
            status: 403,
            body: "permission denied".into(),
        })));
        let users = vec![pgdog_config::User {
            name: "dml_role".into(),
            vault_path: Some("database/creds/dml-role".into()),
            ..Default::default()
        }];
        let delay = init(&vault_cfg(), &users).await;
        assert_eq!(delay, Duration::ZERO);
    }

    #[tokio::test]
    async fn test_init_returns_zero_when_no_vault_users() {
        let delay = init(&vault_cfg(), &[]).await;
        assert_eq!(delay, Duration::ZERO);
    }

    // ── VaultClient::login ────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_login_approle() {
        test_support::set_login(Some(Ok(VaultToken {
            client_token: "approle-tok".into(),
            lease_duration: 3600,
            renewable: true,
        })));
        let tok = vault_client().login().await.unwrap();
        assert_eq!(tok.client_token, "approle-tok");
    }

    #[tokio::test]
    async fn test_login_approle_missing_role_id_errors() {
        let client = VaultClient::new(VaultConfig {
            role_id: None,
            ..vault_cfg()
        })
        .unwrap();
        let err = client.login().await.unwrap_err();
        assert!(matches!(err, Error::SecretId(_)));
    }

    #[tokio::test]
    async fn test_login_kubernetes() {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "eyJhbGciOiJSUzI1NiJ9.stub").unwrap();

        test_support::set_login(Some(Ok(VaultToken {
            client_token: "k8s-tok".into(),
            lease_duration: 7200,
            renewable: true,
        })));

        let client = VaultClient::new(VaultConfig {
            kubernetes_jwt_path: f.path().to_str().unwrap().into(),
            ..k8s_cfg()
        })
        .unwrap();
        let tok = client.login().await.unwrap();
        assert_eq!(tok.client_token, "k8s-tok");
    }

    #[tokio::test]
    async fn test_login_kubernetes_missing_role_errors() {
        let client = VaultClient::new(VaultConfig {
            kubernetes_role: None,
            ..k8s_cfg()
        })
        .unwrap();
        let err = client.login().await.unwrap_err();
        assert!(matches!(err, Error::SecretId(_)));
    }

    #[tokio::test]
    async fn test_login_kubernetes_missing_jwt_file_errors() {
        let client = VaultClient::new(VaultConfig {
            kubernetes_jwt_path: "/nonexistent/token".into(),
            ..k8s_cfg()
        })
        .unwrap();
        let err = client.login().await.unwrap_err();
        assert!(matches!(err, Error::SecretId(_)));
    }

    #[tokio::test]
    async fn test_fetch_credentials_kubernetes() {
        use std::io::Write;
        load_test();

        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "eyJhbGciOiJSUzI1NiJ9.stub").unwrap();

        test_support::set_login(Some(Ok(VaultToken {
            client_token: "k8s-tok".into(),
            lease_duration: 7200,
            renewable: true,
        })));
        test_support::set_credential(Some(Ok(test_cred_with_lease("v-k8s-dml-AbCdEf", 7200))));

        let client = VaultClient::new(VaultConfig {
            kubernetes_jwt_path: f.path().to_str().unwrap().into(),
            ..k8s_cfg()
        })
        .unwrap();
        let pools = vec![("pgdog".into(), "database/creds/dml-role".into())];
        let lease = client
            .fetch_credentials(&pools, apply_credential)
            .await
            .unwrap();
        assert_eq!(lease, 7200);
    }
}
