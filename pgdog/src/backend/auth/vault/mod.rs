//! Vault dynamic credential lifecycle management for pgdog backend pools.
//!
//! ## Startup + background renewal
//!
//! 1. Authenticates to Vault, fetches credentials for all pools, writes them to the
//!    in-memory [`STORE`] cache so pools can connect with the correct dynamic credentials.
//! 2. Sleeps until `pre_rotation_pct`% of the shortest lease TTL has elapsed,
//!    then repeats and calls reload_from_existing to re-create connection with the new set of credentials.
//!
//! If any step fails the task retries with exponential backoff (capped at 60 s).

pub mod api;
pub mod error;

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;

use once_cell::sync::Lazy;
use tokio::fs::read_to_string;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::backend::databases::reload_from_existing;

use api::{build_client, AppRoleLogin, FetchCredential, KubernetesLogin};
pub use api::{VaultCredential, VaultToken};
pub use error::Error;

use pgdog_config::{User, VaultAuthMethod, VaultConfig};

// ── credential cache ───────────────────────────────────────────────────────────
//
// Credentials are stored here rather than in the parsed config so they survive
// config reloads from disk. `Address::new()` reads from here for Vault users.

#[derive(Clone)]
pub struct CachedCredential {
    pub username: String,
    pub password: String,
}

static STORE: Lazy<RwLock<HashMap<String, CachedCredential>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

pub fn cache_set(pool_name: &str, username: &str, password: &str) {
    let mut w = STORE.write().unwrap_or_else(|e| e.into_inner());
    w.insert(
        pool_name.to_string(),
        CachedCredential {
            username: username.to_string(),
            password: password.to_string(),
        },
    );
}

pub fn cache_get(pool_name: &str) -> Option<CachedCredential> {
    STORE
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(pool_name)
        .cloned()
}

// ── Vault HTTP client ─────────────────────────────────────────────────────────

/// Holds the `reqwest` client and config for a single Vault connection.
/// Built once so TLS handshakes and connection pool state are reused across all API calls.
struct VaultClient {
    client: reqwest::Client,
    cfg: VaultConfig,
}

impl VaultClient {
    fn new(cfg: VaultConfig) -> Result<Self, Error> {
        let client = build_client(&cfg)?;
        Ok(Self { client, cfg })
    }

    /// Authenticate to Vault and return a short-lived token.
    async fn login(&self) -> Result<VaultToken, Error> {
        match self.cfg.auth_method {
            VaultAuthMethod::AppRole => {
                let role_id = self.cfg.role_id.as_deref().ok_or_else(|| {
                    Error::SecretId("[vault] role_id is required for AppRole auth".into())
                })?;
                let secret_id = self
                    .cfg
                    .secret_id()
                    .map_err(|e| Error::SecretId(e.to_string()))?;
                AppRoleLogin {
                    client: &self.client,
                    addr: &self.cfg.address,
                    role_id,
                    secret_id: &secret_id,
                }
                .call()
                .await
            }
            VaultAuthMethod::Kubernetes => {
                let role = self.cfg.kubernetes_role.as_deref().ok_or_else(|| {
                    Error::SecretId(
                        "[vault] kubernetes_role is required for Kubernetes auth".into(),
                    )
                })?;
                let jwt = read_to_string(self.cfg.jwt_path())
                    .await
                    .map(|s| s.trim().to_string())
                    .map_err(|e| {
                        Error::SecretId(format!(
                            "[vault] failed to read JWT from {}: {e}",
                            self.cfg.jwt_path().display()
                        ))
                    })?;
                KubernetesLogin {
                    client: &self.client,
                    addr: &self.cfg.address,
                    mount_path: &self.cfg.kubernetes_mount_path,
                    role,
                    jwt: &jwt,
                }
                .call()
                .await
            }
        }
    }

    /// Authenticate once and fetch credentials for every pool.
    /// Returns all credentials and the minimum lease duration seen.
    async fn fetch_credentials(
        &self,
        pools: &[(String, String)],
    ) -> Result<(Vec<(String, VaultCredential)>, u64), Error> {
        let token = self.login().await?;
        let mut credentials = Vec::with_capacity(pools.len());
        let mut min_lease = u64::MAX;

        for (pool_name, vault_path) in pools {
            let cred = match (FetchCredential {
                client: &self.client,
                addr: &self.cfg.address,
                token: &token.client_token,
                path: vault_path,
            })
            .call()
            .await
            {
                Ok(c) => c,
                Err(e) => {
                    error!(pool = %pool_name, "[vault] failed to fetch credential: {e}");
                    continue;
                }
            };
            if cred.lease_duration == 0 {
                warn!(
                    pool = %pool_name,
                    "[vault] lease_duration is 0 — credentials may not be renewable; check Vault backend config"
                );
            }
            min_lease = min_lease.min(cred.lease_duration);
            credentials.push((pool_name.clone(), cred));
        }

        Ok((
            credentials,
            if min_lease == u64::MAX { 0 } else { min_lease },
        ))
    }
}

// ── public interface ──────────────────────────────────────────────────────────

/// Owns the single background renewal task.
pub struct VaultManager {
    handle: JoinHandle<()>,
}

impl VaultManager {
    /// Fetch initial Vault credentials (awaitable), then spawn the background
    /// renewal task. Returns `None` when no users have `vault_path` set.
    pub async fn start(vault_config: &VaultConfig, users: &[User]) -> Option<Self> {
        let pools = vault_pools(users);
        if pools.is_empty() {
            return None;
        }

        let client = match VaultClient::new(vault_config.clone()) {
            Ok(c) => c,
            Err(err) => {
                error!("[vault] failed to build HTTP client: {err}");
                return None;
            }
        };

        let initial_sleep = match client.fetch_credentials(&pools).await {
            Ok((creds, min_lease)) => {
                for (pool_name, cred) in &creds {
                    cache_set(pool_name, &cred.username, &cred.password);
                }
                info!(pools = pools.len(), "[vault] initial credentials fetched");
                rotation_interval(min_lease, client.cfg.pre_rotation_pct)
            }
            Err(err) => {
                error!("[vault] initial credential fetch failed: {err}");
                Duration::from_secs(1)
            }
        };

        let handle = tokio::spawn(renewal_task(client, pools, initial_sleep));

        Some(Self { handle })
    }
}

impl Drop for VaultManager {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

// ── background task ───────────────────────────────────────────────────────────

/// Sleeps for `initial_sleep` (the rotation interval from the initial fetch),
/// then loops: fetch → sleep(rotation_interval) → repeat.
async fn renewal_task(client: VaultClient, pools: Vec<(String, String)>, initial_sleep: Duration) {
    tokio::time::sleep(initial_sleep).await;
    let mut backoff = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(60);

    loop {
        match client.fetch_credentials(&pools).await {
            Ok((creds, min_lease)) => {
                backoff = Duration::from_secs(1);
                for (pool_name, cred) in &creds {
                    cache_set(pool_name, &cred.username, &cred.password);
                }
                if let Err(e) = reload_from_existing() {
                    error!("[vault] pool reload failed after credential rotation: {e}");
                }
                let next = rotation_interval(min_lease, client.cfg.pre_rotation_pct);
                info!(
                    pools = pools.len(),
                    min_lease_secs = min_lease,
                    next_refresh_secs = next.as_secs(),
                    "[vault] credentials rotated"
                );
                tokio::time::sleep(next).await;
            }
            Err(err) => {
                error!(
                    backoff_secs = backoff.as_secs(),
                    "[vault] rotation failed: {err}"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Composite cache key so credentials are scoped per (user, database) pair.
fn pool_key(user_name: &str, database: &str) -> String {
    format!("{user_name}/{database}")
}

fn vault_pools(users: &[User]) -> Vec<(String, String)> {
    users
        .iter()
        .filter_map(|u| {
            u.vault_path
                .as_ref()
                .map(|path| (pool_key(&u.name, &u.database), path.clone()))
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
    use crate::config::load_test;
    use once_cell::sync::Lazy;
    use pgdog_config::VaultConfig;

    // reqwest with rustls-tls needs a process-level CryptoProvider.
    static RING: Lazy<()> = Lazy::new(|| {
        let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
    });

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

    // ── fetch_credentials (mocked) ───────────────────────────────────────────

    #[tokio::test]
    async fn test_fetch_credentials_returns_credentials_and_lease() {
        load_test();
        test_support::set_login(Some(Ok(VaultToken {
            client_token: "tok".into(),
            lease_duration: 3600,
            renewable: true,
        })));
        test_support::set_credential(Some(Ok(test_cred("v-approle-dml-mock"))));

        let pools = vec![("pgdog".into(), "database/creds/dml-role".into())];
        let (creds, lease) = vault_client()
            .fetch_credentials(&pools)
            .await
            .expect("fetch_credentials should succeed with mocked API");
        assert_eq!(lease, 3600);
        assert_eq!(creds.len(), 1);
        assert_eq!(creds[0].0, "pgdog");
        assert_eq!(creds[0].1.username, "v-approle-dml-mock");
    }

    #[tokio::test]
    async fn test_fetch_credentials_propagates_login_error() {
        test_support::set_login(Some(Err(Error::VaultStatus {
            status: 403,
            body: "".into(),
        })));
        let pools = vec![("pool".into(), "database/creds/role".into())];
        let err = vault_client().fetch_credentials(&pools).await.unwrap_err();
        assert!(matches!(err, Error::VaultStatus { status: 403, .. }));
    }

    #[tokio::test]
    async fn test_fetch_credentials_skips_failed_credential() {
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
        let (creds, lease) = vault_client()
            .fetch_credentials(&pools)
            .await
            .expect("should succeed with failed pool skipped");
        assert!(creds.is_empty(), "failed pool should be skipped");
        assert_eq!(lease, 0);
    }

    // ── VaultManager ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_manager_returns_none_when_no_vault_users() {
        let users: Vec<pgdog_config::User> = vec![pgdog_config::User {
            name: "plain_user".into(),
            vault_path: None,
            ..Default::default()
        }];
        assert!(VaultManager::start(&vault_cfg(), &users).await.is_none());
    }

    #[tokio::test]
    async fn test_manager_spawns_tasks_for_vault_users() {
        load_test();
        test_support::set_login(Some(Ok(VaultToken {
            client_token: "tok".into(),
            lease_duration: 3600,
            renewable: true,
        })));
        test_support::set_credential(Some(Ok(test_cred("v-approle-manager-AbCdEf"))));

        let users = vec![pgdog_config::User {
            name: "pgdog".into(),
            vault_path: Some("database/creds/dml-role".into()),
            ..Default::default()
        }];
        assert!(VaultManager::start(&vault_cfg(), &users).await.is_some());
    }

    // ── cache ─────────────────────────────────────────────────────────────────

    #[test]
    fn test_cache_set_and_get() {
        cache_set("cache_test_pool", "v-approle-abc", "pw123");
        let cred = cache_get("cache_test_pool").expect("should find cached credential");
        assert_eq!(cred.username, "v-approle-abc");
        assert_eq!(cred.password, "pw123");
    }

    #[test]
    fn test_cache_get_missing_returns_none() {
        assert!(cache_get("pool_that_was_never_cached_xyz").is_none());
    }

    #[test]
    fn test_cache_overwrite() {
        cache_set("overwrite_pool", "old-user", "old-pw");
        cache_set("overwrite_pool", "new-user", "new-pw");
        let cred = cache_get("overwrite_pool").unwrap();
        assert_eq!(cred.username, "new-user");
        assert_eq!(cred.password, "new-pw");
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
            kubernetes_jwt_path: f.path().into(),
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
            kubernetes_jwt_path: std::path::PathBuf::from("/nonexistent/token"),
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
            kubernetes_jwt_path: f.path().into(),
            ..k8s_cfg()
        })
        .unwrap();
        let pools = vec![("pgdog".into(), "database/creds/dml-role".into())];
        let (creds, lease) = client.fetch_credentials(&pools).await.unwrap();
        assert_eq!(lease, 7200);
        assert_eq!(creds.len(), 1);
    }
}
