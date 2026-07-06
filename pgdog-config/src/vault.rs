//! HashiCorp Vault settings.

use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Default Kubernetes auth mount path.
pub const DEFAULT_KUBERNETES_MOUNT: &str = "kubernetes";
/// Default AppRole auth mount path.
pub const DEFAULT_APPROLE_MOUNT: &str = "approle";
/// Default Kubernetes service account JWT path.
pub const DEFAULT_KUBERNETES_JWT_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";
/// Default percentage of a credential lease after which it's refreshed.
pub const DEFAULT_REFRESH_PERCENT: u8 = 80;
/// Default seconds before the Vault client token expires to trigger a re-login.
pub const DEFAULT_CLIENT_TOKEN_TTL_SECS: u64 = 60;

/// How PgDog authenticates to Vault.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum VaultAuthMethod {
    /// Kubernetes auth: log in with the pod's service account JWT.
    Kubernetes,
    /// AppRole auth: log in with a role ID and secret ID.
    Approle,
}

/// HashiCorp Vault settings, used by pools configured with `server_auth = "vault_dynamic"`
/// or `"vault_static"`.
///
/// PgDog logs into Vault using the configured auth method and fetches
/// database credentials from the per-user `server_vault_path`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Vault {
    /// Vault server URL, e.g. `https://vault.internal:8200`.
    pub url: String,
    /// Vault namespace (Vault Enterprise). Sent as the `X-Vault-Namespace` header.
    pub namespace: Option<String>,
    /// Auth method used to log into Vault.
    pub auth_method: VaultAuthMethod,
    /// Mount path of the auth method.
    ///
    /// _Default:_ `kubernetes` for Kubernetes auth, `approle` for AppRole auth.
    pub auth_mount: Option<String>,
    /// Kubernetes auth: name of the Vault role to log in as.
    pub kubernetes_role: Option<String>,
    /// Kubernetes auth: path to the service account JWT.
    ///
    /// _Default:_ `/var/run/secrets/kubernetes.io/serviceaccount/token`
    pub kubernetes_jwt_path: Option<String>,
    /// AppRole auth: role ID.
    pub approle_role_id: Option<String>,
    /// AppRole auth: path to a file containing the secret ID.
    ///
    /// **Note:** If not set, the secret ID is read from the `VAULT_SECRET_ID`
    /// environment variable.
    pub approle_secret_id_file: Option<String>,
    /// Seconds before the Vault client token expires to trigger a re-login.
    ///
    /// _Default:_ `60`
    pub client_token_ttl: Option<u64>,
}

impl Vault {
    /// Mount path of the auth method, applying defaults.
    pub fn auth_mount(&self) -> &str {
        match self.auth_mount.as_deref() {
            Some(mount) => mount,
            None => match self.auth_method {
                VaultAuthMethod::Kubernetes => DEFAULT_KUBERNETES_MOUNT,
                VaultAuthMethod::Approle => DEFAULT_APPROLE_MOUNT,
            },
        }
    }

    /// Path to the service account JWT, applying the default.
    pub fn kubernetes_jwt_path(&self) -> &str {
        self.kubernetes_jwt_path
            .as_deref()
            .unwrap_or(DEFAULT_KUBERNETES_JWT_PATH)
    }

    /// Duration before the Vault client token expires to trigger a re-login.
    pub fn token_expiry_buffer(&self) -> Duration {
        Duration::from_secs(
            self.client_token_ttl
                .unwrap_or(DEFAULT_CLIENT_TOKEN_TTL_SECS),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kubernetes_defaults() {
        let vault: Vault = toml::from_str(
            r#"
url = "https://vault.internal:8200"
auth_method = "kubernetes"
kubernetes_role = "pgdog"
"#,
        )
        .unwrap();

        assert_eq!(vault.auth_mount(), "kubernetes");
        assert_eq!(
            vault.kubernetes_jwt_path(),
            "/var/run/secrets/kubernetes.io/serviceaccount/token"
        );
        assert_eq!(vault.kubernetes_role.as_deref(), Some("pgdog"));
    }

    #[test]
    fn test_approle_defaults() {
        let vault: Vault = toml::from_str(
            r#"
url = "https://vault.internal:8200"
auth_method = "approle"
approle_role_id = "abc-123"
"#,
        )
        .unwrap();

        assert_eq!(vault.auth_mount(), "approle");
        assert_eq!(vault.approle_role_id.as_deref(), Some("abc-123"));
        assert!(vault.approle_secret_id_file.is_none());
    }

    #[test]
    fn test_mount_override() {
        let vault: Vault = toml::from_str(
            r#"
url = "https://vault.internal:8200"
auth_method = "kubernetes"
auth_mount = "k8s-prod"
kubernetes_role = "pgdog"
"#,
        )
        .unwrap();

        assert_eq!(vault.auth_mount(), "k8s-prod");
    }
}
