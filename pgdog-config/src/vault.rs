use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Vault authentication method used by pgdog to obtain a Vault token.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum VaultAuthMethod {
    /// AppRole authentication — provide `role_id` and `secret_id` / `secret_id_file`.
    #[default]
    AppRole,
    /// Kubernetes authentication — pgdog presents the pod's service account JWT
    /// to Vault, which validates it against the Kubernetes API server.
    Kubernetes,
}

/// Settings for Vault integration.
///
/// When configured, pgdog fetches dynamic PostgreSQL credentials from Vault
/// for pools that have `vault_path` set in `users.toml`, rotating them
/// automatically before expiry.
///
/// # AppRole example
/// ```toml
/// [vault]
/// address       = "https://vault.example.com:8200"
/// auth_method   = "approle"
/// role_id       = "pgdog-role-id"
/// secret_id     = "..."
/// ```
///
/// # Kubernetes example
/// ```toml
/// [vault]
/// address           = "https://vault.example.com:8200"
/// auth_method       = "kubernetes"
/// kubernetes_role   = "pgdog"
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct VaultConfig {
    /// Vault server address, e.g. `https://vault.example.com:8200`.
    pub address: String,

    /// Authentication method used to obtain a Vault token (default: `approle`).
    #[serde(default)]
    pub auth_method: VaultAuthMethod,

    /// Fetch fresh credentials when this percentage of the lease TTL has elapsed.
    /// Must be between 1 and 99. Default: 75.
    #[serde(default = "VaultConfig::default_pre_rotation_pct")]
    pub pre_rotation_pct: u8,

    // ── AppRole fields ────────────────────────────────────────────────────────

    /// AppRole `role_id`. Required when `auth_method = "approle"`.
    pub role_id: Option<String>,

    /// AppRole `secret_id` provided inline. Mutually exclusive with `secret_id_file`.
    pub secret_id: Option<String>,

    /// Path to a file containing the AppRole `secret_id` (e.g. a Kubernetes secret
    /// mounted at `/etc/pgdog/vault-secret-id`). Mutually exclusive with `secret_id`.
    pub secret_id_file: Option<String>,

    // ── Kubernetes fields ─────────────────────────────────────────────────────

    /// Vault role name to authenticate as. Required when `auth_method = "kubernetes"`.
    pub kubernetes_role: Option<String>,

    /// Path to the Kubernetes service account JWT token file.
    /// Default: `/var/run/secrets/kubernetes.io/serviceaccount/token`.
    #[serde(default = "VaultConfig::default_kubernetes_jwt_path")]
    pub kubernetes_jwt_path: String,

    /// Vault mount path for the Kubernetes auth method. Default: `kubernetes`.
    #[serde(default = "VaultConfig::default_kubernetes_mount_path")]
    pub kubernetes_mount_path: String,
}

impl VaultConfig {
    fn default_pre_rotation_pct() -> u8 {
        75
    }

    pub fn default_kubernetes_jwt_path() -> String {
        "/var/run/secrets/kubernetes.io/serviceaccount/token".into()
    }

    pub fn default_kubernetes_mount_path() -> String {
        "kubernetes".into()
    }

    /// Returns the AppRole `secret_id`, reading from `secret_id_file` if necessary.
    /// Trims whitespace so files written with a trailing newline work out of the box.
    pub fn secret_id(&self) -> std::io::Result<String> {
        if let Some(ref s) = self.secret_id {
            return Ok(s.clone());
        }
        if let Some(ref path) = self.secret_id_file {
            return std::fs::read_to_string(path).map(|s| s.trim().to_string());
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "vault: neither secret_id nor secret_id_file is configured",
        ))
    }

    /// Returns the path to the Kubernetes service account JWT.
    /// Callers should read the file asynchronously via `tokio::fs::read_to_string`.
    pub fn jwt_path(&self) -> &str {
        &self.kubernetes_jwt_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approle_base() -> VaultConfig {
        VaultConfig {
            address: "http://127.0.0.1:8200".into(),
            auth_method: VaultAuthMethod::AppRole,
            pre_rotation_pct: 75,
            role_id: Some("test-role".into()),
            secret_id: Some("inline-secret".into()),
            secret_id_file: None,
            kubernetes_role: None,
            kubernetes_jwt_path: VaultConfig::default_kubernetes_jwt_path(),
            kubernetes_mount_path: VaultConfig::default_kubernetes_mount_path(),
        }
    }

    fn kubernetes_base() -> VaultConfig {
        VaultConfig {
            address: "http://127.0.0.1:8200".into(),
            auth_method: VaultAuthMethod::Kubernetes,
            pre_rotation_pct: 75,
            role_id: None,
            secret_id: None,
            secret_id_file: None,
            kubernetes_role: Some("pgdog".into()),
            kubernetes_jwt_path: VaultConfig::default_kubernetes_jwt_path(),
            kubernetes_mount_path: VaultConfig::default_kubernetes_mount_path(),
        }
    }

    // ── AppRole ───────────────────────────────────────────────────────────────

    #[test]
    fn test_secret_id_inline() {
        assert_eq!(approle_base().secret_id().unwrap(), "inline-secret");
    }

    #[test]
    fn test_secret_id_missing_returns_error() {
        let cfg = VaultConfig {
            secret_id: None,
            secret_id_file: None,
            ..approle_base()
        };
        assert!(cfg.secret_id().is_err());
    }

    #[test]
    fn test_secret_id_file() {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "  file-secret  ").unwrap();
        let cfg = VaultConfig {
            secret_id: None,
            secret_id_file: Some(f.path().to_str().unwrap().into()),
            ..approle_base()
        };
        assert_eq!(cfg.secret_id().unwrap(), "file-secret");
    }

    #[test]
    fn test_secret_id_inline_wins_over_file() {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "file-secret").unwrap();
        let cfg = VaultConfig {
            secret_id: Some("inline".into()),
            secret_id_file: Some(f.path().to_str().unwrap().into()),
            ..approle_base()
        };
        assert_eq!(cfg.secret_id().unwrap(), "inline");
    }

    // ── Kubernetes ────────────────────────────────────────────────────────────

    #[test]
    fn test_kubernetes_mount_path_default() {
        assert_eq!(kubernetes_base().kubernetes_mount_path, "kubernetes");
    }

    #[test]
    fn test_kubernetes_jwt_path_default() {
        assert_eq!(
            kubernetes_base().kubernetes_jwt_path,
            "/var/run/secrets/kubernetes.io/serviceaccount/token"
        );
    }

    #[test]
    fn test_jwt_path_returns_configured_path() {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "  eyJhbGciOiJSUzI1NiJ9.stub  ").unwrap();
        let cfg = VaultConfig {
            kubernetes_jwt_path: f.path().to_str().unwrap().into(),
            ..kubernetes_base()
        };
        // Callers read the file with tokio::fs::read_to_string; test the path is returned.
        assert_eq!(cfg.jwt_path(), f.path().to_str().unwrap());
        // Verify the file itself is readable synchronously (tokio::fs would be the same content).
        let content = std::fs::read_to_string(cfg.jwt_path()).unwrap();
        assert_eq!(content.trim(), "eyJhbGciOiJSUzI1NiJ9.stub");
    }

    #[test]
    fn test_jwt_path_default() {
        let cfg = kubernetes_base();
        assert_eq!(
            cfg.jwt_path(),
            "/var/run/secrets/kubernetes.io/serviceaccount/token"
        );
    }

    // ── serde ─────────────────────────────────────────────────────────────────

    #[test]
    fn test_default_pre_rotation_pct() {
        let toml = r#"
            address   = "https://vault.example.com:8200"
            role_id   = "r"
            secret_id = "s"
        "#;
        let cfg: VaultConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.pre_rotation_pct, 75);
    }

    #[test]
    fn test_kubernetes_defaults_applied_when_not_set() {
        let toml = r#"
            address          = "https://vault.example.com:8200"
            auth_method      = "kubernetes"
            kubernetes_role  = "pgdog"
        "#;
        let cfg: VaultConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.auth_method, VaultAuthMethod::Kubernetes);
        assert_eq!(cfg.kubernetes_role.as_deref(), Some("pgdog"));
        assert_eq!(
            cfg.kubernetes_jwt_path,
            "/var/run/secrets/kubernetes.io/serviceaccount/token"
        );
        assert_eq!(cfg.kubernetes_mount_path, "kubernetes");
    }

    #[test]
    fn test_kubernetes_custom_mount_and_jwt_path() {
        let toml = r#"
            address                = "https://vault.example.com:8200"
            auth_method            = "kubernetes"
            kubernetes_role        = "pgdog"
            kubernetes_mount_path  = "k8s-cluster-a"
            kubernetes_jwt_path    = "/custom/token"
        "#;
        let cfg: VaultConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.kubernetes_mount_path, "k8s-cluster-a");
        assert_eq!(cfg.kubernetes_jwt_path, "/custom/token");
    }

    #[test]
    fn test_unknown_field_rejected() {
        let toml = r#"
            address    = "https://vault.example.com:8200"
            role_id    = "r"
            secret_id  = "s"
            typo_field = true
        "#;
        assert!(toml::from_str::<VaultConfig>(toml).is_err());
    }
}
