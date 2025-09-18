//! GSSAPI authentication configuration.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// GSSAPI authentication configuration.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct GssapiConfig {
    /// Enable GSSAPI authentication.
    #[serde(default)]
    pub enabled: bool,

    /// Keytab file for accepting client connections (frontend).
    /// This is the keytab containing PGDog's service principal.
    pub server_keytab: Option<PathBuf>,

    /// Service principal name for PGDog (frontend).
    /// Default: postgres/hostname@REALM
    pub server_principal: Option<String>,

    /// Default keytab for backend connections.
    /// Can be overridden per-database.
    pub default_backend_keytab: Option<PathBuf>,

    /// Default principal for backend connections.
    /// Can be overridden per-database.
    pub default_backend_principal: Option<String>,

    /// Default target service principal for backend connections.
    /// Can be overridden per-database or per-user.
    pub default_backend_target_principal: Option<String>,

    /// Strip realm from client principals.
    /// If true: user@REALM -> user
    #[serde(default = "GssapiConfig::default_strip_realm")]
    pub strip_realm: bool,

    /// Ticket refresh interval in seconds.
    /// How often to refresh Kerberos tickets before they expire.
    #[serde(default = "GssapiConfig::default_ticket_refresh_interval")]
    pub ticket_refresh_interval: u64,

    /// Fall back to other auth methods if GSSAPI fails.
    #[serde(default)]
    pub fallback_enabled: bool,

    /// Require GSSAPI encryption (gssencmode).
    /// Note: Enabling this will prevent SQL inspection/modification.
    #[serde(default)]
    pub require_encryption: bool,
}

impl Default for GssapiConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            server_keytab: None,
            server_principal: None,
            default_backend_keytab: None,
            default_backend_principal: None,
            default_backend_target_principal: None,
            strip_realm: Self::default_strip_realm(),
            ticket_refresh_interval: Self::default_ticket_refresh_interval(),
            fallback_enabled: false,
            require_encryption: false,
        }
    }
}

impl GssapiConfig {
    fn default_strip_realm() -> bool {
        true
    }

    fn default_ticket_refresh_interval() -> u64 {
        14400 // 4 hours
    }

    /// Check if GSSAPI is properly configured.
    pub fn is_configured(&self) -> bool {
        self.enabled && self.server_keytab.is_some()
    }

    /// Check if backend GSSAPI is configured.
    pub fn has_backend_config(&self) -> bool {
        self.default_backend_keytab.is_some() && self.default_backend_principal.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = GssapiConfig::default();
        assert!(!config.enabled);
        assert!(config.server_keytab.is_none());
        assert!(config.server_principal.is_none());
        assert!(config.strip_realm);
        assert_eq!(config.ticket_refresh_interval, 14400);
        assert!(!config.fallback_enabled);
        assert!(!config.require_encryption);
    }

    #[test]
    fn test_is_configured() {
        let mut config = GssapiConfig::default();
        assert!(!config.is_configured());

        config.enabled = true;
        assert!(!config.is_configured());

        config.server_keytab = Some(PathBuf::from("/etc/pgdog/pgdog.keytab"));
        assert!(config.is_configured());
    }

    #[test]
    fn test_has_backend_config() {
        let mut config = GssapiConfig::default();
        assert!(!config.has_backend_config());

        config.default_backend_keytab = Some(PathBuf::from("/etc/pgdog/backend.keytab"));
        assert!(!config.has_backend_config());

        config.default_backend_principal = Some("pgdog@REALM".to_string());
        assert!(config.has_backend_config());
    }

    #[test]
    fn test_gssapi_config_from_toml() {
        let toml_str = r#"
            enabled = true
            server_keytab = "/etc/pgdog/pgdog.keytab"
            server_principal = "postgres/pgdog.example.com@EXAMPLE.COM"
            default_backend_keytab = "/etc/pgdog/backend.keytab"
            default_backend_principal = "pgdog@EXAMPLE.COM"
            strip_realm = false
            ticket_refresh_interval = 7200
            fallback_enabled = true
            require_encryption = false
        "#;

        let config: GssapiConfig = toml::from_str(toml_str).unwrap();
        assert!(config.enabled);
        assert_eq!(
            config.server_keytab,
            Some(PathBuf::from("/etc/pgdog/pgdog.keytab"))
        );
        assert_eq!(
            config.server_principal,
            Some("postgres/pgdog.example.com@EXAMPLE.COM".to_string())
        );
        assert_eq!(
            config.default_backend_keytab,
            Some(PathBuf::from("/etc/pgdog/backend.keytab"))
        );
        assert_eq!(
            config.default_backend_principal,
            Some("pgdog@EXAMPLE.COM".to_string())
        );
        assert!(!config.strip_realm);
        assert_eq!(config.ticket_refresh_interval, 7200);
        assert!(config.fallback_enabled);
        assert!(!config.require_encryption);
        assert!(config.is_configured());
        assert!(config.has_backend_config());
    }

    #[test]
    fn test_partial_gssapi_config() {
        let toml_str = r#"
            enabled = true
            server_keytab = "/etc/pgdog/pgdog.keytab"
        "#;

        let config: GssapiConfig = toml::from_str(toml_str).unwrap();
        assert!(config.enabled);
        assert_eq!(
            config.server_keytab,
            Some(PathBuf::from("/etc/pgdog/pgdog.keytab"))
        );
        assert!(config.server_principal.is_none());
        assert!(config.strip_realm); // Should use default
        assert_eq!(config.ticket_refresh_interval, 14400); // Should use default
        assert!(!config.fallback_enabled); // Should use default
        assert!(config.is_configured());
        assert!(!config.has_backend_config());
    }

    #[test]
    fn test_minimal_gssapi_config() {
        let toml_str = r#"enabled = false"#;

        let config: GssapiConfig = toml::from_str(toml_str).unwrap();
        assert!(!config.enabled);
        assert!(!config.is_configured());
        assert!(!config.has_backend_config());
    }
}
