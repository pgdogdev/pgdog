//! FIPS 140-3 enforcement.
//!
//! PgDog's cryptography comes from AWS-LC (through `aws-lc-rs` and Rustls).
//! Built with the `fips` feature, that is the validated AWS-LC FIPS module.
//! This module decides whether FIPS is enforced for the process, refuses to
//! run when it is enforced but unavailable, and checks that the TLS
//! configurations PgDog builds actually use it.
//!
//! PgDog does not link OpenSSL, so OpenSSL's own FIPS configuration does not
//! affect it. The host-wide signal is the kernel flag
//! `/proc/sys/crypto/fips_enabled`, which FIPS-enabled distributions set.

use std::path::Path;

use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use once_cell::sync::Lazy;
use tracing::{info, warn};

use crate::config::{AuthType, Config, FipsMode, TlsVerifyMode};

use super::Error;

/// Kernel flag set by FIPS-enabled Linux distributions.
const KERNEL_FIPS_FLAG: &str = "/proc/sys/crypto/fips_enabled";

static HOST_FIPS: Lazy<bool> = Lazy::new(|| host_fips_enabled(Path::new(KERNEL_FIPS_FLAG)));

/// Whether FIPS 140-3 is enforced for this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Enforcement {
    Enforced,
    NotEnforced,
}

impl Enforcement {
    /// Resolve the configured mode against how PgDog was built and the host.
    pub(crate) fn resolve(mode: FipsMode) -> Self {
        Self::resolve_with(mode, *HOST_FIPS)
    }

    fn resolve_with(mode: FipsMode, host_fips: bool) -> Self {
        let enforced = match mode {
            FipsMode::Disabled => false,
            FipsMode::Required => true,
            FipsMode::Auto => cfg!(feature = "fips") || host_fips,
        };

        if enforced {
            Self::Enforced
        } else {
            Self::NotEnforced
        }
    }

    pub(crate) fn enforced(self) -> bool {
        self == Self::Enforced
    }

    /// Reject a TLS configuration Rustls doesn't consider FIPS-compliant
    /// (`ClientConfig::fips()` / `ServerConfig::fips()`).
    pub(crate) fn check_tls(self, config_is_fips: bool, what: &str) -> Result<(), Error> {
        if self.enforced() && !config_is_fips {
            return Err(Error::Fips(format!(
                "{what} TLS configuration is not FIPS-compliant"
            )));
        }

        Ok(())
    }

    /// Reject non-approved algorithms used for authentication.
    pub(crate) fn check_auth(self, auth_type: AuthType) -> Result<(), Error> {
        if self.enforced() && auth_type.md5() {
            return Err(Error::Fips(
                "MD5 authentication is not FIPS-approved".to_string(),
            ));
        }

        Ok(())
    }
}

/// `true` if the kernel flag at `path` reports FIPS mode.
fn host_fips_enabled(path: &Path) -> bool {
    std::fs::read_to_string(path).is_ok_and(|flag| flag.trim() == "1")
}

/// The crypto module is running in FIPS mode.
fn module_status() -> Result<(), Error> {
    if !cfg!(feature = "fips") {
        return Err(Error::Fips(
            "PgDog was built without the `fips` feature; rebuild with `--features fips` or set `fips = \"disabled\"`".to_string(),
        ));
    }

    aws_lc_rs::try_fips_mode()
        .map_err(|err| Error::Fips(format!("AWS-LC is not in FIPS mode: {err}")))
}

/// Startup and reload gate.
///
/// Fails when FIPS is enforced but the crypto module isn't in FIPS mode, or
/// the configuration uses a non-approved algorithm. Deployment settings that
/// weaken a FIPS deployment without using non-approved crypto are logged.
pub(crate) fn check(config: &Config) -> Result<Enforcement, Error> {
    let mode = config.general.fips;
    let enforcement = Enforcement::resolve(mode);

    if !enforcement.enforced() {
        if *HOST_FIPS {
            warn!("host is in FIPS mode but FIPS enforcement is disabled (fips = \"{mode}\")");
        }
        return Ok(enforcement);
    }

    module_status()?;
    enforcement.check_auth(config.general.auth_type)?;

    for finding in audit(config) {
        warn!("FIPS: {finding}");
    }

    info!("🔒 FIPS 140-3 mode enforced (fips = \"{mode}\")");

    Ok(enforcement)
}

/// Settings that weaken a FIPS deployment without using non-approved crypto.
fn audit(config: &Config) -> Vec<String> {
    let general = &config.general;
    let mut findings = vec![];

    if general.tls().is_none() {
        findings.push("client TLS is not configured (tls_certificate, tls_private_key)".into());
    } else if !general.tls_client_required {
        findings.push("clients may connect without TLS (tls_client_required = false)".into());
    }

    if general.passthrough_auth() {
        findings.push("passthrough authentication is enabled".into());
    }

    if general.tls_verify != TlsVerifyMode::VerifyFull {
        findings.push(format!(
            "server TLS is not verify_full (tls_verify = \"{}\")",
            general.tls_verify
        ));
    }

    for database in &config.databases {
        let verify = database.tls.tls_verify.unwrap_or(general.tls_verify);
        if database.tls.tls_verify.is_some() && verify != TlsVerifyMode::VerifyFull {
            findings.push(format!(
                "server TLS for database \"{}\" ({}) is not verify_full (tls_verify = \"{}\")",
                database.name, database.host, verify
            ));
        }
    }

    findings
}

/// Fill `dest` from AWS-LC's system RNG, the module's approved DRBG in FIPS
/// builds. Use for secrets only; other randomness can come from `rand`.
pub(crate) fn fill_random(dest: &mut [u8]) -> Result<(), Error> {
    SystemRandom::new().fill(dest).map_err(|_| Error::Rng)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use crate::config::{Database, General};
    use pgdog_config::PassthroughAuth;

    #[test]
    fn test_resolve_explicit_modes() {
        for host in [true, false] {
            assert_eq!(
                Enforcement::resolve_with(FipsMode::Required, host),
                Enforcement::Enforced
            );
            assert_eq!(
                Enforcement::resolve_with(FipsMode::Disabled, host),
                Enforcement::NotEnforced
            );
        }
    }

    #[test]
    fn test_resolve_auto_follows_host() {
        assert_eq!(
            Enforcement::resolve_with(FipsMode::Auto, true),
            Enforcement::Enforced
        );
    }

    #[test]
    fn test_resolve_auto_follows_build() {
        let expected = if cfg!(feature = "fips") {
            Enforcement::Enforced
        } else {
            Enforcement::NotEnforced
        };
        assert_eq!(Enforcement::resolve_with(FipsMode::Auto, false), expected);
    }

    #[test]
    fn test_host_fips_flag() {
        let flag = |contents: &str| {
            let mut file = tempfile::NamedTempFile::new().unwrap();
            file.write_all(contents.as_bytes()).unwrap();
            host_fips_enabled(file.path())
        };

        assert!(flag("1\n"));
        assert!(!flag("0\n"));
        assert!(!flag(""));
        assert!(!host_fips_enabled(Path::new("/nonexistent/fips_enabled")));
    }

    #[test]
    fn test_check_tls() {
        assert!(Enforcement::Enforced.check_tls(true, "server").is_ok());
        assert!(Enforcement::NotEnforced.check_tls(false, "server").is_ok());

        let err = Enforcement::Enforced
            .check_tls(false, "server")
            .unwrap_err();
        assert!(matches!(err, Error::Fips(_)));
        assert!(err.to_string().contains("server TLS configuration"));
    }

    #[test]
    fn test_check_auth_rejects_md5_when_enforced() {
        assert!(Enforcement::Enforced.check_auth(AuthType::Md5).is_err());
        assert!(Enforcement::NotEnforced.check_auth(AuthType::Md5).is_ok());

        for auth_type in [
            AuthType::Scram,
            AuthType::Trust,
            AuthType::Plain,
            AuthType::ExternalToken,
        ] {
            assert!(Enforcement::Enforced.check_auth(auth_type).is_ok());
        }
    }

    #[test]
    fn test_check_disabled_never_fails() {
        let mut config = Config::default();
        config.general.fips = FipsMode::Disabled;
        config.general.auth_type = AuthType::Md5;

        assert_eq!(check(&config).unwrap(), Enforcement::NotEnforced);
    }

    #[test]
    fn test_check_required_matches_module_status() {
        let mut config = Config::default();
        config.general.fips = FipsMode::Required;

        let result = check(&config);
        if cfg!(feature = "fips") {
            assert_eq!(result.unwrap(), Enforcement::Enforced);
        } else {
            // The kill switch: a non-FIPS build refuses to run.
            let err = result.unwrap_err();
            assert!(err.to_string().contains("without the `fips` feature"));
        }
    }

    #[cfg(feature = "fips")]
    #[test]
    fn test_check_required_rejects_md5() {
        let mut config = Config::default();
        config.general.fips = FipsMode::Required;
        config.general.auth_type = AuthType::Md5;

        let err = check(&config).unwrap_err();
        assert!(err.to_string().contains("MD5"));
    }

    #[test]
    fn test_config_set_refuses_failing_config() {
        crate::config::set(crate::config::ConfigAndUsers::default()).unwrap();

        // Fails on every build: non-FIPS builds can't satisfy `required`, and
        // FIPS builds refuse MD5.
        let mut config = crate::config::ConfigAndUsers::default();
        config.config.general.fips = FipsMode::Required;
        config.config.general.auth_type = AuthType::Md5;

        let err = crate::config::set(config).unwrap_err();
        assert!(err.to_string().starts_with("FIPS:"), "{err}");
        assert!(!crate::config::config().config.general.auth_type.md5());
        assert_ne!(
            crate::config::config().config.general.fips,
            FipsMode::Required
        );
    }

    #[test]
    fn test_audit_compliant_deployment() {
        let config = Config {
            general: General {
                tls_certificate: Some("cert.pem".into()),
                tls_private_key: Some("key.pem".into()),
                tls_client_required: true,
                tls_client_ca_certificate: Some("ca.pem".into()),
                tls_verify: TlsVerifyMode::VerifyFull,
                passthrough_auth: PassthroughAuth::Disabled,
                ..Default::default()
            },
            databases: vec![Database {
                name: "prod".into(),
                host: "db.example.com".into(),
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(audit(&config).is_empty(), "{:?}", audit(&config));
    }

    #[test]
    fn test_audit_reports_weak_settings() {
        let mut database = Database {
            name: "replica".into(),
            host: "replica.example.com".into(),
            ..Default::default()
        };
        database.tls.tls_verify = Some(TlsVerifyMode::Prefer);

        let config = Config {
            general: General {
                tls_verify: TlsVerifyMode::VerifyCa,
                passthrough_auth: PassthroughAuth::EnabledPlain,
                ..Default::default()
            },
            databases: vec![database],
            ..Default::default()
        };

        let findings = audit(&config);
        assert_eq!(findings.len(), 4, "{findings:?}");
        assert!(findings[0].contains("client TLS is not configured"));
        assert!(findings[1].contains("passthrough"));
        assert!(findings[2].contains("server TLS is not verify_full"));
        assert!(findings[3].contains("\"replica\""));
    }

    #[test]
    fn test_audit_requires_client_tls() {
        let config = Config {
            general: General {
                tls_certificate: Some("cert.pem".into()),
                tls_private_key: Some("key.pem".into()),
                tls_client_required: false,
                tls_verify: TlsVerifyMode::VerifyFull,
                ..Default::default()
            },
            ..Default::default()
        };

        let findings = audit(&config);
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert!(findings[0].contains("tls_client_required"));
    }

    #[test]
    fn test_fill_random() {
        let mut a = [0u8; 32];
        let mut b = [0u8; 32];
        fill_random(&mut a).unwrap();
        fill_random(&mut b).unwrap();
        assert_ne!(a, [0u8; 32]);
        assert_ne!(a, b);
    }
}
