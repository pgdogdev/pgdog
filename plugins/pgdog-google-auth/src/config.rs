use std::{env, fs, path::Path, time::Duration};

use reqwest::{Url, blocking::Client, redirect::Policy};
use serde::Deserialize;
use thiserror::Error;
use url::Host;

const DEFAULT_TOKENINFO_URL: &str = "https://oauth2.googleapis.com/tokeninfo";
const DEFAULT_TIMEOUT_MS: u64 = 5_000;
const MIN_TIMEOUT_MS: u64 = 100;
const MAX_TIMEOUT_MS: u64 = 60_000;

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum UsernameClaim {
    #[default]
    Email,
    UserId,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Settings {
    pub(crate) tokeninfo_url: String,
    pub(crate) timeout_ms: u64,
    pub(crate) require_tls: bool,
    pub(crate) username_claim: UsernameClaim,
    pub(crate) strip_email_domain: bool,
    pub(crate) require_user_match: bool,
    pub(crate) require_verified_email: bool,
    pub(crate) allowed_audiences: Vec<String>,
    pub(crate) allowed_domains: Vec<String>,
    pub(crate) allowed_emails: Vec<String>,
    pub(crate) required_scopes: Vec<String>,
    pub(crate) provision: bool,
    pub(crate) impersonate: bool,
    pub(crate) server_user: Option<String>,
    pub(crate) server_password_env: Option<String>,
    pub(crate) read_only: Option<bool>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            tokeninfo_url: DEFAULT_TOKENINFO_URL.into(),
            timeout_ms: DEFAULT_TIMEOUT_MS,
            require_tls: true,
            username_claim: UsernameClaim::Email,
            strip_email_domain: false,
            require_user_match: true,
            require_verified_email: true,
            allowed_audiences: Vec::new(),
            allowed_domains: Vec::new(),
            allowed_emails: Vec::new(),
            required_scopes: Vec::new(),
            provision: false,
            impersonate: true,
            server_user: None,
            server_password_env: None,
            read_only: None,
        }
    }
}

pub(crate) struct RuntimeConfig {
    pub(crate) settings: Settings,
    pub(crate) endpoint: Url,
    pub(crate) client: Client,
    pub(crate) server_password: Option<String>,
}

impl RuntimeConfig {
    pub(crate) fn load(path: Option<&Path>) -> Result<Self, ConfigError> {
        let settings = match path {
            Some(path) => {
                let contents = fs::read_to_string(path).map_err(|source| ConfigError::Read {
                    path: path.display().to_string(),
                    source,
                })?;
                toml::from_str(&contents).map_err(|source| ConfigError::Parse {
                    path: path.display().to_string(),
                    source,
                })?
            }
            None => Settings::default(),
        };

        Self::from_settings(settings)
    }

    pub(crate) fn from_settings(mut settings: Settings) -> Result<Self, ConfigError> {
        if !(MIN_TIMEOUT_MS..=MAX_TIMEOUT_MS).contains(&settings.timeout_ms) {
            return Err(ConfigError::Invalid(format!(
                "timeout_ms must be between {MIN_TIMEOUT_MS} and {MAX_TIMEOUT_MS}"
            )));
        }

        normalize_list(&mut settings.allowed_audiences, "allowed_audiences", false)?;
        normalize_list(&mut settings.required_scopes, "required_scopes", false)?;
        normalize_list(&mut settings.allowed_domains, "allowed_domains", true)?;
        normalize_list(&mut settings.allowed_emails, "allowed_emails", true)?;

        let endpoint = validate_endpoint(&settings.tokeninfo_url)?;
        let timeout = Duration::from_millis(settings.timeout_ms);
        let client = Client::builder()
            .connect_timeout(timeout)
            .timeout(timeout)
            .redirect(Policy::none())
            .user_agent(concat!("pgdog-google-auth/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(ConfigError::Client)?;

        let server_password = if settings.provision {
            if settings.allowed_domains.is_empty() && settings.allowed_emails.is_empty() {
                return Err(ConfigError::Invalid(
                    "provision = true requires allowed_domains or allowed_emails".into(),
                ));
            }

            let server_user = settings
                .server_user
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    ConfigError::Invalid("provision = true requires server_user".into())
                })?;
            settings.server_user = Some(server_user.to_owned());

            let variable = settings
                .server_password_env
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    ConfigError::Invalid("provision = true requires server_password_env".into())
                })?
                .to_owned();
            settings.server_password_env = Some(variable.clone());

            let password = env::var(&variable).map_err(|_| ConfigError::MissingSecret {
                variable: variable.clone(),
            })?;
            if password.is_empty() {
                return Err(ConfigError::MissingSecret { variable });
            }
            Some(password)
        } else {
            None
        };

        Ok(Self {
            settings,
            endpoint,
            client,
            server_password,
        })
    }
}

fn normalize_list(
    values: &mut [String],
    field: &'static str,
    lowercase: bool,
) -> Result<(), ConfigError> {
    for value in values {
        *value = value.trim().to_owned();
        if value.is_empty() {
            return Err(ConfigError::Invalid(format!(
                "{field} cannot contain empty values"
            )));
        }
        if lowercase {
            value.make_ascii_lowercase();
        }
    }
    Ok(())
}

fn validate_endpoint(value: &str) -> Result<Url, ConfigError> {
    let url = Url::parse(value).map_err(ConfigError::Url)?;

    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ConfigError::Invalid(
            "tokeninfo_url cannot contain credentials, a query, or a fragment".into(),
        ));
    }

    match url.scheme() {
        "https" => Ok(url),
        "http" if loopback(&url) => Ok(url),
        _ => Err(ConfigError::Invalid(
            "tokeninfo_url must use HTTPS; HTTP is allowed only for loopback tests".into(),
        )),
    }
}

fn loopback(url: &Url) -> bool {
    match url.host() {
        Some(Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    }
}

#[derive(Debug, Error)]
pub(crate) enum ConfigError {
    #[error("failed to read Google auth config {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse Google auth config {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: toml::de::Error,
    },
    #[error("invalid tokeninfo_url: {0}")]
    Url(#[source] url::ParseError),
    #[error("failed to create Google tokeninfo client: {0}")]
    Client(#[source] reqwest::Error),
    #[error("missing backend password in environment variable {variable}")]
    MissingSecret { variable: String },
    #[error("invalid Google auth configuration: {0}")]
    Invalid(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_secure() {
        let settings = Settings::default();

        assert!(settings.require_tls);
        assert!(settings.require_user_match);
        assert!(settings.require_verified_email);
        assert!(settings.impersonate);
        assert!(!settings.provision);
        assert_eq!(settings.username_claim, UsernameClaim::Email);
    }

    #[test]
    fn rejects_non_loopback_http_endpoint() {
        let settings = Settings {
            tokeninfo_url: "http://example.com/tokeninfo".into(),
            ..Default::default()
        };

        assert!(RuntimeConfig::from_settings(settings).is_err());
    }

    #[test]
    fn accepts_loopback_http_endpoint_for_tests() {
        for tokeninfo_url in [
            "http://127.0.0.1:12345/tokeninfo",
            "http://[::1]:12345/tokeninfo",
            "http://localhost:12345/tokeninfo",
        ] {
            let settings = Settings {
                tokeninfo_url: tokeninfo_url.into(),
                ..Default::default()
            };

            assert!(
                RuntimeConfig::from_settings(settings).is_ok(),
                "{tokeninfo_url}"
            );
        }
    }

    #[test]
    fn rejects_auto_provisioning_without_principal_allowlist() {
        let settings = Settings {
            provision: true,
            server_user: Some("pgdog_service".into()),
            server_password_env: Some("PGDOG_TEST_PASSWORD".into()),
            ..Default::default()
        };

        let error = RuntimeConfig::from_settings(settings)
            .err()
            .expect("configuration should fail");
        assert!(error.to_string().contains("allowed_domains"));
    }

    #[test]
    fn parses_config_file() {
        let directory = tempfile::tempdir().expect("create temp directory");
        let path = directory.path().join("google-auth.toml");
        fs::write(
            &path,
            r#"
require_tls = true
username_claim = "user_id"
allowed_domains = ["Example.COM"]
required_scopes = ["scope-a"]
"#,
        )
        .expect("write config");

        let runtime = RuntimeConfig::load(Some(&path)).expect("load config");
        assert_eq!(runtime.settings.username_claim, UsernameClaim::UserId);
        assert_eq!(runtime.settings.allowed_domains, ["example.com"]);
        assert_eq!(runtime.settings.required_scopes, ["scope-a"]);
    }
}
