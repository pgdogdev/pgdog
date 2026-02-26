use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Duration;

use crate::util::human_duration_optional;
use schemars::JsonSchema;

/// how to handle TLS connections to Postgres servers.
///
/// By default, PgDog will attempt to establish TLS and will accept any server certificate.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/general/#tls_verify
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Copy)]
#[serde(rename_all = "snake_case")]
#[derive(JsonSchema)]
pub enum TlsVerifyMode {
    /// TLS is disabled.
    #[default]
    Disabled,
    /// Use TLS if available; do not verify the server certificate (default).
    Prefer,
    /// Validate the server certificate against a CA bundle.
    VerifyCa,
    /// Validate the server certificate and that the hostname matches.
    VerifyFull,
}

impl FromStr for TlsVerifyMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().replace(['_', '-'], "").as_str() {
            "disabled" => Ok(Self::Disabled),
            "prefer" => Ok(Self::Prefer),
            "verifyca" => Ok(Self::VerifyCa),
            "verifyfull" => Ok(Self::VerifyFull),
            _ => Err(format!("Invalid TLS verify mode: {}", s)),
        }
    }
}

/// TCP settings for client and server connections.
///
/// Optimal TCP settings are necessary to quickly recover from database incidents.
///
/// **Note:** Not all networks support or play well with TCP keep-alives. If you see an increased number of dropped connections after enabling these settings, you may have to disable them.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/network/
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[derive(JsonSchema)]
pub struct Tcp {
    #[serde(default = "Tcp::default_keepalive")]
    keepalive: bool,
    user_timeout: Option<u64>,
    time: Option<u64>,
    interval: Option<u64>,
    retries: Option<u32>,
    congestion_control: Option<String>,
}

impl std::fmt::Display for Tcp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "keepalive={} user_timeout={} time={} interval={}, retries={}, congestion_control={}",
            self.keepalive(),
            human_duration_optional(self.user_timeout()),
            human_duration_optional(self.time()),
            human_duration_optional(self.interval()),
            if let Some(retries) = self.retries() {
                retries.to_string()
            } else {
                "default".into()
            },
            if let Some(ref c) = self.congestion_control {
                c.as_str()
            } else {
                ""
            },
        )
    }
}

impl Default for Tcp {
    fn default() -> Self {
        Self {
            keepalive: Self::default_keepalive(),
            user_timeout: None,
            time: None,
            interval: None,
            retries: None,
            congestion_control: None,
        }
    }
}

impl Tcp {
    fn default_keepalive() -> bool {
        true
    }

    pub fn keepalive(&self) -> bool {
        self.keepalive
    }

    pub fn time(&self) -> Option<Duration> {
        self.time.map(Duration::from_millis)
    }

    pub fn interval(&self) -> Option<Duration> {
        self.interval.map(Duration::from_millis)
    }

    pub fn user_timeout(&self) -> Option<Duration> {
        self.user_timeout.map(Duration::from_millis)
    }

    pub fn retries(&self) -> Option<u32> {
        self.retries
    }

    pub fn congestion_control(&self) -> &Option<String> {
        &self.congestion_control
    }
}

/// multi-tenant routing configuration, mapping queries to shards via a tenant identifier column.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "snake_case")]
#[derive(JsonSchema)]
pub struct MultiTenant {
    /// Name of the column carrying the tenant identifier used to route queries.
    pub column: String,
}
