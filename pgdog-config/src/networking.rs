use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Duration;

use crate::util::human_duration_optional;

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Copy)]
#[serde(rename_all = "snake_case")]
pub enum TlsVerifyMode {
    #[default]
    Disabled,
    Prefer,
    VerifyCa,
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

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
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

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct MultiTenant {
    pub column: String,
}
