use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, str::FromStr};

/// toggle automatic creation of connection pools given the user name, database and password.
///
/// See [passthrough authentication](https://docs.pgdog.dev/features/authentication/#passthrough-authentication).
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/general/#passthrough_auth
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PassthoughAuth {
    /// Passthrough auth is disabled (default).
    #[default]
    Disabled,
    /// Enabled; plaintext passwords are not permitted without TLS.
    Enabled,
    /// Enabled without TLS requirement; network traffic may expose plaintext passwords.
    EnabledPlain,
}

/// authentication mechanism for client connections.
///
/// See [authentication](https://docs.pgdog.dev/features/authentication/).
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/general/#auth_type
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuthType {
    /// MD5 password hashing; very quick but not secure.
    Md5,
    /// SCRAM-SHA-256 authentication; slow but has better security features (default).
    #[default]
    Scram,
    /// No authentication required; clients are trusted unconditionally.
    Trust,
    /// Plaintext password.
    Plain,
}

impl Display for AuthType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Md5 => write!(f, "md5"),
            Self::Scram => write!(f, "scram"),
            Self::Trust => write!(f, "trust"),
            Self::Plain => write!(f, "plain"),
        }
    }
}

impl AuthType {
    pub fn md5(&self) -> bool {
        matches!(self, Self::Md5)
    }

    pub fn scram(&self) -> bool {
        matches!(self, Self::Scram)
    }

    pub fn trust(&self) -> bool {
        matches!(self, Self::Trust)
    }
}

impl FromStr for AuthType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "md5" => Ok(Self::Md5),
            "scram" => Ok(Self::Scram),
            "trust" => Ok(Self::Trust),
            "plain" => Ok(Self::Plain),
            _ => Err(format!("Invalid auth type: {}", s)),
        }
    }
}
