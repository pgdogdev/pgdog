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
pub enum PassthroughAuth {
    /// Passthrough auth is disabled (default).
    #[default]
    Disabled,
    /// Enabled; plaintext passwords are not permitted without TLS.
    Enabled,
    /// Enabled without TLS requirement; network traffic may expose plaintext passwords.
    EnabledPlain,
    /// Enabled and allows password changes.
    EnabledAllowChange,
    /// Enabled without TLS requirement and allows password changes.
    EnabledPlainAllowChange,
}

impl PassthroughAuth {
    pub fn allows_change(&self) -> bool {
        matches!(
            self,
            Self::EnabledPlainAllowChange | Self::EnabledAllowChange
        )
    }
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
    /// Delegate client authentication to a loaded plugin exposing the `pgdog_authenticate` hook.
    Plugin,
}

impl Display for AuthType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Md5 => write!(f, "md5"),
            Self::Scram => write!(f, "scram"),
            Self::Trust => write!(f, "trust"),
            Self::Plain => write!(f, "plain"),
            Self::Plugin => write!(f, "plugin"),
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

    pub fn plugin(&self) -> bool {
        matches!(self, Self::Plugin)
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
            "plugin" => Ok(Self::Plugin),
            _ => Err(format!("Invalid auth type: {}", s)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_type_default_is_scram() {
        assert_eq!(AuthType::default(), AuthType::Scram);
    }

    #[test]
    fn test_auth_type_display() {
        assert_eq!(AuthType::Md5.to_string(), "md5");
        assert_eq!(AuthType::Scram.to_string(), "scram");
        assert_eq!(AuthType::Trust.to_string(), "trust");
        assert_eq!(AuthType::Plain.to_string(), "plain");
        assert_eq!(AuthType::Plugin.to_string(), "plugin");
    }

    #[test]
    fn test_auth_type_from_str() {
        assert_eq!("md5".parse::<AuthType>().unwrap(), AuthType::Md5);
        assert_eq!("scram".parse::<AuthType>().unwrap(), AuthType::Scram);
        assert_eq!("trust".parse::<AuthType>().unwrap(), AuthType::Trust);
        assert_eq!("plain".parse::<AuthType>().unwrap(), AuthType::Plain);
        assert_eq!("plugin".parse::<AuthType>().unwrap(), AuthType::Plugin);
        // Case-insensitive.
        assert_eq!("PLUGIN".parse::<AuthType>().unwrap(), AuthType::Plugin);
        assert!("nonsense".parse::<AuthType>().is_err());
    }

    #[test]
    fn test_auth_type_predicates() {
        assert!(AuthType::Plugin.plugin());
        assert!(!AuthType::Scram.plugin());
        assert!(!AuthType::Plugin.scram());
        assert!(!AuthType::Plugin.md5());
        assert!(!AuthType::Plugin.trust());
    }

    #[test]
    fn test_auth_type_serde_round_trip() {
        // serde uses snake_case renaming; "plugin" is a single lowercase word.
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Wrapper {
            auth_type: AuthType,
        }

        let source = r#"auth_type = "plugin""#;
        let parsed: Wrapper = toml::from_str(source).unwrap();
        assert_eq!(parsed.auth_type, AuthType::Plugin);

        let serialized = toml::to_string(&parsed).unwrap();
        assert!(serialized.contains(r#"auth_type = "plugin""#));

        let round_tripped: Wrapper = toml::from_str(&serialized).unwrap();
        assert_eq!(round_tripped, parsed);
    }
}
