use serde::{Deserialize, Serialize};
use std::{fmt::Display, str::FromStr};

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PassthoughAuth {
    #[default]
    Disabled,
    Enabled,
    EnabledPlain,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AuthType {
    Md5,
    #[default]
    Scram,
    Trust,
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
