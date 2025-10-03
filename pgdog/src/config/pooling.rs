use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PreparedStatements {
    Disabled,
    #[default]
    Extended,
    Full,
}

impl PreparedStatements {
    pub fn full(&self) -> bool {
        matches!(self, PreparedStatements::Full)
    }

    pub fn enabled(&self) -> bool {
        !matches!(self, PreparedStatements::Disabled)
    }
}

impl FromStr for PreparedStatements {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "disabled" => Ok(Self::Disabled),
            "extended" => Ok(Self::Extended),
            "full" => Ok(Self::Full),
            _ => Err(format!("Invalid prepared statements mode: {}", s)),
        }
    }
}

/// Empty struct for stats
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Stats {}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq, Ord, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum PoolerMode {
    #[default]
    Transaction,
    Session,
}

impl std::fmt::Display for PoolerMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transaction => write!(f, "transaction"),
            Self::Session => write!(f, "session"),
        }
    }
}

impl FromStr for PoolerMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "transaction" => Ok(Self::Transaction),
            "session" => Ok(Self::Session),
            _ => Err(format!("Invalid pooler mode: {}", s)),
        }
    }
}
