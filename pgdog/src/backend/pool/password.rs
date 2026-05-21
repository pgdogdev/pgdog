use std::{
    fmt::Display,
    hash::Hash,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PasswordSource {
    Config,
    RdsIam,
    AzureIdentity,
}

impl Display for PasswordSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config => write!(f, "config"),
            Self::RdsIam => write!(f, "rds iam"),
            Self::AzureIdentity => write!(f, "azure workload identity"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Password {
    pub(crate) password: String,
    pub(crate) valid: Arc<AtomicBool>,
    pub(crate) source: PasswordSource,
}

impl From<String> for Password {
    fn from(password: String) -> Self {
        Self::new(&password, PasswordSource::Config)
    }
}

impl From<&str> for Password {
    fn from(password: &str) -> Self {
        Self::new(password, PasswordSource::Config)
    }
}

impl Hash for Password {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.password.hash(state);
        self.is_valid().hash(state);
    }
}

impl Eq for Password {}

impl PartialEq<&str> for Password {
    fn eq(&self, other: &&str) -> bool {
        self.password.as_str() == *other
    }
}

impl PartialEq<str> for Password {
    fn eq(&self, other: &str) -> bool {
        self.password.as_str() == other
    }
}

impl PartialEq for Password {
    fn eq(&self, other: &Self) -> bool {
        self.password == other.password
    }
}

impl Deref for Password {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.password
    }
}

impl Password {
    pub(crate) fn is_valid(&self) -> bool {
        self.valid.load(Ordering::Relaxed)
    }

    pub(crate) fn valid(&self, valid: bool) {
        self.valid.store(valid, Ordering::Relaxed)
    }

    pub(crate) fn new(password: &str, source: PasswordSource) -> Self {
        Self {
            password: password.to_owned(),
            source,
            valid: Arc::new(AtomicBool::new(true)),
        }
    }
}
