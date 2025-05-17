//! Request to use a prepared statement.

use crate::net::messages::Bind;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum PreparedRequest {
    Prepare { name: String },
    Describe { name: String },
    PrepareNew { name: String },
    Bind { bind: Bind },
}

impl PreparedRequest {
    pub fn new(name: &str, new: bool) -> Self {
        if new {
            Self::PrepareNew {
                name: name.to_string(),
            }
        } else {
            Self::Prepare {
                name: name.to_string(),
            }
        }
    }

    pub fn new_describe(name: &str) -> Self {
        Self::Describe {
            name: name.to_owned(),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Prepare { name } => name,
            Self::Describe { name } => name,
            Self::PrepareNew { name } => name,
            Self::Bind { bind } => bind.statement(),
        }
    }

    pub fn is_new(&self) -> bool {
        matches!(self, Self::PrepareNew { .. })
    }

    pub fn is_prepare(&self) -> bool {
        matches!(self, Self::Prepare { .. } | Self::PrepareNew { .. })
    }
}
