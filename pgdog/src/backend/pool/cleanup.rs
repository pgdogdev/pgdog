//! Cleanup queries for servers altered by client behavior.
use super::{super::Server, Guard};

/// Queries used to clean up server connections after
/// client modifications.
pub struct Cleanup {
    queries: Vec<&'static str>,
}

impl std::fmt::Display for Cleanup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.queries.join(","))
    }
}

impl Cleanup {
    /// New cleanup operation.
    pub fn new(guard: &Guard, server: &Server) -> Self {
        if guard.reset {
            Self::all()
        } else if server.dirty() {
            Self::parameters()
        } else {
            Self::none()
        }
    }

    /// Cleanup prepared statements.
    pub fn prepared_statements() -> Self {
        Self {
            queries: vec!["DISCARD ALL".into()],
        }
    }

    /// Cleanup parameters.
    pub fn parameters() -> Self {
        Self {
            queries: vec!["RESET ALL".into()],
        }
    }

    /// Cleanup everything.
    pub fn all() -> Self {
        Self {
            queries: vec!["RESET ALL".into(), "DISCARD ALL".into()],
        }
    }

    /// Nothing to clean up.
    pub fn none() -> Self {
        Self { queries: vec![] }
    }

    /// Cleanup needed?
    pub fn needed(&self) -> bool {
        !self.queries.is_empty()
    }

    /// Get queries to execute on the server to perform cleanup.
    pub fn queries(&self) -> &[&str] {
        &self.queries
    }
}
