//! Cleanup queries for servers altered by client behavior.
use once_cell::sync::Lazy;

use crate::net::{Close, Query};

use super::{super::Server, Guard};

static PREPARED: Lazy<Vec<Query>> = Lazy::new(|| vec![Query::new("DEALLOCATE ALL")]);
static DIRTY: Lazy<Vec<Query>> = Lazy::new(|| {
    vec![
        Query::new("RESET ALL"),
        Query::new("SELECT pg_advisory_unlock_all()"),
    ]
});
static ALL: Lazy<Vec<Query>> =
    Lazy::new(|| vec!["DISCARD ALL"].into_iter().map(Query::new).collect());
static NONE: Lazy<Vec<Query>> = Lazy::new(Vec::new);

/// Queries used to clean up server connections after
/// client modifications.
#[allow(dead_code)]
pub struct Cleanup {
    queries: &'static Vec<Query>,
    reset: bool,
    dirty: bool,
    deallocate: bool,
    close: Vec<Close>,
}

impl Default for Cleanup {
    fn default() -> Self {
        Self {
            queries: &*NONE,
            reset: false,
            dirty: false,
            deallocate: false,
            close: vec![],
        }
    }
}

impl std::fmt::Display for Cleanup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.queries
                .iter()
                .map(|s| s.query())
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}

impl Cleanup {
    /// New cleanup operation.
    pub fn new(guard: &Guard, server: &mut Server) -> Self {
        let mut clean = if guard.reset {
            Self::all()
        } else if server.dirty() {
            Self::parameters()
        } else if server.schema_changed() {
            println!("\n\n\nschema changed");
            Self::prepared_statements()
        } else {
            Self::none()
        };

        clean.close = server.ensure_prepared_capacity();

        clean
    }

    /// Cleanup prepared statements.
    pub fn prepared_statements() -> Self {
        Self {
            queries: &*PREPARED,
            deallocate: true,
            ..Default::default()
        }
    }

    /// Cleanup parameters.
    pub fn parameters() -> Self {
        Self {
            queries: &*DIRTY,
            dirty: true,
            ..Default::default()
        }
    }

    /// Cleanup everything.
    pub fn all() -> Self {
        Self {
            reset: true,
            dirty: true,
            deallocate: true,
            queries: &*ALL,
            close: vec![],
        }
    }

    /// Nothing to clean up.
    pub fn none() -> Self {
        Self::default()
    }

    /// Cleanup needed?
    pub fn needed(&self) -> bool {
        !self.queries.is_empty() || !self.close.is_empty()
    }

    /// Get queries to execute on the server to perform cleanup.
    pub fn queries(&self) -> &[Query] {
        self.queries
    }

    /// Prepared statemens to close.
    pub fn close(&self) -> &[Close] {
        &self.close
    }

    pub fn is_reset_params(&self) -> bool {
        self.dirty
    }

    pub fn is_deallocate(&self) -> bool {
        self.deallocate
    }
}
