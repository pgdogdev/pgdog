//! Cleanup queries for servers altered by client behavior.
use once_cell::sync::Lazy;

use crate::net::{Close, Query};

use super::{super::Server, Guard};

static PREPARED: Lazy<Vec<Query>> = Lazy::new(|| vec![Query::new("DEALLOCATE ALL")]);
/// A dirty connection contains session state which can be safely discard because:
///
/// 1. It should never leak between sessions, e.g., advisory locks, temp tables
/// 2. Because we can re-create it when we check the connection out again, e.g., parameters.
///
static DIRTY: Lazy<Vec<Query>> = Lazy::new(|| {
    vec![
        Query::new("RESET ALL"),                       // Reset all parameters.
        Query::new("SELECT pg_advisory_unlock_all()"), // Remove all advisory locks.
        Query::new("DISCARD TEMP"),                    // Drop all temporary tables.
    ]
});

static ALL: Lazy<Vec<Query>> =
    Lazy::new(|| vec!["DISCARD ALL"].into_iter().map(Query::new).collect());
static NONE: Lazy<Vec<Query>> = Lazy::new(Vec::new);

/// `RESET ROLE` restores the role from the startup packet, which on a pool
/// with `server_role` is the impersonated role, not "no role".
///
/// `RESET ALL` does not do this: `role` is `GUC_NO_RESET_ALL` in PostgreSQL
/// and is skipped. Only `DISCARD ALL` covers it, through its implicit
/// `SET SESSION AUTHORIZATION DEFAULT`.
static ROLE: Lazy<Vec<Query>> = Lazy::new(|| vec![Query::new("RESET ROLE")]);
static DIRTY_ROLE: Lazy<Vec<Query>> = Lazy::new(|| {
    let mut queries = DIRTY.clone();
    queries.push(Query::new("RESET ROLE"));
    queries
});
static PREPARED_ROLE: Lazy<Vec<Query>> = Lazy::new(|| {
    let mut queries = PREPARED.clone();
    queries.push(Query::new("RESET ROLE"));
    queries
});

/// Whether the connection belongs to a pool that impersonates a `server_role`
/// and therefore has to have that role restored before it is reused.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum RoleReset {
    /// The pool does not impersonate a role.
    NotNeeded,
    /// Restore the startup-packet role on check-in.
    Needed,
}

/// Queries used to clean up server connections after
/// client modifications.
pub(crate) struct Cleanup {
    queries: &'static Vec<Query>,
    dirty: bool,
    deallocate: bool,
    close: Vec<Close>,
}

impl Default for Cleanup {
    fn default() -> Self {
        Self {
            queries: &*NONE,
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
    pub(crate) fn new(guard: &Guard, server: &mut Server) -> Self {
        // A pool that impersonates a role has to put that role back before the
        // connection serves another session. The query parser rejects the
        // statements that change `role`, but it cannot see every spelling (a
        // `DO` block, a function body, a computed `set_config` name), and
        // `RESET ALL` skips `role`, so without this an escaped role would
        // outlive the session that set it.
        let role = if server.addr().server_role.is_some() {
            RoleReset::Needed
        } else {
            RoleReset::NotNeeded
        };

        let mut clean = if guard.reset {
            // `DISCARD ALL` already restores the startup-packet role.
            Self::all()
        } else if server.dirty() {
            Self::parameters(role)
        } else if server.schema_changed() {
            Self::prepared_statements(role)
        } else if role == RoleReset::Needed {
            Self::role()
        } else {
            Self::none()
        };

        clean.close = server.ensure_prepared_capacity();

        clean
    }

    /// Number of queries to run for cleanup.
    pub(crate) fn len(&self) -> usize {
        self.queries.len()
    }

    /// Cleanup prepared statements.
    pub(super) fn prepared_statements(role: RoleReset) -> Self {
        Self {
            queries: match role {
                RoleReset::NotNeeded => &*PREPARED,
                RoleReset::Needed => &*PREPARED_ROLE,
            },
            deallocate: true,
            ..Default::default()
        }
    }

    /// Cleanup parameters.
    pub(super) fn parameters(role: RoleReset) -> Self {
        Self {
            queries: match role {
                RoleReset::NotNeeded => &*DIRTY,
                RoleReset::Needed => &*DIRTY_ROLE,
            },
            dirty: true,
            ..Default::default()
        }
    }

    /// Restore the impersonated role and nothing else.
    pub(super) fn role() -> Self {
        Self {
            queries: &*ROLE,
            ..Default::default()
        }
    }

    /// Cleanup everything.
    pub(crate) fn all() -> Self {
        Self {
            dirty: true,
            deallocate: true,
            queries: &*ALL,
            close: vec![],
        }
    }

    /// Nothing to clean up.
    pub(crate) fn none() -> Self {
        Self::default()
    }

    /// Cleanup needed?
    pub(crate) fn needed(&self) -> bool {
        !self.queries.is_empty() || !self.close.is_empty()
    }

    /// Get queries to execute on the server to perform cleanup.
    pub(crate) fn queries(&self) -> &[Query] {
        self.queries
    }

    /// Prepared statemens to close.
    pub(crate) fn close(&self) -> &[Close] {
        &self.close
    }

    pub(crate) fn is_reset_params(&self) -> bool {
        self.dirty
    }

    pub(crate) fn is_deallocate(&self) -> bool {
        self.deallocate
    }
}
