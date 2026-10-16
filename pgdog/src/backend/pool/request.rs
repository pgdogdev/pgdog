use tokio::time::Instant;

use crate::net::messages::FrontendPid;

/// Which servers a connection request can use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum RoleTarget {
    /// The primary only: writes, conservative read/write transactions,
    /// `prefer_primary`, role `primary`.
    #[default]
    Primary,
    /// Replicas; the primary only if no replica is available. Role `replica`.
    PreferReplica,
    /// Any server that `read_write_split` allows. Reads without an override.
    Any,
}

impl RoleTarget {
    /// Combine the explicit decision (role, transaction, cluster settings)
    /// with the read/write classification of the statement.
    pub(crate) fn new(explicit: Option<Self>, statement_read: bool) -> Self {
        match explicit {
            Some(role_target) => role_target,
            None if statement_read => Self::Any,
            None => Self::Primary,
        }
    }

    /// The request can use a replica.
    pub(crate) fn allows_replica(self) -> bool {
        self != Self::Primary
    }
}

/// Connection request.
#[derive(Clone, Debug, Copy)]
pub(crate) struct Request {
    pub(crate) id: FrontendPid,
    pub(crate) created_at: Instant,

    // Which servers this request can use.
    pub(crate) role_target: RoleTarget,
}

impl Request {
    pub(crate) fn new(id: FrontendPid, role_target: RoleTarget) -> Self {
        Self {
            id,
            created_at: Instant::now(),
            role_target,
        }
    }

    pub(crate) fn unrouted(id: FrontendPid) -> Self {
        Self::new(id, RoleTarget::default())
    }
}

impl Default for Request {
    fn default() -> Self {
        Self::unrouted(FrontendPid::new())
    }
}

#[cfg(test)]
mod test {
    use super::RoleTarget::{self, *};

    #[test]
    fn test_decision_combines_explicit_and_statement() {
        for (explicit, statement_read, expected) in [
            (None, true, Any),
            (None, false, Primary),
            (Some(Primary), true, Primary),
            (Some(PreferReplica), false, PreferReplica),
            (Some(Any), false, Any),
        ] {
            assert_eq!(
                RoleTarget::new(explicit, statement_read),
                expected,
                "explicit={explicit:?} statement_read={statement_read}"
            );
        }
    }
}
