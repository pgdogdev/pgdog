use pgdog_config::SessionRole;

use crate::frontend::router::sharding::ShardOrLookup;

/// Information that was parsed from a comment on the query string received
/// from a client
#[derive(Debug, Clone, Default)]
pub(in crate::frontend) struct RoutingComment {
    pub(super) shard: Option<ShardOrLookup>,
    pub(super) role: Option<SessionRole>,
    pub(super) sharding_key: Option<String>,
}
