//! Shortcut the parser given the cluster config.

use pgdog_config::Role;

use crate::backend::pool::RoleTarget;

use crate::frontend::router::parser::ShardsWithPriority;
use crate::frontend::router::sharding::PendingLookup;
use crate::{
    backend::ShardingSchema,
    config::{MultiTenant, ReadWriteStrategy},
    frontend::{BufferedQuery, RouterContext},
};

use super::Error;

/// Query parser context.
///
/// Contains a lot of info we collect from the router context
/// and its inputs.
///
#[derive(Clone)]
pub(crate) struct QueryParserContext<'a> {
    /// At least one shard has a primary.
    pub(super) has_primary: bool,
    /// At least one shard has a replica.
    pub(super) has_replicas: bool,
    /// Number of shards in the cluster.
    pub(super) shards: usize,
    /// Are there any sharding tables in the database?
    pub(super) sharded_tables: bool,
    /// Which tables are sharded and using which columns.
    pub(super) sharding_schema: &'a ShardingSchema,
    /// Context created by the router.
    pub(super) router_context: RouterContext<'a>,
    /// How aggressively we want to send reads to replicas.
    pub(super) rw_strategy: &'a ReadWriteStrategy,
    /// Route all queries to replicas by default unless an explicit role hint says otherwise.
    pub(super) prefer_replica: bool,
    /// Do we need the router at all? Shortcut to bypass this for unsharded
    /// clusters with databases that only read or write.
    pub(super) router_needed: bool,
    /// Are we running multi-tenant checks?
    pub(super) multi_tenant: &'a Option<MultiTenant>,
    /// Dry run enabled?
    pub(super) dry_run: bool,
    /// Expanded EXPLAIN annotations enabled?
    pub(super) expanded_explain: bool,
    /// Shards calculator.
    pub(super) shards_calculator: ShardsWithPriority,
    /// Sharding key lookups that missed the cache while routing;
    /// returned to the query engine on the route.
    pub(super) pending_lookups: Vec<PendingLookup>,
    /// Lookups for bare sharding keys (a comment directive or `SET
    /// pgdog.sharding_key`) that missed the cache. Kept separate until
    /// the statement is classified: they don't apply to omnisharded
    /// writes, which route to every shard regardless of the key.
    pub(super) bare_key_lookups: Vec<PendingLookup>,
    /// Role requested by the client session (`pgdog.role`), falling back
    /// to the role configured for the user.
    pub(super) session_role: Option<Role>,
}

impl<'a> QueryParserContext<'a> {
    /// Create query parser context from router context.
    pub(crate) fn new(router_context: RouterContext<'a>) -> Result<Self, Error> {
        let mut shards_calculator = ShardsWithPriority::default();
        let mut bare_key_lookups = Vec::new();

        let sharded_tables = !router_context.cluster.sharded_tables().tables().is_empty();
        let sharding_schema = router_context.cluster.sharding_schema();

        router_context.parameter_hints.compute_shard(
            &mut shards_calculator,
            &mut bare_key_lookups,
            &router_context.resolved_lookups,
            sharding_schema,
        )?;

        let session_role = router_context
            .parameter_hints
            .compute_role()
            .or_else(|| router_context.cluster.role());

        Ok(Self {
            has_primary: router_context.cluster.has_primary(),
            has_replicas: router_context.cluster.has_replicas(),
            shards: router_context.cluster.shards().len(),
            sharded_tables,
            sharding_schema,
            rw_strategy: router_context.cluster.read_write_strategy(),
            prefer_replica: router_context.cluster.prefer_replica(),
            router_needed: router_context.cluster.router_needed(),
            multi_tenant: router_context.cluster.multi_tenant(),
            dry_run: router_context.cluster.dry_run(),
            expanded_explain: router_context.cluster.expanded_explain(),
            session_role,
            router_context,
            shards_calculator,
            pending_lookups: Vec::new(),
            bare_key_lookups,
        })
    }

    /// Check if the role target was explicitly forced by the following entities:
    /// 1. `pgdog.role` through user.role/SET/sql comment - that will override any other source.
    ///    `auto` stops the lookup without forcing a role, so the rules below apply.
    /// 2. BEGIN or implicit write transaction with [`ReadWriteStrategy::Conservative`],
    ///    if the cluster has a primary. Only the session role overrides it, not the sql comment.
    /// 3. `BEGIN READ ONLY` with any strategy: any server, whatever the statements are.
    ///
    /// The cluster decides which servers can serve the role target.
    pub(super) fn explicit_role_target(&self) -> Option<RoleTarget> {
        let transaction = self.router_context.transaction();
        let transaction_write = self.rw_conservative()
            && self.has_primary
            && transaction.is_some_and(|transaction| transaction.write());
        let transaction_read_only = transaction.is_some_and(|transaction| !transaction.write());

        let role = if transaction_write {
            // for conservative transactions we ignore the comment
            // and rely only on `pgdog.role` GUC.
            // To avoid possible issues when later request is write
            self.session_role
        } else {
            self.router_context.pgdog_role().or(self.session_role)
        };

        match role {
            Some(Role::Primary) => Some(RoleTarget::Primary),
            Some(Role::Replica) => Some(RoleTarget::PreferReplica),
            _ if transaction_write => Some(RoleTarget::Primary),
            // for read-only transaction we can route to any server
            // override the route decision by forcing Any
            _ if transaction_read_only => Some(RoleTarget::Any),
            _ => None,
        }
    }

    /// Are we using the conservative read/write separation strategy?
    pub(super) fn rw_conservative(&self) -> bool {
        self.rw_strategy == &ReadWriteStrategy::Conservative
    }

    /// Get the query we're parsing, if any.
    pub(super) fn query(&self) -> Result<&BufferedQuery, Error> {
        self.router_context.query.as_ref().ok_or(Error::EmptyQuery)
    }

    /// Multi-tenant checks.
    pub(super) fn multi_tenant(&self) -> &Option<MultiTenant> {
        self.multi_tenant
    }

    pub(super) fn expanded_explain(&self) -> bool {
        self.expanded_explain
    }

    /// Are we running in session mode?
    ///
    /// In session mode, queries are forwarded to the server without
    /// parsing or validation beyond what's required for routing.
    pub(super) fn is_session_mode(&self) -> bool {
        self.router_context.cluster.pooler_mode() == crate::config::PoolerMode::Session
    }

    pub(super) fn is_canonicalizing_oids(&self) -> bool {
        self.router_context.cluster.is_canonicalizing_oids()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use pgdog_config::Role::{Auto, Primary, Replica};

    use super::*;
    use crate::{
        backend::Cluster,
        config::config,
        frontend::{
            ClientRequest,
            client::{Sticky, TransactionType, transaction_type::Transaction},
            router::RoutingComment,
        },
        net::Query,
        parameters::ParameterStore,
    };

    #[derive(Default)]
    struct Input {
        user: Option<Role>,
        param: Option<&'static str>,
        comment: Option<Role>,
        transaction: Option<TransactionType>,
        strategy: ReadWriteStrategy,
    }

    fn role_target_on(mut cluster: Cluster, input: Input) -> Option<RoleTarget> {
        cluster.set_read_write_strategy(input.strategy);
        cluster.set_role(input.user);

        let mut params = ParameterStore::default();
        if let Some(role) = input.param {
            params.set("pgdog.role", role);
        }

        let mut request = ClientRequest::from(vec![Query::new("SELECT 1").into()]);
        request.routing_comment = input.comment.map(|role| {
            Arc::new(RoutingComment {
                role: Some(role),
                ..Default::default()
            })
        });

        let router_context = RouterContext::new(
            &request,
            &cluster,
            &params,
            input.transaction.map(Transaction::new),
            Sticky::new_test(),
        )
        .unwrap();

        QueryParserContext::new(router_context)
            .unwrap()
            .explicit_role_target()
    }

    fn role_target(input: Input) -> Option<RoleTarget> {
        role_target_on(Cluster::new_test(&config()), input)
    }

    #[test]
    fn test_no_role_lets_statement_decide() {
        assert_eq!(role_target(Input::default()), None);
    }

    #[test]
    fn test_user_role() {
        let input = Input {
            user: Some(Replica),
            ..Default::default()
        };
        assert_eq!(role_target(input), Some(RoleTarget::PreferReplica));
    }

    #[test]
    fn test_param_overrides_user_role() {
        let input = Input {
            user: Some(Replica),
            param: Some("primary"),
            ..Default::default()
        };
        assert_eq!(role_target(input), Some(RoleTarget::Primary));
    }

    #[test]
    fn test_comment_overrides_param() {
        let input = Input {
            param: Some("replica"),
            comment: Some(Primary),
            ..Default::default()
        };
        assert_eq!(role_target(input), Some(RoleTarget::Primary));
    }

    #[test]
    fn test_auto_param_clears_user_role() {
        let input = Input {
            user: Some(Replica),
            param: Some("auto"),
            ..Default::default()
        };
        assert_eq!(role_target(input), None);
    }

    #[test]
    fn test_auto_comment_clears_param() {
        let input = Input {
            param: Some("primary"),
            comment: Some(Auto),
            ..Default::default()
        };
        assert_eq!(role_target(input), None);
    }

    #[test]
    fn test_conservative_transaction_uses_primary() {
        let input = Input {
            transaction: Some(TransactionType::ReadWrite),
            strategy: ReadWriteStrategy::Conservative,
            ..Default::default()
        };
        assert_eq!(role_target(input), Some(RoleTarget::Primary));
    }

    #[test]
    fn test_conservative_transaction_ignores_comment() {
        let input = Input {
            comment: Some(Replica),
            transaction: Some(TransactionType::ReadWrite),
            strategy: ReadWriteStrategy::Conservative,
            ..Default::default()
        };
        assert_eq!(role_target(input), Some(RoleTarget::Primary));
    }

    #[test]
    fn test_conservative_transaction_keeps_param() {
        let input = Input {
            param: Some("replica"),
            transaction: Some(TransactionType::ReadWrite),
            strategy: ReadWriteStrategy::Conservative,
            ..Default::default()
        };
        assert_eq!(role_target(input), Some(RoleTarget::PreferReplica));
    }

    #[test]
    fn test_conservative_transaction_without_primary() {
        let input = Input {
            transaction: Some(TransactionType::ReadWrite),
            strategy: ReadWriteStrategy::Conservative,
            ..Default::default()
        };
        let cluster = Cluster::new_test_single_replica(&config());
        assert_eq!(role_target_on(cluster, input), None);
    }

    #[test]
    fn test_aggressive_transaction_lets_statement_decide() {
        let input = Input {
            transaction: Some(TransactionType::ReadWrite),
            strategy: ReadWriteStrategy::Aggressive,
            ..Default::default()
        };
        assert_eq!(role_target(input), None);
    }

    #[test]
    fn test_aggressive_transaction_uses_comment() {
        let input = Input {
            comment: Some(Replica),
            transaction: Some(TransactionType::ReadWrite),
            strategy: ReadWriteStrategy::Aggressive,
            ..Default::default()
        };
        assert_eq!(role_target(input), Some(RoleTarget::PreferReplica));
    }

    #[test]
    fn test_read_only_transaction_uses_any_server() {
        let input = Input {
            transaction: Some(TransactionType::ReadOnly),
            strategy: ReadWriteStrategy::Conservative,
            ..Default::default()
        };
        assert_eq!(role_target(input), Some(RoleTarget::Any));
    }
}
