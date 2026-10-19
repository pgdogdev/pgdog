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
    /// 2. BEGIN or implicit write transaction with [`ReadWriteStrategy::Conservative`].
    ///    Only the session role overrides it, not the sql comment.
    /// 3. `BEGIN READ ONLY` with any strategy: any server, whatever the statements are.
    ///
    /// The cluster decides which servers can serve the role target.
    pub(super) fn explicit_role_target(&self) -> Option<RoleTarget> {
        let transaction = self.router_context.transaction();
        let transaction_write =
            self.rw_conservative() && transaction.is_some_and(|transaction| transaction.write());
        let transaction_read_only = transaction.is_some_and(|transaction| !transaction.write());

        let role = if transaction_write {
            self.session_role
        } else {
            self.router_context.pgdog_role().or(self.session_role)
        };

        match role {
            Some(Role::Primary) => Some(RoleTarget::Primary),
            Some(Role::Replica) => Some(RoleTarget::PreferReplica),
            Some(Role::Auto) | None if transaction_write => Some(RoleTarget::Primary),
            Some(Role::Auto) | None if transaction_read_only => Some(RoleTarget::Any),
            Some(Role::Auto) | None => None,
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

    use pgdog_config::ReadWriteSplit;
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
        transaction: Option<TransactionType>,
        param: Option<&'static str>,
        user: Option<Role>,
        comment: Option<Role>,
        strategy: ReadWriteStrategy,
        split: ReadWriteSplit,
    }

    const REPLICA: Option<RoleTarget> = Some(RoleTarget::PreferReplica);
    const PRIMARY: Option<RoleTarget> = Some(RoleTarget::Primary);
    const STATEMENT: Option<RoleTarget> = None;
    const ANY: Option<RoleTarget> = Some(RoleTarget::Any);

    fn decision_on(mut cluster: Cluster, input: Input) -> Option<RoleTarget> {
        cluster.set_read_write_strategy(input.strategy);
        cluster.set_rw_split(input.split);
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

    fn decision(input: Input) -> Option<RoleTarget> {
        decision_on(Cluster::new_test(&config()), input)
    }

    #[test]
    fn test_statement_decides_without_overrides() {
        assert_eq!(decision(Input::default()), STATEMENT);
    }

    #[test]
    fn test_role_precedence() {
        for (comment, param, user, expected) in [
            (None, None, Some(Replica), REPLICA),
            (None, Some("primary"), Some(Replica), PRIMARY),
            (Some(Replica), Some("primary"), Some(Primary), REPLICA),
            (Some(Primary), Some("replica"), None, PRIMARY),
            (None, Some("auto"), Some(Replica), STATEMENT),
            (None, Some("invalid"), Some(Replica), REPLICA),
            (Some(Auto), None, Some(Replica), STATEMENT),
            (Some(Auto), Some("primary"), None, STATEMENT),
            (None, None, Some(Auto), STATEMENT),
        ] {
            let input = Input {
                comment,
                param,
                user,
                ..Default::default()
            };

            assert_eq!(
                decision(input),
                expected,
                "comment={comment:?} param={param:?} user={user:?}"
            );
        }
    }

    /// An explicit role applies on every topology.
    #[test]
    fn test_role_ignores_topology() {
        for (cluster, role, expected) in [
            (
                Cluster::new_test_single_primary(&config()),
                "replica",
                REPLICA,
            ),
            (
                Cluster::new_test_single_primary(&config()),
                "primary",
                PRIMARY,
            ),
            (
                Cluster::new_test_single_replica(&config()),
                "replica",
                REPLICA,
            ),
            (
                Cluster::new_test_single_replica(&config()),
                "primary",
                PRIMARY,
            ),
        ] {
            let input = Input {
                param: Some(role),
                transaction: Some(TransactionType::ReadWrite),
                ..Default::default()
            };

            assert_eq!(decision_on(cluster, input), expected, "role={role}");
        }
    }

    /// The cluster topology and `read_write_split` do not change the role target.
    #[test]
    fn test_role_target_ignores_cluster() {
        let transaction = || Input {
            transaction: Some(TransactionType::ReadWrite),
            ..Default::default()
        };
        let prefer_primary = || Input {
            split: ReadWriteSplit::PreferPrimary,
            ..Default::default()
        };

        let replica_only = || Cluster::new_test_single_replica(&config());
        assert_eq!(decision_on(replica_only(), transaction()), PRIMARY);
        assert_eq!(decision_on(replica_only(), prefer_primary()), STATEMENT);

        let primary_only = || Cluster::new_test_single_primary(&config());
        assert_eq!(decision_on(primary_only(), transaction()), PRIMARY);
        assert_eq!(decision_on(primary_only(), prefer_primary()), STATEMENT);
    }

    #[test]
    fn test_transaction_intent() {
        use ReadWriteStrategy::{Aggressive, Conservative};
        use TransactionType::{ErrorReadOnly, ErrorReadWrite, Implicit, ReadOnly, ReadWrite};

        for (strategy, transaction, expected) in [
            (Conservative, ReadWrite, PRIMARY),
            (Conservative, ErrorReadWrite, PRIMARY),
            (Conservative, Implicit, PRIMARY),
            (Conservative, ReadOnly, ANY),
            (Conservative, ErrorReadOnly, ANY),
            (Aggressive, ReadWrite, STATEMENT),
            (Aggressive, Implicit, STATEMENT),
            (Aggressive, ReadOnly, ANY),
            (Aggressive, ErrorReadOnly, ANY),
        ] {
            let input = Input {
                strategy,
                transaction: Some(transaction),
                ..Default::default()
            };

            assert_eq!(decision(input), expected, "{strategy:?} {transaction:?}");
        }
    }

    #[test]
    fn test_role_inside_transaction() {
        use ReadWriteStrategy::{Aggressive, Conservative};
        use TransactionType::{ErrorReadOnly, Implicit, ReadOnly, ReadWrite};

        for (strategy, transaction, comment, param, expected) in [
            (Conservative, ReadWrite, Some(Replica), None, PRIMARY),
            (Conservative, ReadWrite, None, Some("replica"), REPLICA),
            (
                Conservative,
                ReadWrite,
                Some(Primary),
                Some("replica"),
                REPLICA,
            ),
            (Conservative, ReadOnly, Some(Primary), None, PRIMARY),
            (Conservative, ErrorReadOnly, Some(Primary), None, PRIMARY),
            (Conservative, ReadOnly, None, Some("primary"), PRIMARY),
            (Conservative, Implicit, Some(Replica), None, PRIMARY),
            (Conservative, Implicit, None, Some("replica"), REPLICA),
            (Aggressive, ReadWrite, Some(Replica), None, REPLICA),
            (
                Aggressive,
                ReadWrite,
                Some(Replica),
                Some("primary"),
                REPLICA,
            ),
        ] {
            let input = Input {
                strategy,
                transaction: Some(transaction),
                comment,
                param,
                ..Default::default()
            };

            assert_eq!(
                decision(input),
                expected,
                "{strategy:?} {transaction:?} comment={comment:?} param={param:?}"
            );
        }
    }
}
