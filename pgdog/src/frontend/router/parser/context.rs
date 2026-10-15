//! Shortcut the parser given the cluster config.

use pgdog_config::SessionRole;

use crate::frontend::client::TransactionType;
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
    /// Cluster is read-only, i.e. has no primary.
    pub(super) read_only: bool,
    /// Cluster has no replicas, only a primary.
    pub(super) write_only: bool,
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
    /// Route reads to the primary by default unless an explicit role hint says otherwise.
    pub(super) prefer_primary: bool,
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
    pub(super) session_role: Option<SessionRole>,
    /// Read/write returned by a plugin for this statement.
    pub(super) plugin_read: Option<bool>,
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
            read_only: router_context.cluster.read_only(),
            write_only: router_context.cluster.write_only(),
            shards: router_context.cluster.shards().len(),
            sharded_tables,
            sharding_schema,
            rw_strategy: router_context.cluster.read_write_strategy(),
            prefer_primary: router_context.cluster.prefer_primary(),
            prefer_replica: router_context.cluster.prefer_replica(),
            router_needed: router_context.cluster.router_needed(),
            multi_tenant: router_context.cluster.multi_tenant(),
            dry_run: router_context.cluster.dry_run(),
            expanded_explain: router_context.cluster.expanded_explain(),
            session_role,
            plugin_read: None,
            router_context,
            shards_calculator,
            pending_lookups: Vec::new(),
            bare_key_lookups,
        })
    }

    /// Read/write intent fixed by `BEGIN`: `BEGIN READ ONLY` reads and
    /// a conservative `BEGIN` writes. `None` lets the statement decide.
    fn transaction_read(&self) -> Option<bool> {
        match self.transaction_type()? {
            TransactionType::ReadOnly | TransactionType::ErrorReadOnly => Some(true),
            TransactionType::ReadWrite | TransactionType::ErrorReadWrite
                if self.rw_conservative() =>
            {
                Some(false)
            }
            _ => None,
        }
    }

    /// Role requested for this statement. The query comment cannot change
    /// a transaction whose intent is fixed; the session role can. A cluster
    /// with only a primary or only replicas has nothing to choose from.
    fn requested_role(&self, transaction_fixed: bool) -> Option<SessionRole> {
        if self.read_only || self.write_only {
            return None;
        }

        if transaction_fixed {
            self.session_role
        } else {
            self.router_context
                .comment
                .and_then(|comment| comment.role)
                .or(self.session_role)
        }
    }

    /// Final read/write decision for a statement, from the highest priority:
    /// cluster without a primary, requested role, transaction intent, plugin,
    /// then `default_read`, the decision of the statement itself. A statement
    /// read goes to the primary with `prefer_primary`, or inside a
    /// conservative implicit transaction.
    pub(super) fn route_read(&self, default_read: bool) -> bool {
        if self.read_only {
            return true;
        }

        let transaction_read = self.transaction_read();

        if let Some(role) = self.requested_role(transaction_read.is_some()) {
            return role == SessionRole::Replica;
        }

        transaction_read.or(self.plugin_read).unwrap_or_else(|| {
            let implicit_write = self.rw_conservative()
                && self.transaction_type() == Some(TransactionType::Implicit);

            default_read && !self.prefer_primary && !implicit_write
        })
    }

    /// Type of the transaction this statement runs in.
    fn transaction_type(&self) -> Option<TransactionType> {
        self.router_context
            .transaction()
            .map(|transaction| transaction.transaction_type())
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
    use pgdog_config::SessionRole::{Primary, Replica};

    use super::*;
    use crate::{
        backend::Cluster,
        config::config,
        frontend::{
            ClientRequest,
            client::{Sticky, transaction_type::Transaction},
            router::RoutingComment,
        },
        net::Query,
        parameters::ParameterStore,
    };

    #[derive(Default)]
    struct Input {
        transaction: Option<TransactionType>,
        param: Option<&'static str>,
        user: Option<SessionRole>,
        comment: Option<SessionRole>,
        plugin: Option<bool>,
        strategy: ReadWriteStrategy,
        split: ReadWriteSplit,
    }

    fn route_read_on(mut cluster: Cluster, input: Input, default_read: bool) -> bool {
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
        let mut context = QueryParserContext::new(router_context).unwrap();
        context.plugin_read = input.plugin;

        context.route_read(default_read)
    }

    fn route_read(input: Input, default_read: bool) -> bool {
        route_read_on(Cluster::new_test(&config()), input, default_read)
    }

    const READ: bool = true;
    const WRITE: bool = false;

    #[test]
    fn test_statement_decides_without_overrides() {
        assert!(route_read(Input::default(), READ));
        assert!(!route_read(Input::default(), WRITE));
    }

    #[test]
    fn test_cluster_without_primary_reads() {
        let cluster = Cluster::new_test_single_replica(&config());
        let input = Input {
            param: Some("primary"),
            transaction: Some(TransactionType::ReadWrite),
            ..Default::default()
        };

        assert!(route_read_on(cluster, input, WRITE));
    }

    #[test]
    fn test_cluster_without_replicas_ignores_role() {
        let input = || Input {
            param: Some("replica"),
            comment: Some(Replica),
            ..Default::default()
        };
        let cluster = || Cluster::new_test_single_primary(&config());

        assert!(!route_read_on(cluster(), input(), WRITE));
    }

    #[test]
    fn test_role_precedence() {
        for (comment, param, user, read_expected) in [
            (None, None, Some(Replica), true),
            (None, Some("primary"), Some(Replica), false),
            (Some(Replica), Some("primary"), Some(Primary), true),
            (Some(Primary), Some("replica"), None, false),
            (None, Some("auto"), Some(Replica), true),
        ] {
            let input = Input {
                comment,
                param,
                user,
                ..Default::default()
            };

            assert_eq!(
                route_read(input, WRITE),
                read_expected,
                "comment={comment:?} param={param:?} user={user:?}"
            );
        }
    }

    #[test]
    fn test_transaction_intent() {
        use ReadWriteStrategy::{Aggressive, Conservative};
        use TransactionType::{ErrorReadOnly, ErrorReadWrite, Implicit, ReadOnly, ReadWrite};

        for (strategy, transaction, default_read, read_expected) in [
            (Conservative, ReadWrite, READ, false),
            (Conservative, ErrorReadWrite, READ, false),
            (Conservative, Implicit, READ, false),
            (Conservative, ReadOnly, WRITE, true),
            (Conservative, ErrorReadOnly, WRITE, true),
            (Aggressive, ReadWrite, READ, true),
            (Aggressive, ReadWrite, WRITE, false),
            (Aggressive, Implicit, READ, true),
            (Aggressive, ReadOnly, WRITE, true),
        ] {
            let input = Input {
                strategy,
                transaction: Some(transaction),
                ..Default::default()
            };

            assert_eq!(
                route_read(input, default_read),
                read_expected,
                "{strategy:?} {transaction:?}"
            );
        }
    }

    #[test]
    fn test_role_inside_transaction() {
        use ReadWriteStrategy::{Aggressive, Conservative};
        use TransactionType::{Implicit, ReadOnly, ReadWrite};

        for (strategy, transaction, comment, param, read_expected) in [
            (Conservative, ReadWrite, Some(Replica), None, false),
            (Conservative, ReadWrite, None, Some("replica"), true),
            (
                Conservative,
                ReadWrite,
                Some(Primary),
                Some("replica"),
                true,
            ),
            (Conservative, ReadOnly, Some(Primary), None, true),
            (Conservative, ReadOnly, None, Some("primary"), false),
            (Conservative, Implicit, Some(Replica), None, true),
            (Aggressive, ReadWrite, Some(Replica), None, true),
            (Aggressive, ReadWrite, Some(Replica), Some("primary"), true),
        ] {
            let input = Input {
                strategy,
                transaction: Some(transaction),
                comment,
                param,
                ..Default::default()
            };

            assert_eq!(
                route_read(input, READ),
                read_expected,
                "{strategy:?} {transaction:?} comment={comment:?} param={param:?}"
            );
        }
    }

    #[test]
    fn test_plugin_precedence() {
        let plugin_write = || Input {
            plugin: Some(false),
            ..Default::default()
        };

        assert!(!route_read(plugin_write(), READ));
        assert!(route_read(
            Input {
                plugin: Some(true),
                ..Default::default()
            },
            WRITE
        ));
        assert!(route_read(
            Input {
                param: Some("replica"),
                ..plugin_write()
            },
            READ
        ));
        assert!(route_read(
            Input {
                transaction: Some(TransactionType::ReadOnly),
                ..plugin_write()
            },
            READ
        ));
    }

    #[test]
    fn test_prefer_primary() {
        let prefer_primary = || Input {
            split: ReadWriteSplit::PreferPrimary,
            ..Default::default()
        };

        assert!(!route_read(prefer_primary(), READ));
        assert!(route_read(
            Input {
                param: Some("replica"),
                ..prefer_primary()
            },
            READ
        ));
        assert!(route_read(
            Input {
                comment: Some(Replica),
                ..prefer_primary()
            },
            READ
        ));
    }
}
