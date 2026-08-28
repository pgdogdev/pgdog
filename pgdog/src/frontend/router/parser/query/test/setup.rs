use std::ops::Deref;

use pgdog_config::{ConfigAndUsers, ReadWriteSplit};

use crate::{
    backend::Cluster,
    config::{self, ReadWriteStrategy, config},
    frontend::{
        ClientRequest, Command, PreparedStatements, RouterContext,
        client::{Sticky, TransactionType},
        router::{
            QueryParser,
            parser::{AstContext, Cache, Error},
        },
    },
    net::{Parameters, ProtocolMessage, parameter::ParameterValue},
};

pub(super) use crate::net::*;

/// Test harness for QueryParser that uses builder pattern for configuration.
pub(crate) struct QueryParserTest {
    cluster: Cluster,
    params: Parameters,
    transaction: Option<TransactionType>,
    sticky: Sticky,
    prepared: PreparedStatements,
    pub(crate) parser: QueryParser,
    last_parse: Option<String>,
    resolved_lookups: crate::frontend::router::sharding::ResolvedLookups,
}

impl QueryParserTest {
    /// Create a new test with default settings (no transaction, default cluster).
    pub(crate) fn new() -> Self {
        Self::new_with_config(&config())
    }

    /// Create a test with a single-shard cluster.
    pub(crate) fn new_single_shard(config: &ConfigAndUsers) -> Self {
        let cluster = Cluster::new_test_single_shard(config);

        Self {
            cluster,
            params: Parameters::default(),
            transaction: None,
            sticky: Sticky::default(),
            parser: QueryParser::default(),
            prepared: PreparedStatements::new(),
            last_parse: None,
            resolved_lookups: Default::default(),
        }
    }

    /// Create new test with specific general settings.
    pub(crate) fn new_with_config(config: &ConfigAndUsers) -> Self {
        let cluster = Cluster::new_test(config);

        Self {
            cluster,
            params: Parameters::default(),
            transaction: None,
            sticky: Sticky::default(),
            parser: QueryParser::default(),
            prepared: PreparedStatements::new(),
            last_parse: None,
            resolved_lookups: Default::default(),
        }
    }

    pub(crate) fn new_single_primary(config: &ConfigAndUsers) -> Self {
        let mut me = Self::new_with_config(config);
        me.cluster = Cluster::new_test_single_primary(config);

        me
    }

    pub(crate) fn new_single_replica(config: &ConfigAndUsers) -> Self {
        let mut me = Self::new_with_config(config);
        me.cluster = Cluster::new_test_single_replica(config);

        me
    }

    pub(crate) fn new_session_mode(config: &ConfigAndUsers) -> Self {
        let mut me = Self::new_with_config(config);
        me.cluster = Cluster::new_test_session_mode(config);

        me
    }

    /// Set whether we're in a transaction.
    pub(crate) fn in_transaction(mut self, in_tx: bool) -> Self {
        self.transaction = if in_tx {
            Some(TransactionType::ReadWrite)
        } else {
            None
        };
        self
    }

    /// Set the read/write strategy on the cluster.
    pub(crate) fn with_read_write_strategy(mut self, strategy: ReadWriteStrategy) -> Self {
        self.cluster.set_read_write_strategy(strategy);
        self
    }

    /// Enable rewriting of simple-protocol PREPARE/EXECUTE statements.
    pub(crate) fn with_full_prepared_statements(mut self) -> Self {
        self.prepared
            .set_level(pgdog_config::PreparedStatementsLevel::Full);
        self
    }

    /// Replace the sharded tables configuration on the cluster.
    pub(crate) fn with_sharded_tables(
        mut self,
        sharded_tables: crate::backend::ShardedTables,
    ) -> Self {
        self.cluster.set_sharded_tables(sharded_tables);
        self
    }

    /// Sharding key translations resolved for the statements this
    /// test executes, as if a first routing pass had resolved them.
    pub(crate) fn with_resolved_lookups(
        mut self,
        resolved: crate::frontend::router::sharding::ResolvedLookups,
    ) -> Self {
        self.resolved_lookups = resolved;
        self
    }

    /// Remove the sharded schemas configuration from the cluster.
    pub(crate) fn without_sharded_schemas(mut self) -> Self {
        self.cluster
            .set_sharded_schemas(crate::backend::replication::ShardedSchemas::default());
        self
    }

    /// Route reads to the primary by default on the cluster.
    pub(crate) fn with_rw_split(mut self, rw_split: ReadWriteSplit) -> Self {
        self.cluster.set_rw_split(rw_split);
        self
    }

    /// Enable expanded explain for this test.
    pub(crate) fn with_expanded_explain(mut self) -> Self {
        let mut updated = config().deref().clone();
        updated.config.general.expanded_explain = true;
        config::set(updated).unwrap();
        self.cluster = Cluster::new_test(&config());
        self
    }

    /// Enable dry run mode for this test.
    pub(crate) fn with_dry_run(mut self) -> Self {
        let mut updated = config().deref().clone();
        updated.config.general.dry_run = true;
        config::set(updated).unwrap();
        // Recreate cluster with the new config
        self.cluster = Cluster::new_test(&config());
        self
    }

    /// Set a parameter value.
    pub(crate) fn with_param(
        mut self,
        name: impl AsRef<str>,
        value: impl Into<ParameterValue>,
    ) -> Self {
        self.params.insert(name, value);
        self
    }

    /// Startup parameters.
    ///
    /// Execute a request and return the command (panics on error).
    pub(crate) fn execute(&mut self, request: Vec<ProtocolMessage>) -> Command {
        self.try_execute(request).expect("execute failed")
    }

    /// Execute a request and return Result (for testing error conditions).
    pub(crate) fn try_execute(&mut self, request: Vec<ProtocolMessage>) -> Result<Command, Error> {
        let mut request: ClientRequest = request.into();

        for message in request.iter_mut() {
            if let ProtocolMessage::Parse(parse) = message {
                let (_, name) = PreparedStatements::global().write().insert(parse);
                self.last_parse = Some(name);
            }

            if let ProtocolMessage::Bind(bind) = message
                && let Some(ref name) = self.last_parse
            {
                bind.rename(name);
            }

            if let ProtocolMessage::Describe(desc) = message
                && let Some(ref name) = self.last_parse
            {
                desc.rename(name);
            }
        }

        let use_parser = self.cluster.use_query_parser(&request);

        if use_parser {
            // Some requests (like Close) don't have a query
            if let Ok(Some(buffered_query)) = request.query() {
                let ctx = AstContext::from_cluster(&self.cluster, &self.params);
                // The engine surfaces cache-time errors (e.g. a comment
                // directive that fails to resolve) as client errors.
                let ast = Cache::get().query(&buffered_query, &ctx, &mut self.prepared)?;
                request.ast = Some(ast);
            }
        }

        let router_ctx = RouterContext::new(
            &request,
            &self.cluster,
            &self.params,
            self.transaction,
            self.sticky,
        )
        .unwrap()
        .with_resolved_lookups(self.resolved_lookups.clone());

        let command = self.parser.parse(router_ctx)?;
        Ok(command.clone())
    }

    /// Get access to the cluster (for assertions).
    pub(crate) fn cluster(&self) -> &Cluster {
        &self.cluster
    }
}
