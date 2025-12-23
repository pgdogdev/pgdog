use std::ops::Deref;

use crate::{
    backend::Cluster,
    config::{self, config, ReadWriteStrategy},
    frontend::{
        client::{Sticky, TransactionType},
        router::{
            parser::{Cache, Error},
            QueryParser,
        },
        ClientRequest, Command, PreparedStatements, RouterContext,
    },
    net::{parameter::ParameterValue, Parameters, ProtocolMessage},
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
}

impl QueryParserTest {
    /// Create a new test with default settings (no transaction, default cluster).
    pub(crate) fn new() -> Self {
        let cluster = Cluster::new_test();

        Self {
            cluster,
            params: Parameters::default(),
            transaction: None,
            sticky: Sticky::default(),
            parser: QueryParser::default(),
            prepared: PreparedStatements::new(),
            last_parse: None,
        }
    }

    /// Create a test with a single-shard cluster.
    pub(crate) fn new_single_shard() -> Self {
        let cluster = Cluster::new_test_single_shard();

        Self {
            cluster,
            params: Parameters::default(),
            transaction: None,
            sticky: Sticky::default(),
            parser: QueryParser::default(),
            prepared: PreparedStatements::new(),
            last_parse: None,
        }
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

    /// Enable dry run mode for this test.
    pub(crate) fn with_dry_run(mut self) -> Self {
        let mut updated = config().deref().clone();
        updated.config.general.dry_run = true;
        config::set(updated).unwrap();
        // Recreate cluster with the new config
        self.cluster = Cluster::new_test();
        self
    }

    /// Set a parameter value.
    pub(crate) fn with_param(
        mut self,
        name: impl ToString,
        value: impl Into<ParameterValue>,
    ) -> Self {
        self.params.insert(name, value);
        self
    }

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

            if let ProtocolMessage::Bind(bind) = message {
                if let Some(ref name) = self.last_parse {
                    bind.rename(name);
                }
            }

            if let ProtocolMessage::Describe(desc) = message {
                if let Some(ref name) = self.last_parse {
                    desc.rename(name);
                }
            }
        }

        // Some requests (like Close) don't have a query
        if let Ok(Some(buffered_query)) = request.query() {
            let ast = Cache::get()
                .query(
                    &buffered_query,
                    &self.cluster.sharding_schema(),
                    &mut self.prepared,
                )
                .unwrap();
            request.ast = Some(ast);
        }

        let router_ctx = RouterContext::new(
            &request,
            &self.cluster,
            &self.params,
            self.transaction,
            self.sticky,
        )
        .unwrap();

        let command = self.parser.parse(router_ctx)?;
        Ok(command.clone())
    }

    /// Get access to the cluster (for assertions).
    pub(crate) fn cluster(&self) -> &Cluster {
        &self.cluster
    }
}
