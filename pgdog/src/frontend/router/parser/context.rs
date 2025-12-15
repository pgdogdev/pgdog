//! Shortcut the parser given the cluster config.

use std::os::raw::c_void;

use pgdog_plugin::pg_query::protobuf::ParseResult;
use pgdog_plugin::{PdParameters, PdRouterContext, PdStatement};

use crate::frontend::client::TransactionType;
use crate::net::Bind;
use crate::{
    backend::ShardingSchema,
    config::{MultiTenant, ReadWriteStrategy, RewriteMode},
    frontend::{BufferedQuery, RouterContext},
};

use super::Error;

/// Query parser context.
///
/// Contains a lot of info we collect from the router context
/// and its inputs.
///
pub struct QueryParserContext<'a> {
    /// Cluster is read-only, i.e. has no primary.
    pub(super) read_only: bool,
    /// Cluster has no replicas, only a primary.
    pub(super) write_only: bool,
    /// Number of shards in the cluster.
    pub(super) shards: usize,
    /// Which tables are sharded and using which columns.
    pub(super) sharding_schema: ShardingSchema,
    /// Context created by the router.
    pub(super) router_context: RouterContext<'a>,
    /// How aggressively we want to send reads to replicas.
    pub(super) rw_strategy: &'a ReadWriteStrategy,
    /// Do we need the router at all? Shortcut to bypass this for unsharded
    /// clusters with databases that only read or write.
    pub(super) router_needed: bool,
    /// Are we running multi-tenant checks?
    pub(super) multi_tenant: &'a Option<MultiTenant>,
    /// Dry run enabled?
    pub(super) dry_run: bool,
    /// Expanded EXPLAIN annotations enabled?
    pub(super) expanded_explain: bool,
    /// How to handle sharding-key updates.
    pub(super) shard_key_update_mode: RewriteMode,
}

impl<'a> QueryParserContext<'a> {
    /// Create query parser context from router context.
    pub fn new(router_context: RouterContext<'a>) -> Self {
        Self {
            read_only: router_context.cluster.read_only(),
            write_only: router_context.cluster.write_only(),
            shards: router_context.cluster.shards().len(),
            sharding_schema: router_context.cluster.sharding_schema(),
            rw_strategy: router_context.cluster.read_write_strategy(),
            router_needed: router_context.cluster.router_needed(),
            multi_tenant: router_context.cluster.multi_tenant(),
            dry_run: router_context.cluster.dry_run(),
            expanded_explain: router_context.cluster.expanded_explain(),
            shard_key_update_mode: router_context.cluster.rewrite().shard_key,
            router_context,
        }
    }

    /// Write override enabled?
    pub(super) fn write_override(&self) -> bool {
        matches!(
            self.router_context.transaction(),
            Some(TransactionType::ReadWrite)
        ) && self.rw_conservative()
    }

    /// Are we using the conservative read/write separation strategy?
    pub(super) fn rw_conservative(&self) -> bool {
        self.rw_strategy == &ReadWriteStrategy::Conservative
    }

    /// We need to parse queries using pg_query.
    ///
    /// Shortcut to avoid the overhead if we can.
    pub(super) fn use_parser(&self) -> bool {
        self.router_context.cluster.use_query_parser()
    }

    /// Get the query we're parsing, if any.
    pub(super) fn query(&self) -> Result<&BufferedQuery, Error> {
        self.router_context.query.as_ref().ok_or(Error::EmptyQuery)
    }

    /// Multi-tenant checks.
    pub(super) fn multi_tenant(&self) -> &Option<MultiTenant> {
        self.multi_tenant
    }

    /// Create plugin context.
    pub(super) fn plugin_context(
        &self,
        ast: &ParseResult,
        bind: &Option<&Bind>,
    ) -> PdRouterContext {
        let params = if let Some(bind) = bind {
            PdParameters {
                params: bind.params_raw().as_ptr() as *mut c_void,
                num_params: bind.params_raw().len() as u64,
                format_codes: bind.format_codes_raw().as_ptr() as *mut c_void,
                num_format_codes: bind.format_codes_raw().len() as u64,
            }
        } else {
            PdParameters::default()
        };
        PdRouterContext {
            shards: self.shards as u64,
            has_replicas: if self.read_only { 0 } else { 1 },
            has_primary: if self.write_only { 0 } else { 1 },
            in_transaction: if self.router_context.in_transaction() {
                1
            } else {
                0
            },
            // SAFETY: ParseResult lives for the entire time the plugin is executed.
            // We could use lifetimes to guarantee this, but bindgen doesn't generate them.
            query: unsafe { PdStatement::from_proto(ast) },
            write_override: 0, // This is set inside `QueryParser::plugins`.
            params,
        }
    }

    pub(super) fn expanded_explain(&self) -> bool {
        self.expanded_explain
    }

    pub(super) fn shard_key_update_mode(&self) -> RewriteMode {
        self.shard_key_update_mode
    }
}
