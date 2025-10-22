//! Shortcut the parser given the cluster config.

use std::{os::raw::c_void, sync::Once};

use pgdog_plugin::pg_query::protobuf::ParseResult;
use pgdog_plugin::{PdParameters, PdRouterContext, PdStatement};

use crate::frontend::client::TransactionType;
use crate::net::Bind;
use crate::{
    backend::ShardingSchema,
    config::{config, MultiTenant, ReadWriteStrategy, RewriteMode},
    frontend::{BufferedQuery, PreparedStatements, RouterContext},
};

use tracing::warn;

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
    /// Are we re-writing prepared statements sent over the simple protocol?
    pub(super) full_prepared_statements: bool,
    /// Do we need the router at all? Shortcut to bypass this for unsharded
    /// clusters with databases that only read or write.
    pub(super) router_needed: bool,
    /// Do we have support for LISTEN/NOTIFY enabled?
    pub(super) pub_sub_enabled: bool,
    /// Are we running multi-tenant checks?
    pub(super) multi_tenant: &'a Option<MultiTenant>,
    /// Dry run enabled?
    pub(super) dry_run: bool,
    /// Expanded EXPLAIN annotations enabled?
    pub(super) expanded_explain: bool,
    /// Rewrite features toggled on/off globally.
    pub(super) rewrite_enabled: bool,
    /// How to handle sharding-key updates.
    pub(super) shard_key_update_mode: RewriteMode,
    /// How to handle multi-row INSERTs into sharded tables.
    pub(super) split_insert_mode: RewriteMode,
}

static SHARD_KEY_REWRITE_WARNING: Once = Once::new();

impl<'a> QueryParserContext<'a> {
    /// Create query parser context from router context.
    pub fn new(router_context: RouterContext<'a>) -> Self {
        let config = config();
        let rewrite_cfg = &config.config.rewrite;
        if rewrite_cfg.shard_key == RewriteMode::Rewrite
            && rewrite_cfg.enabled
            && !config.config.general.two_phase_commit
        {
            SHARD_KEY_REWRITE_WARNING.call_once(|| {
                warn!(
                    "rewrite.shard_key=rewrite will apply non-atomic shard-key rewrites; enabling two_phase_commit is strongly recommended"
                );
            });
        }
        Self {
            read_only: router_context.cluster.read_only(),
            write_only: router_context.cluster.write_only(),
            shards: router_context.cluster.shards().len(),
            sharding_schema: router_context.cluster.sharding_schema(),
            rw_strategy: router_context.cluster.read_write_strategy(),
            full_prepared_statements: config.prepared_statements_full(),
            router_needed: router_context.cluster.router_needed(),
            pub_sub_enabled: config.config.general.pub_sub_enabled(),
            multi_tenant: router_context.cluster.multi_tenant(),
            dry_run: config.config.general.dry_run,
            expanded_explain: config.config.general.expanded_explain,
            rewrite_enabled: rewrite_cfg.enabled,
            shard_key_update_mode: rewrite_cfg.shard_key,
            split_insert_mode: rewrite_cfg.split_inserts,
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
        self.full_prepared_statements
            || self.router_needed
            || self.pub_sub_enabled
            || self.multi_tenant().is_some()
            || self.dry_run
    }

    /// Get the query we're parsing, if any.
    pub(super) fn query(&self) -> Result<&BufferedQuery, Error> {
        self.router_context.query.as_ref().ok_or(Error::EmptyQuery)
    }

    /// Mutable reference to client's prepared statements cache.
    pub(super) fn prepared_statements(&mut self) -> &mut PreparedStatements {
        self.router_context.prepared_statements
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

    pub(super) fn rewrite_enabled(&self) -> bool {
        self.rewrite_enabled
    }

    pub(super) fn split_insert_mode(&self) -> RewriteMode {
        self.split_insert_mode
    }
}
