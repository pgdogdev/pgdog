//! Shortcut the parser given the cluster config.

use crate::{
    backend::ShardingSchema,
    config::{config, MultiTenant, ReadWriteStrategy},
    frontend::{buffer::BufferedQuery, PreparedStatements, RouterContext},
};

use super::Error;

pub struct QueryParserContext<'a> {
    pub(super) read_only: bool,
    pub(super) write_only: bool,
    pub(super) shards: usize,
    pub(super) sharding_schema: ShardingSchema,
    pub(super) router_context: RouterContext<'a>,
    pub(super) rw_strategy: &'a ReadWriteStrategy,
    pub(super) full_prepared_statements: bool,
    pub(super) router_needed: bool,
    pub(super) pub_sub_enabled: bool,
    pub(super) multi_tenant: &'a Option<MultiTenant>,
    pub(super) dry_run: bool,
}

impl<'a> QueryParserContext<'a> {
    pub fn new(router_context: RouterContext<'a>) -> Self {
        let config = config();
        Self {
            read_only: router_context.cluster.read_only(),
            write_only: router_context.cluster.write_only(),
            shards: router_context.cluster.shards().len(),
            sharding_schema: router_context.cluster.sharding_schema(),
            rw_strategy: router_context.cluster.read_write_strategy(),
            full_prepared_statements: config.config.general.prepared_statements.full(),
            router_needed: router_context.cluster.router_needed(),
            pub_sub_enabled: config.config.general.pub_sub_enabled(),
            multi_tenant: router_context.cluster.multi_tenant(),
            dry_run: config.config.general.dry_run,
            router_context,
        }
    }

    pub(super) fn write_override(&self) -> bool {
        self.router_context.in_transaction && self.rw_conservative()
    }

    pub(super) fn rw_conservative(&self) -> bool {
        self.rw_strategy == &ReadWriteStrategy::Conservative
    }

    /// We need to parse the query to figure out where it should go.
    pub(super) fn use_parser(&self) -> bool {
        self.full_prepared_statements || self.router_needed || self.pub_sub_enabled
    }

    pub(super) fn query(&self) -> Result<&BufferedQuery, Error> {
        self.router_context.query.as_ref().ok_or(Error::EmptyQuery)
    }

    pub(super) fn prepared_statements(&mut self) -> &mut PreparedStatements {
        self.router_context.prepared_statements
    }

    pub(super) fn multi_tenant(&self) -> &Option<MultiTenant> {
        self.multi_tenant
    }
}
