//! Shortcut the parser given the cluster config.

use crate::{backend::ShardingSchema, frontend::RouterContext};

pub struct QueryParserContext<'a> {
    pub(super) read_only: bool,
    pub(super) write_only: bool,
    pub(super) shards: usize,
    pub(super) sharding_schema: ShardingSchema,
    pub(super) router_context: RouterContext<'a>,
}

impl<'a> QueryParserContext<'a> {
    pub fn new(router_context: RouterContext<'a>) -> Self {
        Self {
            read_only: router_context.cluster.read_only(),
            write_only: router_context.cluster.write_only(),
            shards: router_context.cluster.shards().len(),
            sharding_schema: router_context.cluster.sharding_schema(),
            router_context,
        }
    }
}
