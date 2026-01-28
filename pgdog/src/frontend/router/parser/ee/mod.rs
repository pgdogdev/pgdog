//! Parser hooks for the Enterprise edition.

use pgdog_config::Role;

use crate::{
    frontend::router::{
        parser::Value,
        parser::{Column, Shard},
    },
    net::Bind,
};

#[derive(Debug, Default, Clone)]
pub(crate) struct ParserHooks {}

impl ParserHooks {
    /// Record that the parser detected a sharding key from a query.
    pub(crate) fn record_sharding_key(
        &self,
        _shard: &Shard,
        _column: &Column<'_>,
        _value: &Value,
        _bind: &Option<&Bind>,
    ) {
    }

    /// Record a `SET pgdog.sharding_key` command.
    pub(crate) fn record_set_sharding_key(&self, _shard: &Shard, _value: &str) {}

    /// Record a `SET pgdog.shard` command.
    pub(crate) fn record_set_shard(&self, _shard: &Shard) {}

    /// Record a schema-based sharding route, either via `SET search_path` or `SET pgdog.sharding_key`.
    pub(crate) fn record_sharded_schema(&self, _shard: &Shard, _schema_name: &str) {}

    /// Record `SET pgdog.role` command.
    pub(crate) fn record_set_role(&self, _role: &Role) {}
}
