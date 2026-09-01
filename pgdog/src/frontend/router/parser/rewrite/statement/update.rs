use pg_raw_parse::make::owned;
use pg_raw_parse::{DeparseResult, Node, deparse, nodes};
use pgdog_config::RewriteMode;

use super::*;
use crate::frontend::router::parser::{Column, Table, Value};

impl<'a> StatementRewrite<'a> {
    /// Create a plan for sharding key updates, if we suspect there is one
    /// in the query.
    pub(super) fn sharding_key_update(
        &mut self,
        stmt: &nodes::UpdateStmt,
        plan: &mut RewritePlan,
    ) -> Result<(), Error> {
        if self.schema.shards == 1 || self.schema.rewrite.shard_key == RewriteMode::Ignore {
            return Ok(());
        }

        if self.sharding_key_update_check(stmt)? {
            // Without a WHERE clause, this is a huge
            // cross-shard rewrite.
            if let Node::None = stmt.where_clause() {
                return Err(Error::WhereClauseMissing);
            }
            plan.sharding_key_update = true;
        }

        Ok(())
    }

    /// Check if the sharding key could be updated.
    fn sharding_key_update_check(&'a self, stmt: &'a nodes::UpdateStmt) -> Result<bool, Error> {
        let table = stmt
            .relation()
            .map(Table::from)
            .expect("UPDATE always has a table");

        let Some(shard_key_assignment) = stmt.target_list().into_iter().find(|c| {
            Column::try_from(*c).is_ok_and(|mut c| {
                c.qualify(table);
                self.schema.tables().get_table(c).is_some()
            })
        }) else {
            return Ok(false);
        };

        // Check that it's a value assignment and not something like
        // id = id + 1
        if Value::try_from(shard_key_assignment.val()).is_ok() {
            Ok(true)
        } else {
            let expr = shard_key_assignment.val();
            let expr = deparse_expr([expr])?;
            // FIXME:
            //
            // We can technically support this. We can inject this into
            // the `SELECT` statement we use to pull the existing row
            // and use the computed value for assignment.
            Err(Error::UnsupportedShardingKeyUpdate(format!(
                "\"{}\" = {}",
                shard_key_assignment.name().unwrap_or_default(),
                expr.as_str().strip_prefix("SELECT ").unwrap_or("<unknown>"),
            )))
        }
    }
}

/// Deparse an expression node by wrapping it in a SELECT statement.
pub(crate) fn deparse_expr<'a>(
    nodes: impl IntoIterator<Item = Node<'a>>,
) -> Result<DeparseResult, Error> {
    let node = owned(|mem| {
        let mut select = mem.make_node::<nodes::SelectStmt>();
        let res_targets = nodes
            .into_iter()
            .map(|node| match node {
                Node::ResTarget(r) => mem.make_unique(r),
                _ => mem.make_res_target(None, mem.empty(), mem.make_unique(node)),
            })
            .collect::<Vec<_>>();
        select.as_mut().set_target_list(mem.make_list(&res_targets));
        mem.make_raw_stmt(select.uncast())
    });
    deparse(&*node).map_err(Into::into)
}
