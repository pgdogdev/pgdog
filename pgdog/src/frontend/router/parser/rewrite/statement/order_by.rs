use pg_raw_parse::{Node, make, nodes};

use super::{RewritePlan, StatementRewrite, aggregate::OrderByHelperMapping};
use crate::frontend::router::parser::Column;

impl StatementRewrite<'_> {
    /// Project simple ORDER BY columns that are absent from the result so the
    /// cross-shard merge can sort by their values.
    pub(super) fn rewrite_order_by<'a>(
        &mut self,
        select: &mut nodes::SelectStmtMut<'a, '_>,
        mem: make::MemoryToken<'a>,
        plan: &mut RewritePlan,
    ) {
        if self.schema.shards == 1 || matches!(select.distinct_clause().first(), Some(Node::None)) {
            return;
        }

        let original_target_len = select.target_list().len();
        let helpers = select
            .sort_clause()
            .iter()
            .enumerate()
            .filter_map(|(order_by, sort)| {
                let Node::ColumnRef(column_ref) = sort.node() else {
                    return None;
                };
                let column = Column::try_from(column_ref).ok()?;

                if column_is_projected(select, column) {
                    return None;
                }

                Some((order_by, sort.node()))
            })
            .enumerate()
            .map(|(helper_offset, (order_by, node))| {
                let helper_column = original_target_len + helper_offset;
                let alias = format!("__pgdog_order_by_{order_by}");
                let target = mem.make_res_target(Some(&alias), mem.empty(), mem.make_unique(node));

                (
                    target,
                    OrderByHelperMapping {
                        order_by,
                        helper_column,
                    },
                )
            })
            .collect::<Vec<_>>();

        if helpers.is_empty() {
            return;
        }

        let (targets, mappings): (Vec<_>, Vec<_>) = helpers.into_iter().unzip();
        select
            .target_list_mut()
            .extend(mem, mem.make_list(&targets));

        for helper in mappings {
            plan.aggregates.add_order_by_helper(helper);
        }
        self.rewritten = true;
    }
}

fn column_is_projected(select: &nodes::SelectStmt, order_by: Column<'_>) -> bool {
    select.target_list().iter().any(|target| {
        if target.name() == Some(order_by.name) {
            return true;
        }

        let Node::ColumnRef(projected_ref) = target.val() else {
            return false;
        };
        let Ok(projected) = Column::try_from(projected_ref) else {
            return column_ref_is_matching_star(target.val(), order_by);
        };

        projected.name == order_by.name
            && (order_by.table.is_none()
                || (projected.table == order_by.table && projected.schema == order_by.schema))
    })
}

fn column_ref_is_matching_star(node: Node<'_>, order_by: Column<'_>) -> bool {
    let Node::ColumnRef(column_ref) = node else {
        return false;
    };
    let fields = column_ref.fields();
    if !matches!(fields.iter().next_back(), Some(Node::A_Star(_))) {
        return false;
    }

    let qualifiers = fields
        .iter()
        .take(fields.len().saturating_sub(1))
        .filter_map(Node::as_str)
        .collect::<Vec<_>>();

    match qualifiers.as_slice() {
        [] => true,
        [table] => order_by.table == Some(*table),
        [schema, table] => order_by.schema == Some(*schema) && order_by.table == Some(*table),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{ShardingSchema, schema::Schema};
    use crate::frontend::PreparedStatements;
    use crate::frontend::router::parser::rewrite::statement::StatementRewriteContext;

    fn rewrite(sql: &str) -> (String, RewritePlan) {
        let ast = pg_raw_parse::parse(sql).unwrap();
        let schema = ShardingSchema {
            shards: 2,
            ..Default::default()
        };
        let db_schema = Schema::default();
        let mut prepared = PreparedStatements::default();
        let mut rewriter = StatementRewrite::new(StatementRewriteContext {
            extended: false,
            prepared: false,
            prepared_statements: &mut prepared,
            schema: &schema,
            db_schema: &db_schema,
            user: "postgres",
            search_path: None,
        });
        let mut plan = RewritePlan::default();
        let ast = make::owned(|mem| {
            let mut ast = mem.make_unique(&*ast.into_inner());
            plan = rewriter
                .maybe_rewrite(ast.as_mut().into_iter().next().unwrap(), mem)
                .unwrap();
            ast
        });

        (pg_raw_parse::deparse_stmts(&*ast).unwrap(), plan)
    }

    #[test]
    fn adds_non_projected_order_by_column() {
        let (sql, plan) = rewrite("SELECT id FROM users ORDER BY name");

        assert_eq!(
            sql,
            "SELECT id, name AS __pgdog_order_by_0 FROM users ORDER BY name"
        );
        assert_eq!(plan.aggregates.drop_columns().collect::<Vec<_>>(), [1]);
        assert_eq!(plan.aggregates.order_by_helpers().len(), 1);
        assert_eq!(plan.aggregates.order_by_helpers()[0].order_by, 0);
        assert_eq!(plan.aggregates.order_by_helpers()[0].helper_column, 1);
    }

    #[test]
    fn leaves_projected_order_by_columns_unchanged() {
        for sql in [
            "SELECT id, name FROM users ORDER BY name",
            "SELECT id AS name FROM users ORDER BY name",
            "SELECT * FROM users ORDER BY name",
            "SELECT users.* FROM users ORDER BY users.name",
        ] {
            let (_, plan) = rewrite(sql);
            assert!(plan.aggregates.order_by_helpers().is_empty(), "{sql}");
        }
    }

    #[test]
    fn tracks_multiple_helpers_by_order_position() {
        let (_, plan) = rewrite("SELECT id FROM users ORDER BY id, name DESC, email");
        let helpers = plan.aggregates.order_by_helpers();

        assert_eq!(helpers.len(), 2);
        assert_eq!(helpers[0].order_by, 1);
        assert_eq!(helpers[0].helper_column, 1);
        assert_eq!(helpers[1].order_by, 2);
        assert_eq!(helpers[1].helper_column, 2);
    }

    #[test]
    fn plain_distinct_keeps_postgres_validation() {
        let (_, plan) = rewrite("SELECT DISTINCT id FROM users ORDER BY name");
        assert!(plan.aggregates.order_by_helpers().is_empty());
    }
}
