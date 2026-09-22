use pg_raw_parse::{ConstValue, Node, make, nodes};

use crate::frontend::router::parser::Column;

use super::projection::{OrderByHelper, OrderBySource, ProjectionRewritePlan};

/// Compare complete column references so `a.price` and `b.price` remain distinct.
fn same_column(left: &nodes::ColumnRef, right: &nodes::ColumnRef) -> bool {
    left.fields()
        .into_iter()
        .map(Node::as_str)
        .eq(right.fields().into_iter().map(Node::as_str))
}

/// An unqualified `*` covers every column. A qualified star only covers an
/// equally-qualified ORDER BY reference; mixed qualification is handled
/// conservatively by projecting a helper and resolving its alias at runtime.
fn star_covers(star: &nodes::ColumnRef, column: &nodes::ColumnRef) -> bool {
    let star_fields = star.fields();
    let column_fields = column.fields();
    let star_len = star_fields.len();

    matches!(star_fields.into_iter().next_back(), Some(Node::A_Star(_)))
        && (star_len == 1
            || (star_len == column_fields.len()
                && star
                    .fields()
                    .into_iter()
                    .take(star_len - 1)
                    .map(Node::as_str)
                    .eq(column_fields
                        .into_iter()
                        .take(star_len - 1)
                        .map(Node::as_str))))
}

/// Whether the SELECT list already returns the value needed for this sort.
fn projects_column(select: &nodes::SelectStmtMut<'_, '_>, column: &nodes::ColumnRef) -> bool {
    let fields = column.fields();
    let unqualified = fields.len() == 1;
    let name = fields.into_iter().next_back().and_then(Node::as_str);

    select.target_list().iter().any(|target| {
        (unqualified && target.name() == name)
            || matches!(target.val(), Node::ColumnRef(projected)
                if star_covers(projected, column)
                    || same_column(projected, column)
                    || (unqualified
                        && projected.fields().into_iter().next_back().and_then(Node::as_str)
                            == name))
    })
}

/// Project missing sort values so PgDog can merge shard results. Helpers use
/// aliases rather than AST target positions because `*` expands only in Postgres.
pub(super) fn rewrite_select<'a>(
    select: &mut nodes::SelectStmtMut<'a, '_>,
    mem: make::MemoryToken<'a>,
    plan: &mut ProjectionRewritePlan,
) {
    let mut helpers = Vec::new();
    let mut sort_position = 0;
    for sort in select.sort_clause() {
        let node = sort.node();
        let source = match node {
            Node::ColumnRef(column) => column
                .fields()
                .into_iter()
                .next_back()
                .and_then(Node::as_str)
                .map(|name| OrderBySource::Column(name.to_owned())),
            Node::A_Expr(expr)
                if expr.name().iter().next().and_then(Node::as_str) == Some("<->") =>
            {
                [expr.lexpr(), expr.rexpr()]
                    .into_iter()
                    .find_map(|node| Column::try_from(node).ok())
                    .map(|column| OrderBySource::Vector(column.name.to_owned()))
            }
            _ => None,
        };
        let supported = source.is_some()
            || matches!(node, Node::A_Const(constant)
                if matches!(constant.val(), Some(ConstValue::Integer(_))));
        if !supported {
            continue;
        }

        let current_sort_position = sort_position;
        sort_position += 1;

        let needs_helper = match node {
            Node::ColumnRef(column) => !projects_column(select, column),
            Node::A_Expr(_) => source.is_some(),
            _ => false,
        };
        if !needs_helper {
            continue;
        }

        let alias = format!("__pgdog_order_col{current_sort_position}");
        helpers.push(mem.make_res_target(
            Some(&alias),
            mem.empty(),
            mem.make_unique(node).uncast(),
        ));
        plan.order_by_helpers.push(OrderByHelper {
            sort_position: current_sort_position,
            source: source.expect("only columns and vector expressions need helpers"),
            alias,
        });
    }

    if !helpers.is_empty() {
        select
            .target_list_mut()
            .extend(mem, mem.make_list(&helpers));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_raw_parse::{Node, make};

    fn rewrite(sql: &str) -> (String, ProjectionRewritePlan) {
        let ast = pg_raw_parse::parse(sql).unwrap();
        let mut plan = ProjectionRewritePlan::default();
        let rewritten = make::owned(|mem| {
            let Node::SelectStmt(select) = ast.stmts().next().unwrap() else {
                panic!("expected SELECT");
            };
            let mut select = mem.make_unique(select);
            rewrite_select(&mut select.as_mut(), mem, &mut plan);
            select
        });
        (
            pg_raw_parse::deparse(&*rewritten)
                .unwrap()
                .as_str()
                .to_owned(),
            plan,
        )
    }

    #[test]
    fn projects_missing_sort_column() {
        let (sql, plan) = rewrite("SELECT id FROM products ORDER BY price");

        assert!(sql.contains("price AS __pgdog_order_col0"));
        assert_eq!(plan.order_by_helpers.len(), 1);
        assert_eq!(plan.order_by_helpers[0].alias, "__pgdog_order_col0");
    }

    #[test]
    fn skips_already_projected_sort_column() {
        let (sql, plan) = rewrite("SELECT id, price FROM products ORDER BY price");

        assert!(!sql.contains("__pgdog_order_col"));
        assert!(plan.is_noop());
    }

    #[test]
    fn skips_star_select() {
        let (sql, plan) = rewrite("SELECT * FROM products ORDER BY id");

        assert!(!sql.contains("__pgdog_order_col"));
        assert!(plan.is_noop());
    }

    #[test]
    fn skips_qualified_star_select() {
        let (sql, plan) = rewrite("SELECT products.* FROM products ORDER BY products.id");

        assert!(!sql.contains("__pgdog_order_col"));
        assert!(plan.is_noop());
    }

    #[test]
    fn projects_column_not_covered_by_qualified_star() {
        let (sql, plan) = rewrite("SELECT a.* FROM a JOIN b ON a.id = b.a_id ORDER BY b.score");

        assert!(sql.contains("b.score AS __pgdog_order_col0"));
        assert_eq!(plan.order_by_helpers.len(), 1);
    }

    #[test]
    fn projects_unqualified_sort_for_qualified_star() {
        let (sql, plan) = rewrite("SELECT p.* FROM products p ORDER BY price");

        assert!(sql.contains("price AS __pgdog_order_col0"));
        assert_eq!(plan.order_by_helpers[0].alias, "__pgdog_order_col0");
    }

    #[test]
    fn distinguishes_same_named_columns_from_different_relations() {
        let (sql, plan) =
            rewrite("SELECT a.price FROM a JOIN b ON a.id = b.a_id ORDER BY a.price, b.price");

        assert!(sql.contains("b.price AS __pgdog_order_col1"));
        assert_eq!(plan.order_by_helpers.len(), 1);
        assert_eq!(plan.order_by_helpers[0].sort_position, 1);
    }

    #[test]
    fn projects_vector_distance_without_resolved_parameter() {
        let (sql, plan) = rewrite("SELECT id FROM products ORDER BY embedding <-> $1 LIMIT 5");

        assert!(sql.contains("embedding <-> $1"));
        assert!(sql.contains("AS __pgdog_order_col0"));
        assert_eq!(plan.order_by_helpers.len(), 1);
    }
}
