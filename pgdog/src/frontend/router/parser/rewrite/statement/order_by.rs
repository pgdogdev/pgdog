use pg_raw_parse::{ConstValue, Node, make, nodes, walk};

use crate::backend::schema::Schema;
use crate::frontend::router::parser::aggregate::AggregateFunction;
use crate::frontend::router::parser::{Column, Function, OrderBy};

use super::projection::{OrderByHelper, OrderBySource, ProjectionRewritePlan};

impl OrderBySource {
    fn name(&self) -> &str {
        match self {
            Self::Column(name) | Self::Vector(name) => name,
        }
    }
}

fn column_name(column: &nodes::ColumnRef) -> Option<&str> {
    column
        .fields()
        .into_iter()
        .next_back()
        .and_then(Node::as_str)
}

fn is_star(column: &nodes::ColumnRef) -> bool {
    matches!(
        column.fields().into_iter().next_back(),
        Some(Node::A_Star(_))
    )
}

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

fn target_output_name(target: &nodes::ResTarget) -> Option<&str> {
    if let Some(alias) = target.name() {
        return Some(alias);
    }
    match target.val() {
        Node::ColumnRef(column) if !is_star(column) => column_name(column),
        _ => None,
    }
}

fn single_relation(select: &nodes::SelectStmtMut<'_, '_>) -> bool {
    let from = select.from_clause();
    from.len() == 1 && matches!(from.first(), Some(Node::RangeVar(_)))
}

fn explicit_output_name(
    target: &nodes::ResTarget,
    column: &nodes::ColumnRef,
    name: &str,
    unqualified: bool,
) -> Option<String> {
    let Node::ColumnRef(projected) = target.val() else {
        return None;
    };
    if is_star(projected) {
        return None;
    }
    if same_column(projected, column) || (unqualified && column_name(projected) == Some(name)) {
        Some(target.name().unwrap_or(name).to_owned())
    } else {
        None
    }
}

/// Cross-shard sort looks up a RowDescription name. Reuse a projected column
/// only when that name is unique; stars and duplicate names get a helper.
fn unique_output_name(
    select: &nodes::SelectStmtMut<'_, '_>,
    column: &nodes::ColumnRef,
) -> Option<String> {
    let name = column_name(column)?;
    let unqualified = column.fields().len() == 1;

    if unqualified && select.target_list().iter().any(|t| t.name() == Some(name)) {
        return Some(name.to_owned());
    }

    let mut star_match = false;
    let mut output = None;
    for target in select.target_list() {
        if let Some(found) = explicit_output_name(target, column, name, unqualified) {
            output = Some(found);
            star_match = false;
            break;
        }
        if matches!(target.val(), Node::ColumnRef(projected) if star_covers(projected, column)) {
            star_match = true;
        }
    }
    let output = output.or_else(|| star_match.then(|| name.to_owned()))?;

    let mut sources = 0;
    let mut unqualified_star = false;
    for target in select.target_list() {
        if let Node::ColumnRef(projected) = target.val()
            && is_star(projected)
        {
            sources += 1;
            if projected.fields().len() == 1 {
                unqualified_star = true;
            }
        } else if target_output_name(target) == Some(output.as_str()) {
            sources += 1;
        }
    }

    if sources == 1 && (!unqualified_star || single_relation(select)) {
        Some(output)
    } else {
        None
    }
}

fn push_helper(
    plan: &mut ProjectionRewritePlan,
    sort_position: usize,
    source: OrderBySource,
    alias: String,
    injected: bool,
) {
    plan.order_by_helpers.push(OrderByHelper {
        sort_position,
        source,
        alias,
        injected,
    });
}

pub(super) fn is_computed_sort_key(sort_key: Node) -> bool {
    match sort_key {
        Node::ColumnRef(_) => false,
        Node::A_Const(sort_constant) => {
            !matches!(sort_constant.val(), Some(ConstValue::Integer(_)))
        }
        Node::A_Expr(sort_expression) => {
            sort_expression.name().iter().next().and_then(Node::as_str) != Some("<->")
        }
        _ => true,
    }
}

pub(super) fn contains_aggregate(sort_key: Node, schema: &Schema) -> bool {
    let mut found_aggregate = false;

    walk::walk(sort_key, |sort_key_node| {
        found_aggregate |= Function::try_from(sort_key_node).is_ok_and(|sort_key_function| {
            AggregateFunction::from_name(sort_key_function.name, schema).is_some()
        });
    });

    found_aggregate
}

/// Project missing or ambiguous sort values so PgDog can merge shard results.
/// Helpers use aliases rather than AST target positions because `*` expands
/// only in Postgres.
pub(super) fn rewrite_select<'a>(
    select: &mut nodes::SelectStmtMut<'a, '_>,
    mem: make::MemoryToken<'a>,
    plan: &mut ProjectionRewritePlan,
) {
    let mut helpers = Vec::new();
    for (current_sort_position, sort) in select.sort_clause().into_iter().enumerate() {
        let node = sort.node();
        let source = match node {
            Node::ColumnRef(column) => {
                column_name(column).map(|name| OrderBySource::Column(name.to_owned()))
            }
            Node::A_Expr(expr)
                if expr.name().iter().next().and_then(Node::as_str) == Some("<->") =>
            {
                [expr.lexpr(), expr.rexpr()]
                    .into_iter()
                    .find_map(|node| Column::try_from(node).ok())
                    .map(|column| OrderBySource::Vector(column.name.to_owned()))
            }
            sort_key if is_computed_sort_key(sort_key) => Some(OrderBySource::Column(
                OrderBy::helper_column_name(current_sort_position),
            )),
            _ => None,
        };
        let supported = source.is_some()
            || matches!(node, Node::A_Const(constant)
                if matches!(constant.val(), Some(ConstValue::Integer(_))));
        if !supported {
            continue;
        }

        match node {
            Node::ColumnRef(column) => match unique_output_name(select, column) {
                Some(name) if source.as_ref().is_some_and(|source| source.name() == name) => {}
                Some(name) => push_helper(
                    plan,
                    current_sort_position,
                    source.expect("column sorts always have a source"),
                    name,
                    false,
                ),
                None => {
                    let alias = OrderBy::helper_column_name(current_sort_position);
                    helpers.push(mem.make_res_target(
                        Some(&alias),
                        mem.empty(),
                        mem.make_unique(node).uncast(),
                    ));
                    push_helper(
                        plan,
                        current_sort_position,
                        source.expect("column sorts always have a source"),
                        alias,
                        true,
                    );
                }
            },
            _ if source.is_some() => {
                let alias = OrderBy::helper_column_name(current_sort_position);
                helpers.push(mem.make_res_target(
                    Some(&alias),
                    mem.empty(),
                    mem.make_unique(node).uncast(),
                ));
                push_helper(
                    plan,
                    current_sort_position,
                    source.expect("expression and vector sorts always have a source"),
                    alias,
                    true,
                );
            }
            _ => {}
        }
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
    fn adds_helper_column_for_case_sort_key() {
        let (sql, plan) = rewrite(
            "SELECT id, name FROM tenant_items ORDER BY CASE name WHEN 'pay' THEN 0 ELSE 1 END, id",
        );

        assert_eq!(
            sql,
            "SELECT id, name, CASE name WHEN 'pay' THEN 0 ELSE 1 END AS __pgdog_order_col0 \
             FROM tenant_items ORDER BY CASE name WHEN 'pay' THEN 0 ELSE 1 END, id"
        );
        assert_eq!(
            plan.order_by_helpers,
            [OrderByHelper {
                sort_position: 0,
                source: OrderBySource::Column("__pgdog_order_col0".into()),
                alias: "__pgdog_order_col0".into(),
                injected: true,
            }]
        );
    }

    #[test]
    fn projects_missing_sort_column() {
        let (sql, plan) = rewrite("SELECT id FROM products ORDER BY price");

        assert!(sql.contains("price AS __pgdog_order_col0"));
        assert_eq!(plan.order_by_helpers.len(), 1);
        assert_eq!(plan.order_by_helpers[0].alias, "__pgdog_order_col0");
        assert!(plan.order_by_helpers[0].injected);
    }

    #[test]
    fn skips_already_projected_sort_column() {
        let (sql, plan) = rewrite("SELECT id, price FROM products ORDER BY price");

        assert!(!sql.contains("__pgdog_order_col"));
        assert!(plan.is_noop());
    }

    #[test]
    fn remaps_aliased_projected_sort_column() {
        let (sql, plan) = rewrite("SELECT price AS item_price FROM products ORDER BY price");

        assert!(!sql.contains("__pgdog_order_col"));
        assert_eq!(plan.order_by_helpers.len(), 1);
        assert_eq!(plan.order_by_helpers[0].alias, "item_price");
        assert!(!plan.order_by_helpers[0].injected);
    }

    #[test]
    fn skips_sort_by_output_alias() {
        let (sql, plan) = rewrite("SELECT price AS item_price FROM products ORDER BY item_price");

        assert!(!sql.contains("__pgdog_order_col"));
        assert!(plan.is_noop());
    }

    #[test]
    fn skips_sort_by_output_alias_in_star_select() {
        for star in ["*", "persons.*"] {
            let (sql, plan) = rewrite(&format!(
                "SELECT {star}, length(email) AS score FROM persons ORDER BY score"
            ));

            assert!(!sql.contains("__pgdog_order_col"));
            assert!(plan.is_noop());
        }
    }

    #[test]
    fn skips_sort_by_output_alias_shadowing_input_column() {
        let (sql, plan) =
            rewrite("SELECT price AS item_price, x AS price FROM products ORDER BY price");

        assert!(!sql.contains("__pgdog_order_col"));
        assert!(plan.is_noop());
    }

    #[test]
    fn injects_helper_for_duplicate_output_names() {
        let (sql, plan) =
            rewrite("SELECT a.price, b.price FROM a JOIN b ON a.id = b.a_id ORDER BY b.price");

        assert!(sql.contains("b.price AS __pgdog_order_col0"));
        assert_eq!(plan.order_by_helpers.len(), 1);
        assert!(plan.order_by_helpers[0].injected);
    }

    #[test]
    fn remaps_unique_alias_among_same_named_columns() {
        let (sql, plan) = rewrite(
            "SELECT a.price AS a_price, b.price AS b_price FROM a JOIN b ON a.id = b.a_id ORDER BY b.price",
        );

        assert!(!sql.contains("__pgdog_order_col"));
        assert_eq!(plan.order_by_helpers.len(), 1);
        assert_eq!(plan.order_by_helpers[0].alias, "b_price");
        assert!(!plan.order_by_helpers[0].injected);
    }

    #[test]
    fn injects_helper_when_star_can_collide() {
        let (sql, plan) =
            rewrite("SELECT a.*, b.price FROM a JOIN b ON a.id = b.a_id ORDER BY b.price");

        assert!(sql.contains("b.price AS __pgdog_order_col0"));
        assert!(plan.order_by_helpers[0].injected);
    }

    #[test]
    fn injects_helper_for_unqualified_star_join() {
        let (sql, plan) = rewrite("SELECT * FROM a JOIN b ON a.id = b.a_id ORDER BY b.price");

        assert!(sql.contains("b.price AS __pgdog_order_col0"));
        assert!(plan.order_by_helpers[0].injected);
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
        assert!(plan.order_by_helpers[0].injected);
    }
}
