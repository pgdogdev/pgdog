use pg_raw_parse::{Node, make, nodes};

use crate::frontend::router::parser::OrderBy;

use super::projection::{OrderByHelper, ProjectionRewritePlan};

/// Project ORDER BY expressions that are missing from the SELECT list
/// so cross-shard results can be sorted, then stripped.
pub(super) fn rewrite_select<'a>(
    select: &mut nodes::SelectStmtMut<'a, '_>,
    mem: make::MemoryToken<'a>,
    order_by: &[OrderBy],
    plan: &mut ProjectionRewritePlan,
) {
    let mut helpers = Vec::new();
    let mut sort_position = 0;
    for sort in select.sort_clause() {
        let node = sort.node();
        let Some(order) = order_by.get(sort_position) else {
            break;
        };
        let supported = matches!(
            (node, order),
            (Node::A_Const(_), OrderBy::Asc(_) | OrderBy::Desc(_))
                | (
                    Node::ColumnRef(_),
                    OrderBy::AscColumn(_) | OrderBy::DescColumn(_)
                )
                | (Node::A_Expr(_), OrderBy::AscVectorL2Column(_, _))
        );
        if !supported {
            continue;
        }

        let needs_helper = match node {
            Node::ColumnRef(column) => {
                let Some(name) = column
                    .fields()
                    .into_iter()
                    .next_back()
                    .and_then(Node::as_str)
                else {
                    continue;
                };
                !select.target_list().iter().any(|target| {
                    target.name() == Some(name)
                        || matches!(
                            target.val(),
                            Node::ColumnRef(projected)
                                if projected.fields().into_iter().next_back().and_then(Node::as_str)
                                    == Some(name)
                        )
                })
            }
            Node::A_Expr(_) => matches!(order, OrderBy::AscVectorL2Column(_, _)),
            _ => false,
        };
        let current_sort_position = sort_position;
        sort_position += 1;
        if !needs_helper {
            continue;
        }

        let projected_column = select.target_list().len() + helpers.len();
        let alias = format!("__pgdog_order_col{current_sort_position}");
        helpers.push(mem.make_res_target(
            Some(&alias),
            mem.empty(),
            mem.make_unique(node).uncast(),
        ));
        plan.add_order_by_helper(OrderByHelper {
            sort_position: current_sort_position,
            projected_column,
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

    fn rewrite(sql: &str, order_by: Vec<OrderBy>) -> (String, ProjectionRewritePlan) {
        let ast = pg_raw_parse::parse(sql).unwrap();
        let mut plan = ProjectionRewritePlan::default();
        let rewritten = make::owned(|mem| {
            let Node::SelectStmt(select) = ast.stmts().next().unwrap() else {
                panic!("expected SELECT");
            };
            let mut select = mem.make_unique(select);
            rewrite_select(&mut select.as_mut(), mem, &order_by, &mut plan);
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
        let (sql, plan) = rewrite(
            "SELECT id FROM products ORDER BY price",
            vec![OrderBy::AscColumn("price".into())],
        );

        assert!(sql.contains("price AS __pgdog_order_col0"));
        assert_eq!(plan.order_by_helpers().len(), 1);
        assert_eq!(plan.order_by_helpers()[0].projected_column, 1);
    }

    #[test]
    fn skips_already_projected_sort_column() {
        let (sql, plan) = rewrite(
            "SELECT id, price FROM products ORDER BY price",
            vec![OrderBy::AscColumn("price".into())],
        );

        assert!(!sql.contains("__pgdog_order_col"));
        assert!(plan.is_noop());
    }
}
