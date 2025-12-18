//! AST visitor utilities for statement rewriting.

use pg_query::protobuf::ParseResult;
use pg_query::{Node, NodeEnum};

/// Count the maximum parameter number ($1, $2, etc.) in the parse result.
pub fn count_params(ast: &mut ParseResult) -> u16 {
    let mut max_param = 0i32;
    let _: Result<(), std::convert::Infallible> = visit_and_mutate_nodes(ast, |node| {
        if let Some(NodeEnum::ParamRef(param)) = &node.node {
            max_param = max_param.max(param.number);
        }
        Ok(None)
    });
    max_param.max(0) as u16
}

/// Recursively visit and potentially mutate all nodes in the AST.
/// The callback returns Ok(Some(new_node)) to replace, Ok(None) to keep, or Err to abort.
pub fn visit_and_mutate_nodes<F, E>(ast: &mut ParseResult, mut callback: F) -> Result<(), E>
where
    F: FnMut(&mut Node) -> Result<Option<Node>, E>,
{
    for stmt in &mut ast.stmts {
        if let Some(ref mut node) = stmt.stmt {
            visit_and_mutate_node(node, &mut callback)?;
        }
    }
    Ok(())
}

fn visit_and_mutate_node<F, E>(node: &mut Node, callback: &mut F) -> Result<(), E>
where
    F: FnMut(&mut Node) -> Result<Option<Node>, E>,
{
    // Try to replace this node
    if let Some(replacement) = callback(node)? {
        *node = replacement;
        return Ok(());
    }

    // Otherwise, recurse into children
    let Some(inner) = &mut node.node else {
        return Ok(());
    };

    visit_and_mutate_children(inner, callback)
}

pub fn visit_and_mutate_children<F, E>(node: &mut NodeEnum, callback: &mut F) -> Result<(), E>
where
    F: FnMut(&mut Node) -> Result<Option<Node>, E>,
{
    match node {
        NodeEnum::SelectStmt(stmt) => {
            for target in &mut stmt.target_list {
                visit_and_mutate_node(target, callback)?;
            }
            for from in &mut stmt.from_clause {
                visit_and_mutate_node(from, callback)?;
            }
            if let Some(where_clause) = &mut stmt.where_clause {
                visit_and_mutate_node(where_clause, callback)?;
            }
            if let Some(having) = &mut stmt.having_clause {
                visit_and_mutate_node(having, callback)?;
            }
            for group in &mut stmt.group_clause {
                visit_and_mutate_node(group, callback)?;
            }
            for order in &mut stmt.sort_clause {
                visit_and_mutate_node(order, callback)?;
            }
            if let Some(limit) = &mut stmt.limit_count {
                visit_and_mutate_node(limit, callback)?;
            }
            if let Some(offset) = &mut stmt.limit_offset {
                visit_and_mutate_node(offset, callback)?;
            }
            for cte in stmt.with_clause.iter_mut().flat_map(|w| &mut w.ctes) {
                visit_and_mutate_node(cte, callback)?;
            }
            for values in &mut stmt.values_lists {
                visit_and_mutate_node(values, callback)?;
            }
        }

        NodeEnum::InsertStmt(stmt) => {
            if let Some(select) = &mut stmt.select_stmt {
                visit_and_mutate_node(select, callback)?;
            }
            for returning in &mut stmt.returning_list {
                visit_and_mutate_node(returning, callback)?;
            }
            for cte in stmt.with_clause.iter_mut().flat_map(|w| &mut w.ctes) {
                visit_and_mutate_node(cte, callback)?;
            }
        }

        NodeEnum::UpdateStmt(stmt) => {
            for target in &mut stmt.target_list {
                visit_and_mutate_node(target, callback)?;
            }
            if let Some(where_clause) = &mut stmt.where_clause {
                visit_and_mutate_node(where_clause, callback)?;
            }
            for from in &mut stmt.from_clause {
                visit_and_mutate_node(from, callback)?;
            }
            for returning in &mut stmt.returning_list {
                visit_and_mutate_node(returning, callback)?;
            }
            for cte in stmt.with_clause.iter_mut().flat_map(|w| &mut w.ctes) {
                visit_and_mutate_node(cte, callback)?;
            }
        }

        NodeEnum::DeleteStmt(stmt) => {
            if let Some(where_clause) = &mut stmt.where_clause {
                visit_and_mutate_node(where_clause, callback)?;
            }
            for using in &mut stmt.using_clause {
                visit_and_mutate_node(using, callback)?;
            }
            for returning in &mut stmt.returning_list {
                visit_and_mutate_node(returning, callback)?;
            }
            for cte in stmt.with_clause.iter_mut().flat_map(|w| &mut w.ctes) {
                visit_and_mutate_node(cte, callback)?;
            }
        }

        NodeEnum::ResTarget(res) => {
            if let Some(val) = &mut res.val {
                visit_and_mutate_node(val, callback)?;
            }
        }

        NodeEnum::AExpr(expr) => {
            if let Some(lexpr) = &mut expr.lexpr {
                visit_and_mutate_node(lexpr, callback)?;
            }
            if let Some(rexpr) = &mut expr.rexpr {
                visit_and_mutate_node(rexpr, callback)?;
            }
        }

        NodeEnum::FuncCall(func) => {
            for arg in &mut func.args {
                visit_and_mutate_node(arg, callback)?;
            }
        }

        NodeEnum::TypeCast(cast) => {
            if let Some(arg) = &mut cast.arg {
                visit_and_mutate_node(arg, callback)?;
            }
        }

        NodeEnum::SubLink(sub) => {
            if let Some(subselect) = &mut sub.subselect {
                visit_and_mutate_node(subselect, callback)?;
            }
            if let Some(testexpr) = &mut sub.testexpr {
                visit_and_mutate_node(testexpr, callback)?;
            }
        }

        NodeEnum::BoolExpr(bool_expr) => {
            for arg in &mut bool_expr.args {
                visit_and_mutate_node(arg, callback)?;
            }
        }

        NodeEnum::RangeSubselect(range) => {
            if let Some(subquery) = &mut range.subquery {
                visit_and_mutate_node(subquery, callback)?;
            }
        }

        NodeEnum::JoinExpr(join) => {
            if let Some(larg) = &mut join.larg {
                visit_and_mutate_node(larg, callback)?;
            }
            if let Some(rarg) = &mut join.rarg {
                visit_and_mutate_node(rarg, callback)?;
            }
            if let Some(quals) = &mut join.quals {
                visit_and_mutate_node(quals, callback)?;
            }
        }

        NodeEnum::CommonTableExpr(cte) => {
            if let Some(query) = &mut cte.ctequery {
                visit_and_mutate_node(query, callback)?;
            }
        }

        NodeEnum::List(list) => {
            for item in &mut list.items {
                visit_and_mutate_node(item, callback)?;
            }
        }

        NodeEnum::SortBy(sort) => {
            if let Some(node) = &mut sort.node {
                visit_and_mutate_node(node, callback)?;
            }
        }

        NodeEnum::CoalesceExpr(coalesce) => {
            for arg in &mut coalesce.args {
                visit_and_mutate_node(arg, callback)?;
            }
        }

        NodeEnum::CaseExpr(case) => {
            if let Some(arg) = &mut case.arg {
                visit_and_mutate_node(arg, callback)?;
            }
            for when in &mut case.args {
                visit_and_mutate_node(when, callback)?;
            }
            if let Some(defresult) = &mut case.defresult {
                visit_and_mutate_node(defresult, callback)?;
            }
        }

        NodeEnum::CaseWhen(when) => {
            if let Some(expr) = &mut when.expr {
                visit_and_mutate_node(expr, callback)?;
            }
            if let Some(result) = &mut when.result {
                visit_and_mutate_node(result, callback)?;
            }
        }

        NodeEnum::NullTest(test) => {
            if let Some(arg) = &mut test.arg {
                visit_and_mutate_node(arg, callback)?;
            }
        }

        NodeEnum::RowExpr(row) => {
            for arg in &mut row.args {
                visit_and_mutate_node(arg, callback)?;
            }
        }

        NodeEnum::ArrayExpr(arr) => {
            for elem in &mut arr.elements {
                visit_and_mutate_node(elem, callback)?;
            }
        }

        _ => (),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_params_none() {
        let mut ast = pg_query::parse("SELECT 1").unwrap();
        assert_eq!(count_params(&mut ast.protobuf), 0);
    }

    #[test]
    fn test_count_params_single() {
        let mut ast = pg_query::parse("SELECT $1").unwrap();
        assert_eq!(count_params(&mut ast.protobuf), 1);
    }

    #[test]
    fn test_count_params_multiple() {
        let mut ast = pg_query::parse("SELECT $1, $2, $3").unwrap();
        assert_eq!(count_params(&mut ast.protobuf), 3);
    }

    #[test]
    fn test_count_params_out_of_order() {
        let mut ast = pg_query::parse("SELECT $3, $1, $5").unwrap();
        assert_eq!(count_params(&mut ast.protobuf), 5);
    }

    #[test]
    fn test_count_params_in_where() {
        let mut ast = pg_query::parse("SELECT * FROM t WHERE id = $1 AND name = $2").unwrap();
        assert_eq!(count_params(&mut ast.protobuf), 2);
    }

    #[test]
    fn test_count_params_in_subquery() {
        let mut ast =
            pg_query::parse("SELECT * FROM t WHERE id IN (SELECT id FROM s WHERE val = $1)")
                .unwrap();
        assert_eq!(count_params(&mut ast.protobuf), 1);
    }
}
