use pg_raw_parse::{Node, deparse, nodes::SubLink};

use crate::frontend::router::parser::{
    StatementParser, StatementRewrite,
    rewrite::statement::{RewritePlan, insert::Subquery},
};

impl<'a> StatementRewrite<'a> {
    pub(crate) fn handle_subquery_in_where(&mut self, sub_link: &SubLink, plan: &mut RewritePlan) {
        let subselect = sub_link.subselect();

        // Is it sharded? if not, doesn't need to be separate (strictly for WHERE).
        // Note: this doesn't apply when we're dealing with INSERTs / etc, we'll
        // still want to evaluate not sharded things for stuff like INSERT split.
        //
        // TODO(jkaczman): We don't need to separately evaluate subqueries that are
        // sharded the exact same as the outer query. There's no point; it'd be evaluated the same.
        let mut parser = StatementParser::new(subselect, None, self.schema);
        let is_subselect_sharded = parser.is_sharded(self.db_schema, self.user, self.search_path);

        if is_subselect_sharded {
            let subselect_sql = deparse(subselect).unwrap();
            let subselect_sql = subselect_sql.as_str().to_string();

            plan.subquery = Some(Subquery {
                inner_stmt: subselect_sql,
            });
        }
    }
}

/// When we have a SELECT / UPDATE / DELETE, is there a SubLink present in its
/// WhereClause? If there is, return it.
///
/// This is re-used by the multi_step subquery code (that's why it's not in the StatementRewrite impl)
///
/// TODO(jkaczman): It may be better to walk this instead, although I'm not sure what that performance tradeoff
/// looks like. However, we might miss something otherwise. Perhaps we could make a tradeoff where
/// we try to cover everything in our test suites, and add a test-only code block that
/// runs the walk, and if it catches something, and we fall into the None branch, it panics.
pub(crate) fn has_sub_link_in_where<'a>(stmt: Node<'a>) -> Option<(&'a SubLink, Node<'a>)> {
    let where_clause = match stmt {
        Node::SelectStmt(stmt) => stmt.where_clause(),
        Node::UpdateStmt(stmt) => stmt.where_clause(),
        Node::DeleteStmt(stmt) => stmt.where_clause(),
        _ => return None,
    };

    if let Node::SubLink(sublink) = where_clause {
        Some((sublink, sublink.testexpr()))
    } else if let Node::BoolExpr(bool_expr) = where_clause
        && let Some(Node::SubLink(sublink)) = bool_expr.args().first()
    {
        Some((sublink, sublink.testexpr()))
    } else if let Node::A_Expr(a_expr) = where_clause
        && let Node::SubLink(sublink) = a_expr.rexpr()
    {
        Some((sublink, a_expr.lexpr()))
    } else {
        None
    }
}
