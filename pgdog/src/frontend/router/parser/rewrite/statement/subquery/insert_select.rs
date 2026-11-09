use pg_raw_parse::Node;

use crate::frontend::router::parser::{
    StatementParser,
    rewrite::statement::{insert::Subquery, subquery::SubqueryRewrite},
};

impl<'a, 'mem> SubqueryRewrite<'a, 'mem> {
    pub(crate) fn handle_subquery_in_insert_select(&self) -> Option<Subquery> {
        if let Node::InsertStmt(insert_stmt) = self.outer_stmt
            && parse_insert_select(self.outer_stmt).is_some()
        {
            // Is it inserting into a sharded table?
            let mut parser = StatementParser::new(
                Node::InsertStmt(insert_stmt),
                None,
                self.statement_rewrite.schema,
            );

            let is_insert_sharded = parser.is_sharded(
                self.statement_rewrite.db_schema,
                self.statement_rewrite.user,
                self.statement_rewrite.search_path,
            );

            if is_insert_sharded {
                // We're selecting data from something into a sharded table.
                // Even if it's a function like generate_series, we need to evaluate it.
                return Some(Subquery::InsertSelect);
            }
        }

        None
    }
}

pub(crate) fn parse_insert_select<'a>(entire_ast: Node<'a>) -> Option<Node<'a>> {
    if let Node::InsertStmt(insert_stmt) = entire_ast
        && let Node::SelectStmt(select) = insert_stmt.select_stmt()
    // This won't be a SubLink node as the select statement is naturally apart of the insert's AST.
    {
        // We don't want to recurse back through for a re-written query.
        // TODO(jkaczman): this is a redundant check that we use in multiple places; could
        // put this in a separate helper method in a util mod
        if let Some(Node::RangeFunction(range_func)) = select.from_clause().first()
            && let Some(Node::NodeList(node_list)) = range_func.functions().first()
            && let Some(Node::FuncCall(func_call)) = node_list.first()
            && let Some(funcname) = func_call.funcname().first()
            && let Some(funcname) = funcname.as_str()
            && funcname.eq("unnest")
        {
            return None;
        }

        if !select.values_lists().is_empty() {
            return None;
        }

        return Some(insert_stmt.select_stmt());
    }

    None
}
