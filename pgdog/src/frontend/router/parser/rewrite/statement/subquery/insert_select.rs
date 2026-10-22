use pg_raw_parse::{Node, deparse};

use crate::frontend::router::parser::{
    StatementParser,
    rewrite::statement::{insert::Subquery, subquery::SubqueryRewrite},
};

impl<'a, 'mem> SubqueryRewrite<'a, 'mem> {
    pub(crate) fn handle_subquery_in_insert_select(&self) -> Option<Subquery> {
        if let Node::InsertStmt(insert_stmt) = self.outer_stmt
            && let Node::SelectStmt(select) = insert_stmt.select_stmt()
            // This won't be a SubLink node as the select statement is naturally apart of the insert's AST.
            && !select.from_clause().is_empty()
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
                return Some(Subquery {
                    inner_stmt: deparse(select).unwrap().as_str().to_string(),
                });
            }
        }

        None
    }
}
