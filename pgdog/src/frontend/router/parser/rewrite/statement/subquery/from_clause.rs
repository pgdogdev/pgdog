use pg_raw_parse::Node;

use crate::frontend::router::parser::rewrite::statement::{
    insert::Subquery, subquery::SubqueryRewrite,
};

impl<'a, 'mem> SubqueryRewrite<'a, 'mem> {
    pub(crate) fn handle_subquery_in_from_clause(&self) -> Option<Subquery> {
        parse_from_clause(self.outer_stmt)
            .is_some()
            .then_some(Subquery::FromClause)
    }
}

pub fn parse_from_clause<'a>(outer_stmt: Node<'a>) -> Option<Node<'a>> {
    let from_clause = match outer_stmt {
        Node::UpdateStmt(update) => update.from_clause(),
        Node::SelectStmt(select) => select.from_clause(),
        _ => return None,
    };

    let from_clause = from_clause.first()?;

    match from_clause {
        Node::RangeSubselect(range_subselect) => {
            let subquery = range_subselect.subquery();
            if range_subselect.alias().is_some()
                && let Node::SelectStmt(inner_subselect) = subquery
            {
                // TODO(jkaczman): Redundant with INSERT SELECT

                // We don't want to recurse back through for a re-written inner query.
                if let Some(Node::RangeFunction(range_func)) = inner_subselect.from_clause().first()
                    && let Some(Node::NodeList(node_list)) = range_func.functions().first()
                    && let Some(Node::FuncCall(func_call)) = node_list.first()
                    && let Some(func_name) = func_call.funcname().first()
                    && let Some(func_name) = func_name.as_str()
                    && func_name.eq("unnest")
                {
                    return None;
                }

                // If VALUES is specified, no need for us to re-write.
                if !inner_subselect.values_lists().is_empty() {
                    return None;
                }

                // There's no `is_sharded` check here, because we could have
                // a case where we need to execute a subquery that's NOT sharded,
                // such as generate_series, which would resolve into a
                // `ShardingKeyUpdate`, requiring coordination here to
                // (1) resolve the actual VALUES and (2) split apart to he
                // relevant shards to ensure that `ShardingKeyUpdate` is executed
                // as intended.
                //
                // TODO(jkaczman): The above case does not work yet. ShardingKeyUpdate
                // does not work as-is for this; this will require actual changes
                // to that multi-step machinery to handle those cases. However,
                // subqueries correctly fetches the rows, so we have that information already.

                return Some(Node::SelectStmt(inner_subselect));
            }
        }

        _ => return None,
    }

    None
}
