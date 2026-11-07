use pg_raw_parse::{Node, deparse};

use crate::frontend::router::parser::rewrite::statement::{
    insert::{Subquery, SubqueryType},
    subquery::SubqueryRewrite,
};

impl<'a, 'mem> SubqueryRewrite<'a, 'mem> {
    pub(crate) fn handle_subquery_in_from_clause(&self) -> Option<Subquery> {
        let from_clause = match self.outer_stmt {
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
                    && (!inner_subselect.from_clause().is_empty()
                        || !matches!(inner_subselect.where_clause(), Node::None))
                {
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

                    let subquery_sql = deparse(subquery).unwrap();
                    let subquery_sql = subquery_sql.as_str();

                    return Some(Subquery {
                        inner_stmt: subquery_sql.to_owned(),
                        identifier: SubqueryType::FromClause,
                    });
                }
            }
            _ => return None,
        }

        None
    }
}
