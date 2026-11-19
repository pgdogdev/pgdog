use pg_raw_parse::{
    Node, NodeMut,
    make::{MemoryToken, Unique},
    nodes::{A_Star, Alias, RangeSubselect, RangeVar, SelectStmt},
    transform,
};

use crate::{
    frontend::{
        client::query_engine::multi_step::SubqueryMulti,
        router::parser::rewrite::statement::insert::JoinFetch,
    },
    net::{Bind, DataRow, RowDescription},
};

impl<'a> SubqueryMulti<'a> {
    /// Puts the rows that we fetched from all shards (`data_rows`) in place of the table at `join_fetch.location`
    /// E.g., for SELECT .. FROM users x JOIN orders y ON x.status = y.status, we rewrite
    /// - .. JOIN orders y ON ..
    ///
    /// to
    /// - .. JOIN (SELECT u.c1, u.c2 FROM unnest($1::pg_catalog._int8, $2::pg_catalog._text) u(c1, c2)) AS y(id, status) ON ..
    ///
    /// The column names (id, status) are the names of the columns that the shards returned for SELECT * FROM orders y
    pub fn handle_join_from<'mem>(
        &self,
        join_fetch: &JoinFetch,
        mem: MemoryToken<'mem>,
        mut statement: Unique<'mem, Node<'mem>>,
        row_description: Option<&RowDescription>,
        data_rows: &[DataRow],
        new_bind: &mut Bind,
    ) -> Unique<'mem, Node<'mem>> {
        let Some(row_description) = row_description else {
            return statement;
        };

        let new_right_side = {
            let alias = {
                let mut alias = mem.make_node::<Alias>();
                alias
                    .as_mut()
                    .set_aliasname(Some(mem.copy_string(&join_fetch.name)));
                Self::make_alias(mem, &alias, row_description.clone())
            };

            let mut new_right_side = mem.make_node::<RangeSubselect>();
            new_right_side.as_mut().set_subquery(
                Self::new_select_with_unnest(new_bind, mem, Node::None, data_rows, row_description)
                    .uncast(),
            );
            new_right_side.as_mut().set_alias(alias.as_option());
            new_right_side.uncast()
        };

        transform::transform(&mut statement, |node| match &*node {
            NodeMut::RangeVar(table) if table.location == join_fetch.location => {
                node.replace(mem.make_unique(new_right_side.as_ref()));
                None
            }
            _ => Some(node),
        });

        statement
    }

    /// Returns SELECT * FROM `table`, which we send to all shards to fetch the rows of `table`
    /// E.g., for orders y in .. JOIN orders y ON .., we return SELECT * FROM orders y
    ///
    /// TODO(jkaczman): We should only fetch the columns and rows of `table` that the statement reads
    /// E.g., for SELECT x.id FROM users x JOIN orders y ON x.status = y.status WHERE y.total > 100, we only need the status of the orders with a total over 100
    /// Currently, SELECT * fetches all columns and all rows of `table`, which is expensive for large tables
    pub(crate) fn new_select_from_table<'mem>(
        mem: MemoryToken<'mem>,
        table: &RangeVar,
    ) -> Unique<'mem, &'mem SelectStmt> {
        let target_list = {
            let star = mem
                .make_column_ref(mem.make_list(&[mem.make_node::<A_Star>().uncast()]))
                .uncast();
            mem.make_list(&[mem.make_res_target(None, mem.empty(), star)])
        };

        let from_clause = mem.make_list(&[mem.make_unique(table).uncast()]);

        let mut select = mem.make_node::<SelectStmt>();
        select.as_mut().set_target_list(target_list);
        select.as_mut().set_from_clause(from_clause);
        select
    }
}
