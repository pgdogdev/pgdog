//! Handles re-writing the WHERE clause of a Select / Update / Delete statement when a subquery is present.

use pg_raw_parse::{
    ConstValue, Node, NodeMut,
    make::{MemoryToken, Unique},
    nodes::SubLink,
    raw::SubLinkType,
    transform,
};

use crate::{
    frontend::{client::query_engine::multi_step::SubqueryMulti, router::parser::rewrite},
    net::{Bind, DataRow, RowDescription},
};

impl<'a> SubqueryMulti<'a> {
    pub(crate) fn handle_where_clause<'mem>(
        &self,
        mem: MemoryToken<'mem>,
        mut statement: Unique<'mem, Node<'mem>>,
        row_description: Option<&RowDescription>,
        data_rows: &[DataRow],
        new_bind: &mut Bind,
    ) -> Unique<'mem, Node<'mem>> {
        let Some((sublink, _)) =
            rewrite::statement::subquery::where_clause::has_sub_link_in_where(statement.as_ref())
        else {
            return statement;
        };

        // Fetch the where clause.
        let mut where_clause = mem.make_unique(match statement.as_ref() {
            Node::SelectStmt(stmt) => stmt.where_clause(),
            Node::UpdateStmt(stmt) => stmt.where_clause(),
            Node::DeleteStmt(stmt) => stmt.where_clause(),
            _ => unreachable!("Parser should prevent other statements from reaching here."),
        });

        // Replace the SubLink node present with our own re-written version.
        transform::transform(&mut where_clause, |node| match &*node {
            // The WHERE clause can have multiple subqueries, e.g., WHERE id IN (SELECT user_id FROM orders) AND state IN (SELECT state FROM states), so we match the subquery that we ran by `location` (instead of matching all SubLink nodes).
            NodeMut::SubLink(candidate) if candidate.location == sublink.location => {
                let sublink_replacement = Self::rewrite_sublink_in_where_clause(
                    sublink,
                    data_rows,
                    row_description.as_ref().unwrap(),
                    mem,
                    new_bind,
                );

                node.replace(sublink_replacement);

                // We shouldn't recurse here. Even if we have nested subqueries,
                // that'll be handled later through routing through the QueryEngine again.
                None
            }
            _ => Some(node),
        });

        // Put back the modified where clause.
        match statement.as_mut() {
            NodeMut::SelectStmt(mut stmt) => stmt.set_where_clause(where_clause),
            NodeMut::UpdateStmt(mut stmt) => stmt.set_where_clause(where_clause),
            NodeMut::DeleteStmt(mut stmt) => stmt.set_where_clause(where_clause),
            _ => unreachable!("Parser should prevent other statements from reaching here."),
        };

        statement
    }

    fn rewrite_sublink_in_where_clause<'mem>(
        sublink: &SubLink,
        data_rows: &[DataRow],
        row_description: &RowDescription,
        mem: MemoryToken<'mem>,
        new_bind: &mut Bind,
    ) -> Unique<'mem, Node<'mem>> {
        if sublink.sub_link_type == SubLinkType::EXISTS_SUBLINK {
            // If there's an EXISTS clause within the WHERE, we can replace the
            // ENTIRE subquery with the boolean result
            return mem
                .make_a_const(ConstValue::Boolean(!data_rows.is_empty()))
                .uncast();
        }

        let unnest_select =
            Self::new_select_with_unnest(new_bind, mem, Node::None, data_rows, row_description);

        let mut rewritten_sublink = mem.make_unique(sublink);
        rewritten_sublink
            .as_mut()
            .set_subselect(unnest_select.uncast());

        rewritten_sublink.uncast()
    }
}
