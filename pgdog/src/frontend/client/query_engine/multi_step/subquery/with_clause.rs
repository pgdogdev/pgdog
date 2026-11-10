use pg_raw_parse::{
    Node, NodeMut,
    make::{MemoryToken, Unique},
    transform,
};

use crate::{
    frontend::{
        client::query_engine::multi_step::SubqueryMulti,
        router::parser::rewrite::statement::subquery::where_clause::has_sub_link_in_where,
    },
    net::{Bind, DataRow, RowDescription},
};

impl<'a> SubqueryMulti<'a> {
    pub fn handle_where_clause_cte<'mem>(
        &self,
        alias: &String,
        mem: MemoryToken<'mem>,
        mut statement: Unique<'mem, Node<'mem>>,
        row_description: Option<&'a RowDescription>,
        data_rows: &'a [DataRow],
        new_bind: &mut Bind,
    ) -> Unique<'mem, Node<'mem>> {
        let Some((sublink, _)) = has_sub_link_in_where(statement.as_ref()) else {
            return statement;
        };

        let rewritten_subselect = self.handle_with_clause(
            alias,
            mem,
            mem.make_unique(sublink.subselect()),
            row_description,
            data_rows,
            new_bind,
        );

        let mut rewritten_sublink = mem.make_unique(sublink);
        rewritten_sublink
            .as_mut()
            .set_subselect(rewritten_subselect);

        let mut rewritten_sublink = Some(rewritten_sublink.uncast());

        let mut where_clause = mem.make_unique(match statement.as_ref() {
            Node::SelectStmt(stmt) => stmt.where_clause(),
            Node::UpdateStmt(stmt) => stmt.where_clause(),
            Node::DeleteStmt(stmt) => stmt.where_clause(),
            _ => unreachable!("Parser should prevent other statements from reaching here."),
        });

        transform::transform(&mut where_clause, |node| match &*node {
            NodeMut::SubLink(_) => {
                if let Some(rewritten_sublink) = rewritten_sublink.take() {
                    node.replace(rewritten_sublink);
                }

                None
            }

            _ => Some(node),
        });

        match statement.as_mut() {
            NodeMut::SelectStmt(mut stmt) => stmt.set_where_clause(where_clause),
            NodeMut::UpdateStmt(mut stmt) => stmt.set_where_clause(where_clause),
            NodeMut::DeleteStmt(mut stmt) => stmt.set_where_clause(where_clause),
            _ => unreachable!("Parser should prevent other statements from reaching here."),
        };

        statement
    }

    pub fn handle_with_clause<'mem>(
        &self,
        alias: &String,
        mem: MemoryToken<'mem>,
        mut statement: Unique<'mem, Node<'mem>>,
        row_description: Option<&'a RowDescription>,
        data_rows: &'a [DataRow],
        new_bind: &mut Bind,
    ) -> Unique<'mem, Node<'mem>> {
        let Some(row_description) = row_description else {
            return statement;
        };

        let with_clause = match statement.as_ref() {
            Node::SelectStmt(select) => select.with_clause(),
            Node::UpdateStmt(update) => update.with_clause(),
            _ => unreachable!("Parser wouldn't have let through otherwise"),
        };

        let Some(with_clause) = with_clause else {
            return statement;
        };

        // TODO(jkaczman): create shared structure between derived tables
        // and CTEs as both are defining the columns explicitly in the alias,
        // as well as re-writing to use explicit VALUES

        let mut new_with_clause = mem.make_unique(with_clause);

        let mut new_cte_list = Vec::new();
        for cte in with_clause.ctes() {
            if let Some(name) = cte.ctename()
                && !name.eq(alias)
            {
                // If we're not modifying this CTE, we still want it in our new CTE list
                // Especially in the same order.
                new_cte_list.push(mem.make_unique(cte));

                continue;
            }

            // TODO: This should be based on the list that's already
            // fed to us. Although, what's the argument for parsing all
            // these beforehand vs. just knowing we have a CTE?
            //
            // Any memory implications from storing it? Is it good
            // as it's reusable? What about the length of the strings?
            let new_cte_query = Self::new_select_with_unnest(
                new_bind,
                mem,
                cte.ctequery(),
                data_rows,
                row_description,
            );

            let mut alias_col_names = mem.make_unique(cte.aliascolnames());
            if alias_col_names.is_empty() {
                // If no explicit columns are provided in the alias, then we added them
                // to correctly resolve against alias columns referenced in the outer query.
                for i in 0..row_description.len() {
                    alias_col_names.as_mut().push(
                        mem,
                        mem.make_string(Some(row_description.fields.get(i).unwrap().name.as_str()))
                            .uncast(),
                    );
                }
            }

            let new_cte_node = mem.make_common_table_expr(
                cte.ctename().unwrap(),
                alias_col_names,
                new_cte_query.uncast(),
            );

            new_cte_list.push(new_cte_node);
        }

        new_with_clause
            .as_mut()
            .set_ctes(mem.make_list(new_cte_list.as_slice()));

        // Now we set the new WITH onto the statement
        match statement.as_mut() {
            NodeMut::SelectStmt(mut select) => select.set_with_clause(new_with_clause.as_option()),
            NodeMut::UpdateStmt(mut update) => update.set_with_clause(new_with_clause.as_option()),
            _ => unreachable!("Parser doesn't allow this."),
        }

        statement
    }
}
