use std::ops::Deref;

use pg_raw_parse::{
    Node, NodeMut, deparse,
    make::{MemoryToken, Unique, owned},
    nodes::{self, Alias, SelectStmt},
    transform,
};
use pgdog_postgres_types::{DataType, Datum};

use crate::{
    frontend::client::query_engine::multi_step::{Error, SubqueryMulti},
    net::{DataRow, Decoder, RowDescription},
};

impl<'a> SubqueryMulti<'a> {
    pub fn handle_from_clause(
        &self,
        statement: Node<'a>,
        row_description: Option<&RowDescription>,
        data_rows: &[DataRow],
    ) -> Result<Option<String>, Error> {
        let Some(row_description) = row_description else {
            return Ok(None);
        };

        let new_statement = owned(|mem| {
            let owned_stmt = mem.make_unique(statement);
            let mut owned_raw_stmt = mem.make_raw_stmt(owned_stmt);
            let mutable_raw_stmt = owned_raw_stmt.as_mut();

            let new_from_clause = mem.make_unique(match mutable_raw_stmt.stmt() {
                Node::SelectStmt(stmt) => stmt.from_clause(),
                Node::UpdateStmt(stmt) => stmt.from_clause(),
                _ => unreachable!("The parser only allows through SELECT and UPDATE"),
            });

            let mut new_from_clause = mem.make_unique(new_from_clause.first().unwrap());

            transform::transform(&mut new_from_clause, |node| match &*node {
                NodeMut::RangeSubselect(range_subselect) => {
                    let mut range_subselect = mem.make_unique(range_subselect.deref());
                    // Replace the subquery in the statement with the DataRows (via VALUES)
                    {
                        let explicit_values_list = Self::new_select_with_explicit_values(
                            mem,
                            row_description.clone(),
                            data_rows,
                        );

                        range_subselect
                            .as_mut()
                            .set_subquery(explicit_values_list.uncast());
                    }

                    {
                        let existing_alias = range_subselect
                            .alias()
                            .expect("alias was checked in the parser");

                        if existing_alias.colnames().is_empty() {
                            // If no explicit columns are provided in the alias, then we added them
                            // to correctly resolve against alias columns referenced in the outer query.

                            let new_alias =
                                Self::make_alias(mem, existing_alias, row_description.clone());

                            range_subselect.as_mut().set_alias(new_alias.as_option());
                        }
                    }

                    node.replace(range_subselect.uncast());
                    None
                }
                _ => Some(node),
            });

            let new_from_clause = mem.make_list(&[new_from_clause]);

            match owned_raw_stmt.as_mut().stmt_mut() {
                NodeMut::SelectStmt(mut stmt) => stmt.set_from_clause(new_from_clause),
                NodeMut::UpdateStmt(mut stmt) => stmt.set_from_clause(new_from_clause),
                _ => unreachable!("The parser only allows through SELECT and UPDATE"),
            };

            if data_rows.is_empty() {
                // If rows are empty, now set WHERE to false to cancel out the NULL row we created
                // with `explicit_values_list` to properly return type OIDs in the RowDescription
                // when we have an empty derived table response.

                match owned_raw_stmt.as_mut().stmt_mut() {
                    NodeMut::SelectStmt(mut stmt) => stmt.set_where_clause(
                        mem.make_a_const(pg_raw_parse::ConstValue::Boolean(false))
                            .uncast(),
                    ),
                    NodeMut::UpdateStmt(mut stmt) => stmt.set_where_clause(
                        mem.make_a_const(pg_raw_parse::ConstValue::Boolean(false))
                            .uncast(),
                    ),
                    _ => unreachable!("The parser only allows through SELECT and UPDATE"),
                }
            }

            owned_raw_stmt
        });

        let new_sql = {
            let new_sql = deparse(new_statement.deref()).unwrap();
            let new_sql = new_sql.as_str();

            new_sql.to_owned()
        };

        Ok(Some(new_sql))
    }

    /// Construct a new `SelectStmt` to be set as the `subselect` of our new `RangeSubselect` node,
    /// whose inner-statement now is an *explicit* VALUES list of the returning results
    /// of the executed inner-query (as in, the returned `DataRow` list articulated explicitly
    /// within the outer statement)
    ///
    /// TODO(jkaczman): we need to account for too much data being present, and split
    /// apart the query into multiple when that occurs. This TODO also exists elsewhere for subqueries.
    fn new_select_with_explicit_values<'mem>(
        mem: MemoryToken<'mem>,
        row_description: RowDescription,
        data_rows: &[DataRow],
    ) -> Unique<'mem, &'mem SelectStmt> {
        let mut values_select = mem.make_node::<SelectStmt>();

        let mut decoder = Decoder::default();
        decoder.set_row_description(row_description);

        let mut elems = Vec::new();

        if data_rows.is_empty() {
            // We need to create a fake, all-NULL row, representing all columns
            // within our subquery, so that we can tell Postgres what the
            // RowDescription types to be to the client
            //
            // TODO(jkaczman): See the matching TODO for this in the area
            // below where we do the explicit type casts for the first row
            // when we don't have `DataRows::is_empty()`
            let mut singular_empty_top_level_list = Vec::new();
            for index in 0..decoder.row_description().fields.len() {
                if let Some(field) = decoder.row_description().field(index) {
                    let data_type: DataType = field.data_type();
                    let pg_catalog_type: &str = data_type.as_pg_catalog_type();

                    let mut node = mem.make_node::<nodes::A_Const>();
                    node.as_mut().set_isnull(true);

                    let node = mem
                        .make_type_cast(
                            node.uncast(),
                            mem.make_list(&[
                                mem.make_string(Some("pg_catalog")),
                                mem.make_string(Some(pg_catalog_type)),
                            ]),
                        )
                        .uncast();

                    singular_empty_top_level_list.push(node);
                }
            }

            // Now form a singular VALUES list containing NULL entries for all applicable
            // columns
            let new_list = mem.make_list(singular_empty_top_level_list.as_slice());
            elems.push(new_list);
        } else {
            for (row_idx, row) in data_rows.iter().enumerate() {
                let mut second = Vec::new();

                for col in 0..row.len() {
                    let datum: Datum = row
                        .get_column(col, &decoder)
                        .expect("get_column never returns an Error")
                        .unwrap()
                        .value;

                    let mut node = Self::convert_datum_to_a_const(datum.clone(), mem);

                    // TODO(jkaczman): Right now, this does explicit casts for each type on the
                    // first row as a workaround to properly set the RowDescription
                    // type OIDs in the response to the client. This is more query
                    // verbosity, which saves us backend logic to intercept the
                    // RowDescription response and modify it, however, at the expense
                    // of (well, making the query more verbose). Aka, this works fine,
                    // but could be represented better, perhaps by in the future intercepting
                    // when this is more mature.
                    if row_idx == 0 {
                        let data_type_str: &str = datum.data_type().as_pg_catalog_type();

                        // TODO(jkaczman): this is a frequently used construct;
                        // I think that we can create a method to share this throughout
                        // all the kinds... they all use pg_catalog casts
                        node = mem
                            .make_type_cast(
                                node.uncast(),
                                mem.make_list(&[
                                    mem.make_string(Some("pg_catalog")),
                                    mem.make_string(Some(data_type_str)),
                                ]),
                            )
                            .uncast();
                    }

                    second.push(node.uncast());
                }

                let new_list = mem.make_list(second.as_slice());
                elems.push(new_list);
            }
        }

        let new_list = mem.make_list(elems.as_slice());
        values_select.as_mut().set_values_lists(new_list);

        values_select
    }

    /// Assemble a new alias to be used within the `RangeSubselect` node we're re-writing,
    /// to include the colnames if they're not already present. This allows for explicit
    /// <ALIAS>.<col> use within the outer-query (whom references the `RangeSubselect` results)
    fn make_alias<'mem>(
        mem: MemoryToken<'mem>,
        existing_alias: &Alias,
        row_description: RowDescription,
    ) -> Unique<'mem, &'mem Alias> {
        let mut new_alias = mem.make_unique(existing_alias);

        let mut col_name_list = Vec::new();
        for i in 0..row_description.len() {
            col_name_list.push(
                mem.make_string(Some(row_description.fields.get(i).unwrap().name.as_str()))
                    .uncast(),
            );
        }

        new_alias
            .as_mut()
            .set_colnames(mem.make_list(col_name_list.as_ref()));

        new_alias
    }
}
