use std::ops::Deref;

use pg_raw_parse::{Node, deparse, make::owned, nodes::SelectStmt};

use crate::{
    frontend::client::query_engine::multi_step::{Error, SubqueryMulti},
    frontend::router::parser::Error as ParserError,
    net::{DataRow, Decoder, RowDescription},
};

impl<'a> SubqueryMulti<'a> {
    /// Re-writes the top-level INSERT (with SELECT FROM subquery) with the evaluated subquery's RowDescription and DataRows,
    /// and returns the re-written SQL.
    pub fn handle_insert_select(
        &self,
        stmt: Node<'a>,
        row_description: Option<&RowDescription>,
        data_rows: &Vec<DataRow>,
    ) -> Result<Option<String>, Error> {
        let Node::InsertStmt(insert) = stmt else {
            return Ok(None);
        };

        // Replace with SelectStmt with new VALUES
        let new_insert = owned(|mem| {
            let mut new_select = mem.make_node::<SelectStmt>();
            let mut new_select_mut = new_select.as_mut();

            let mut decoder = Decoder::default();
            decoder.set_row_description(row_description.unwrap().clone());

            // Build the VALUES list from the DataRows
            let mut lists = Vec::new();
            for row in data_rows {
                let mut elems = Vec::new();

                for col in 0..row.len() {
                    let datum = row.get_column(col, &decoder).unwrap().unwrap().value;

                    elems.push(Self::convert_datum_to_a_const(datum, mem).uncast());
                }

                let tuple = mem.make_list(elems.as_ref()).uncast();
                lists.push(tuple);
            }

            let values_lists = mem.make_list(lists.as_ref());
            new_select_mut.set_values_lists(values_lists);

            let mut cloned_insert = mem.make_unique(insert);
            cloned_insert.as_mut().set_select_stmt(new_select.uncast());

            cloned_insert
        });

        let new_sql: String = {
            let new_insert_sql = deparse(new_insert.deref()).map_err(ParserError::from)?;
            let new_insert_sql = new_insert_sql.as_str();

            new_insert_sql.to_string()
        };

        Ok(Some(new_sql))
    }
}
