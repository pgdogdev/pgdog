use pg_raw_parse::{
    Node, NodeMut,
    make::{MemoryToken, Unique},
    nodes::{SelectStmt, WithClause},
};

use crate::{
    frontend::client::query_engine::multi_step::SubqueryMulti,
    net::{DataRow, Decoder, RowDescription},
};

impl<'a> SubqueryMulti<'a> {
    /// Re-writes the top-level INSERT (with SELECT FROM subquery) with the evaluated subquery's RowDescription and DataRows,
    /// and returns the re-written SQL.
    pub fn handle_insert_select<'mem>(
        &self,
        mem: MemoryToken<'mem>,
        mut stmt: Unique<'mem, Node<'mem>>,
        row_description: Option<&RowDescription>,
        data_rows: &[DataRow],
    ) -> Unique<'mem, Node<'mem>> {
        let NodeMut::InsertStmt(mut insert) = stmt.as_mut() else {
            return stmt;
        };

        // Replace with SelectStmt with new VALUES
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

        insert.set_select_stmt(new_select.uncast());
        insert.set_with_clause(mem.make_unique(None::<&WithClause>));

        stmt
    }
}
