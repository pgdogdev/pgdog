use pg_raw_parse::{
    Node, NodeMut,
    make::{MemoryToken, Unique},
    nodes::WithClause,
};

use crate::{
    frontend::client::query_engine::multi_step::SubqueryMulti,
    net::{Bind, DataRow, RowDescription},
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
        new_bind: &mut Bind,
    ) -> Unique<'mem, Node<'mem>> {
        let NodeMut::InsertStmt(mut insert) = stmt.as_mut() else {
            return stmt;
        };

        let Some(row_description) = row_description else {
            return stmt;
        };

        let new_select = Self::new_select_with_unnest(
            new_bind,
            mem,
            insert.select_stmt(),
            data_rows,
            row_description,
        );

        insert.set_select_stmt(new_select.uncast());
        insert.set_with_clause(mem.make_unique(None::<&WithClause>));

        stmt
    }
}
