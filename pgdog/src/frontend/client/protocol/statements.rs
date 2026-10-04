//! Build result rows and delegate protocol replies to Commands.

use super::{ClientMessages, Commands, Statement};
use crate::{
    frontend::client::{Transaction, query_engine::Pipeline},
    net::{DataRow, Error, Field, RowDescription, Stream, ToDataRowColumn},
};

#[derive(Debug, Clone)]
pub(in crate::frontend) struct Statements {
    commands: Commands,
}

#[bon::bon]
impl Statements {
    #[builder(start_fn = builder, finish_fn = build)]
    pub(in crate::frontend) fn new(
        transaction: Option<Transaction>,
        pipeline: &Pipeline,
        statement: Statement,
    ) -> Self {
        Self {
            commands: Commands::builder()
                .maybe_transaction(transaction)
                .pipeline(pipeline)
                .statement(statement)
                .build(),
        }
    }

    /// Describe the result columns as text fields.
    pub(in crate::frontend) fn columns(mut self, columns: &[&str]) -> Self {
        let fields: Vec<_> = columns.iter().map(|name| Field::text(name)).collect();
        self.commands.row_description = Some(RowDescription::new(&fields));
        self
    }

    /// Append a result row, preserving NULL values and updating the row count.
    pub(in crate::frontend) fn with_row(
        mut self,
        values: impl IntoIterator<Item = impl ToDataRowColumn>,
    ) -> Self {
        let mut row = DataRow::new();
        for value in values {
            row.add(value);
        }
        self.commands.data_rows.push(row);
        self.commands.rows_affected = self.commands.data_rows.len();
        self
    }

    pub(in crate::frontend) async fn send_reply<'a>(
        &self,
        messages: impl Into<ClientMessages<'a>>,
        stream: &mut Stream,
    ) -> Result<(usize, bool), Error> {
        self.commands.send_reply(messages, stream).await
    }
}
