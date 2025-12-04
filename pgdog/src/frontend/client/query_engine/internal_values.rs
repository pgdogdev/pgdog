use crate::{
    net::{CommandComplete, DataRow, Field, Protocol, ReadyForQuery, RowDescription},
    unique_id,
};

use super::*;

impl QueryEngine {
    /// SHOW pgdog.shards.
    pub(super) async fn show_internal_value(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        field: String,
        value: String,
    ) -> Result<(), Error> {
        let bytes_sent = context
            .stream
            .send_many(&[
                RowDescription::new(&[Field::text(&field)]).message()?,
                DataRow::from_columns(vec![value]).message()?,
                CommandComplete::from_str("SHOW").message()?,
                ReadyForQuery::in_transaction(context.in_transaction()).message()?,
            ])
            .await?;

        self.stats.sent(bytes_sent);

        Ok(())
    }

    pub(super) async fn unique_id(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        let id = unique_id::UniqueId::generator()?.next_id();
        let bytes_sent = context
            .stream
            .send_many(&[
                RowDescription::new(&[Field::bigint("unique_id")]).message()?,
                DataRow::from_columns(vec![id.to_string()]).message()?,
                CommandComplete::from_str("SHOW").message()?,
                ReadyForQuery::in_transaction(context.in_transaction()).message()?,
            ])
            .await?;

        self.stats.sent(bytes_sent);

        Ok(())
    }
}
