use crate::net::{CommandComplete, DataRow, Field, Protocol, ReadyForQuery, RowDescription};

use super::*;

impl QueryEngine {
    /// SHOW pgdog.shards.
    pub(super) async fn show_shards(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        shards: usize,
    ) -> Result<(), Error> {
        let bytes_sent = context
            .stream
            .send_many(&[
                RowDescription::new(&[Field::bigint("shards")]).message()?,
                DataRow::from_columns(vec![shards]).message()?,
                CommandComplete::from_str("SHOW").message()?,
                ReadyForQuery::in_transaction(context.in_transaction()).message()?,
            ])
            .await?;

        self.stats.sent(bytes_sent);

        Ok(())
    }
}
