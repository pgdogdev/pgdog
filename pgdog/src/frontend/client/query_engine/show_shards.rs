use crate::{
    frontend::{Error, Stats},
    net::{CommandComplete, DataRow, Field, Protocol, ReadyForQuery, RowDescription},
};

use super::engine_impl::Stream;

pub struct ShowShards<'a> {
    shards: usize,
    stats: &'a mut Stats,
    in_transaction: bool,
}

impl<'a> ShowShards<'a> {
    pub fn new(shards: usize, stats: &'a mut Stats, in_transaction: bool) -> Self {
        Self {
            shards,
            stats,
            in_transaction,
        }
    }

    pub async fn handle(&'a mut self, client_socket: &mut Stream) -> Result<(), Error> {
        let bytes_sent = client_socket
            .send_many(&[
                RowDescription::new(&[Field::bigint("shards")]).message()?,
                DataRow::from_columns(&[self.shards]).message()?,
                CommandComplete::new("SHOW").message()?,
                ReadyForQuery::in_transaction(self.in_transaction).message()?,
            ])
            .await?;

        self.stats.sent(bytes_sent);

        Ok(())
    }
}
