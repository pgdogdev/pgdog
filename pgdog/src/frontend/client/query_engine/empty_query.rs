use crate::{
    frontend::{Error, Stats},
    net::{EmptyQueryResponse, ReadyForQuery},
};

use super::engine_impl::Stream;

pub struct EmptyQuery<'a> {
    in_transaction: bool,
    stats: &'a mut Stats,
}

impl<'a> EmptyQuery<'a> {
    pub fn new(in_transaction: bool, stats: &'a mut Stats) -> Self {
        Self {
            in_transaction,
            stats,
        }
    }

    pub async fn handle(&mut self, stream: &mut Stream) -> Result<(), Error> {
        let mut sent = stream.send(&EmptyQueryResponse).await?;
        sent += stream
            .send_flush(&ReadyForQuery::in_transaction(self.in_transaction))
            .await?;

        self.stats.sent(sent);

        Ok(())
    }
}
