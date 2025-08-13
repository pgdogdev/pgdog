use crate::{
    frontend::Error,
    net::{EmptyQueryResponse, ReadyForQuery},
};

use super::engine_impl::Stream;

pub struct EmptyQuery {
    in_transaction: bool,
}

impl EmptyQuery {
    pub fn new(in_transaction: bool) -> Self {
        Self { in_transaction }
    }

    pub async fn handle(&self, stream: &mut Stream) -> Result<(), Error> {
        stream.send(&EmptyQueryResponse).await?;
        stream
            .send_flush(&ReadyForQuery::in_transaction(self.in_transaction))
            .await?;

        Ok(())
    }
}
