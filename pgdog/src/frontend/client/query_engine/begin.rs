use crate::{
    frontend::Error,
    net::{CommandComplete, Protocol, ReadyForQuery},
};

use super::engine_impl::Stream;

pub struct Begin {
    // TODO: Send notice if transaction already started.
    #[allow(dead_code)]
    in_transaction: bool,
}

impl Begin {
    pub fn new(in_transaction: bool) -> Self {
        Self { in_transaction }
    }

    pub async fn handle(&mut self, client_socket: &mut Stream) -> Result<(), Error> {
        client_socket
            .send_many(&[
                CommandComplete::new_begin().message()?.backend(),
                ReadyForQuery::in_transaction(true).message()?.backend(),
            ])
            .await?;

        Ok(())
    }
}
