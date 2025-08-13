use crate::{
    backend::pool::Connection,
    frontend::Error,
    net::{CommandComplete, Protocol, ReadyForQuery},
};

use super::engine_impl::Stream;

pub struct Commit<'a> {
    backend: &'a mut Connection,
}

impl<'a> Commit<'a> {
    pub fn new(backend: &'a mut Connection) -> Self {
        Self { backend }
    }

    pub async fn handle(&mut self, client_socket: &mut Stream) -> Result<(), Error> {
        // TODO: Handle 2pc.
        if self.backend.connected() {
            self.backend.execute("COMMIT").await?;
        }

        client_socket
            .send_many(&[
                CommandComplete::new_commit().message()?,
                ReadyForQuery::in_transaction(false).message()?,
            ])
            .await?;

        Ok(())
    }
}
