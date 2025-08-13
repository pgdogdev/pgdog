use crate::{
    frontend::{Error, Stats},
    net::ErrorResponse,
};
use tracing::error;

use super::engine_impl::Stream;

pub struct ErrorHandler<'a> {
    in_transaction: bool,
    stats: &'a mut Stats,
}

impl<'a> ErrorHandler<'a> {
    pub fn new(in_transaction: bool, stats: &'a mut Stats) -> Self {
        Self {
            in_transaction,
            stats,
        }
    }

    pub async fn handle(
        &mut self,
        err: &impl std::error::Error,
        stream: &mut Stream,
    ) -> Result<(), Error> {
        let err = err.to_string();

        error!("{} [{:?}]", err, stream.peer_addr());

        let bytes_sent = stream
            .error(
                ErrorResponse::syntax(err.to_string().as_str()),
                self.in_transaction,
            )
            .await?;

        self.stats.sent(bytes_sent);

        Ok(())
    }
}
