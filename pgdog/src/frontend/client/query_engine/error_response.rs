use crate::{frontend::Error, net::ErrorResponse};
use tracing::error;

use super::engine_impl::Stream;

pub struct ErrorHandler {
    in_transaction: bool,
}

impl ErrorHandler {
    pub fn new(in_transaction: bool) -> Self {
        Self { in_transaction }
    }

    pub async fn handle(
        &self,
        err: impl std::error::Error,
        stream: &mut Stream,
    ) -> Result<(), Error> {
        let err = err.to_string();

        error!("{} [{:?}]", err, stream.peer_addr());

        stream
            .error(
                ErrorResponse::syntax(err.to_string().as_str()),
                self.in_transaction,
            )
            .await?;

        Ok(())
    }
}
