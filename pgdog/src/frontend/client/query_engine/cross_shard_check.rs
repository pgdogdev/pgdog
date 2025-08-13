use crate::{
    frontend::{router::Route, Error, Stats},
    net::ErrorResponse,
};

use super::engine_impl::Stream;

pub struct CrossShardCheck<'a> {
    disabled: bool,
    in_transaction: bool,
    route: &'a Route,
    stats: &'a mut Stats,
}

impl<'a> CrossShardCheck<'a> {
    /// Creates a new CrossShardCheck instance.
    ///
    /// # Arguments
    /// * `disabled` - Whether cross-shard queries are disabled
    /// * `route` - The query route to check for cross-shard operations
    /// * `transaction` - Mutable reference to the transaction state
    ///
    pub fn new(
        disabled: bool,
        route: &'a Route,
        in_transaction: bool,
        stats: &'a mut Stats,
    ) -> Self {
        Self {
            disabled,
            route,
            in_transaction,
            stats,
        }
    }

    pub async fn handle(&'a mut self, client_socket: &mut Stream) -> Result<bool, Error> {
        if self.disabled && self.route.is_cross_shard() {
            let bytes_sent = client_socket
                .error(ErrorResponse::cross_shard_disabled(), self.in_transaction)
                .await?;
            self.stats.sent(bytes_sent);

            Ok(true)
        } else {
            Ok(false)
        }
    }
}
