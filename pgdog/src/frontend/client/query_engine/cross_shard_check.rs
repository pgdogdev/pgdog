use crate::{
    frontend::{router::Route, Error},
    net::ErrorResponse,
};

use super::engine_impl::Stream;

pub struct CrossShardCheck<'a> {
    disabled: bool,
    in_transaction: bool,
    route: &'a Route,
}

impl<'a> CrossShardCheck<'a> {
    /// Creates a new CrossShardCheck instance.
    ///
    /// # Arguments
    /// * `disabled` - Whether cross-shard queries are disabled
    /// * `route` - The query route to check for cross-shard operations
    /// * `transaction` - Mutable reference to the transaction state
    ///
    pub fn new(disabled: bool, route: &'a Route, in_transaction: bool) -> Self {
        Self {
            disabled,
            route,
            in_transaction,
        }
    }

    pub async fn handle(&'a mut self, client_socket: &mut Stream) -> Result<bool, Error> {
        if self.disabled && self.route.is_cross_shard() {
            client_socket
                .error(ErrorResponse::cross_shard_disabled(), self.in_transaction)
                .await?;

            Ok(true)
        } else {
            Ok(false)
        }
    }
}
