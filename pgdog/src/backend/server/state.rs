//! Server state and parameter management.

use tracing::debug;

use super::{Error, Server};
use crate::net::{
    messages::{ErrorResponse, FromBytes, Protocol, ToBytes},
    parameter::Parameters,
    Close, Sync,
};

impl Server {
    /// Synchronize parameters between client and server.
    pub async fn link_client(&mut self, params: &Parameters) -> Result<usize, Error> {
        // Sync application_name parameter
        // and update it in the stats.
        let default_name = "PgDog";
        let server_name = self
            .client_params
            .get_default("application_name", default_name);
        let client_name = params.get_default("application_name", default_name);
        self.stats.link_client(client_name, server_name);

        // Clear any params previously tracked by SET.
        self.changed_params.clear();

        // Compare client and server params.
        if !params.identical(&self.client_params) {
            let tracked = params.tracked();
            let mut queries = self.client_params.reset_queries();
            queries.extend(tracked.set_queries());
            if !queries.is_empty() {
                debug!("syncing {} params", queries.len());
                self.execute_batch(&queries).await?;
            }
            self.client_params = tracked;
            Ok(queries.len())
        } else {
            Ok(0)
        }
    }

    pub fn changed_params(&self) -> &Parameters {
        &self.changed_params
    }

    pub fn reset_changed_params(&mut self) {
        self.changed_params.clear();
    }

    #[inline]
    pub fn reset_re_synced(&mut self) {
        self.re_synced = false;
    }

    #[inline]
    pub fn re_synced(&self) -> bool {
        self.re_synced
    }

    /// Reset error state caused by schema change.
    #[inline]
    pub fn reset_schema_changed(&mut self) {
        self.schema_changed = false;
        self.prepared_statements.clear();
    }

    #[inline]
    pub fn reset_params(&mut self) {
        self.client_params.clear();
    }

    pub async fn sync_prepared_statements(&mut self) -> Result<(), Error> {
        let names = self
            .fetch_all::<String>("SELECT name FROM pg_prepared_statements")
            .await?;

        for name in names {
            self.prepared_statements.prepared(&name);
        }

        debug!("prepared statements synchronized [{}]", self.addr());

        let count = self.prepared_statements.len();
        self.stats_mut().set_prepared_statements(count);

        Ok(())
    }

    /// Close any prepared statements that exceed cache capacity.
    pub fn ensure_prepared_capacity(&mut self) -> Vec<Close> {
        let close = self.prepared_statements.ensure_capacity();
        self.stats
            .close_many(close.len(), self.prepared_statements.len());
        close
    }

    /// Close multiple prepared statements.
    pub async fn close_many(&mut self, close: &[Close]) -> Result<(), Error> {
        if close.is_empty() {
            return Ok(());
        }

        let mut buf = vec![];
        for close in close {
            buf.push(close.message()?);
        }

        buf.push(Sync.message()?);

        debug!(
            "closing {} prepared statements [{}]",
            close.len(),
            self.addr()
        );

        self.stream().send_many(&buf).await?;

        for close in close {
            let response = self.stream().read().await?;
            match response.code() {
                '3' => self.prepared_statements.remove(close.name()),
                'E' => {
                    return Err(Error::PreparedStatementError(Box::new(
                        ErrorResponse::from_bytes(response.to_bytes()?)?,
                    )));
                }
                c => {
                    return Err(Error::UnexpectedMessage(c));
                }
            };
        }

        let rfq = self.stream().read().await?;
        if rfq.code() != 'Z' {
            return Err(Error::UnexpectedMessage(rfq.code()));
        }

        Ok(())
    }
}

#[cfg(test)]
pub mod test {
    // State management tests will be moved here
}
