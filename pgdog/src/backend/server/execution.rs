//! Server query execution operations.

use tracing::debug;

use super::{Error, Server};
use crate::net::{
    messages::{DataRow, ErrorResponse, FromBytes, Message, Protocol, Query, ToBytes},
    ProtocolMessage,
};

impl Server {
    /// Execute a batch of queries and return all results.
    pub async fn execute_batch(&mut self, queries: &[Query]) -> Result<Vec<Message>, Error> {
        let mut err = None;
        if !self.in_sync() {
            return Err(Error::NotInSync);
        }

        // Empty queries will throw the server out of sync.
        if queries.is_empty() {
            return Ok(vec![]);
        }

        #[cfg(debug_assertions)]
        for query in queries {
            debug!("{} [{}]", query.query(), self.addr());
        }

        let mut messages = vec![];
        let queries = queries
            .iter()
            .map(Clone::clone)
            .map(ProtocolMessage::Query)
            .collect::<Vec<ProtocolMessage>>();
        let expected = queries.len();

        self.send(&queries.into()).await?;

        let mut zs = 0;
        while zs < expected {
            let message = self.read().await?;
            if message.code() == 'Z' {
                zs += 1;
            }

            if message.code() == 'E' {
                err = Some(ErrorResponse::from_bytes(message.to_bytes()?)?);
            }
            messages.push(message);
        }

        if let Some(err) = err {
            Err(Error::ExecutionError(Box::new(err)))
        } else {
            Ok(messages)
        }
    }

    /// Execute a query on the server and return the result.
    pub async fn execute(&mut self, query: impl Into<Query>) -> Result<Vec<Message>, Error> {
        let query = query.into();
        self.execute_batch(&[query]).await
    }

    /// Execute query and raise an error if one is returned by PostgreSQL.
    pub async fn execute_checked(
        &mut self,
        query: impl Into<Query>,
    ) -> Result<Vec<Message>, Error> {
        let messages = self.execute(query).await?;
        let error = messages.iter().find(|m| m.code() == 'E');
        if let Some(error) = error {
            let error = ErrorResponse::from_bytes(error.to_bytes()?)?;
            Err(Error::ExecutionError(Box::new(error)))
        } else {
            Ok(messages)
        }
    }

    /// Execute a query and return all rows.
    pub async fn fetch_all<T: From<DataRow>>(
        &mut self,
        query: impl Into<Query>,
    ) -> Result<Vec<T>, Error> {
        let messages = self.execute_checked(query).await?;
        Ok(messages
            .into_iter()
            .filter(|message| message.code() == 'D')
            .map(|message| message.to_bytes().unwrap())
            .map(DataRow::from_bytes)
            .collect::<Result<Vec<DataRow>, crate::net::Error>>()?
            .into_iter()
            .map(|row| T::from(row))
            .collect())
    }

    /// Perform a healthcheck on this connection using the provided query.
    pub async fn healthcheck(&mut self, query: &str) -> Result<(), Error> {
        debug!("running healthcheck \"{}\" [{}]", query, self.addr);

        self.execute(query).await?;
        self.stats.healthcheck();

        Ok(())
    }
}

#[cfg(test)]
pub mod test {
    // Execution tests will be moved here
}
