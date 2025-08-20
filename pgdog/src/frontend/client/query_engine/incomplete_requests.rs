use tokio::io::AsyncWriteExt;

use crate::net::{Close, CloseComplete, FromBytes, Protocol, ReadyForQuery, ToBytes};

use super::*;

impl QueryEngine {
    /// Check for incomplete requests that don't need to be
    /// sent to a server.
    pub(super) async fn intercept_incomplete(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<bool, Error> {
        // Client sent Sync only
        let only_sync = context.client_request.iter().all(|m| m.code() == 'S');
        // Client sent only Close.
        let only_close = context
            .client_request
            .iter()
            .all(|m| ['C', 'S'].contains(&m.code()))
            && !only_sync;
        let mut bytes_sent = 0;

        for msg in context.client_request.iter() {
            match msg.code() {
                'C' => {
                    let close = Close::from_bytes(msg.to_bytes()?)?;
                    if close.is_statement() {
                        context.prepared_statements.close(close.name());
                    }
                    if only_close {
                        bytes_sent += context.stream.send(&CloseComplete).await?;
                    }
                }
                'S' => {
                    if only_close || only_sync && !self.backend.connected() {
                        bytes_sent += context
                            .stream
                            .send(&ReadyForQuery::in_transaction(context.in_transaction()))
                            .await?;
                    }
                }
                c => {
                    if only_close {
                        return Err(Error::UnexpectedMessage(c)); // Impossible.
                    }
                }
            }
        }

        self.stats.sent(bytes_sent);

        if bytes_sent > 0 {
            debug!("incomplete request intercepted");
            context.stream.flush().await?;
        }

        Ok(bytes_sent > 0)
    }
}
