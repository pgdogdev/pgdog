use tokio::io::AsyncWriteExt;

use crate::net::{CloseComplete, Protocol, ReadyForQuery};

use super::*;

impl QueryEngine {
    /// Check for incomplete requests that don't need to be
    /// sent to a server.
    pub(super) async fn intercept_incomplete(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<bool, Error> {
        // Don't intercept requests when we are connected.
        if self.backend.connected() {
            return Ok(false);
        }

        // Client sent Sync only
        let only_sync = context
            .client_request
            .messages
            .iter()
            .all(|m| m.code() == 'S');

        // Client sent only Close.
        let only_close = context
            .client_request
            .messages
            .iter()
            .all(|m| ['C', 'S'].contains(&m.code()))
            && !only_sync;

        let mut bytes_sent = 0;

        for msg in context.client_request.messages.iter() {
            match msg.code() {
                'C' => {
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
