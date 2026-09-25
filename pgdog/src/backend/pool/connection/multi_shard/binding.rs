use std::ops::{Deref, DerefMut};

use futures::future::join_all;

use crate::backend::Error;
use crate::frontend::ClientRequest;
use crate::frontend::router::parser::Shard;
use crate::frontend::router::{CopyRow, Route};
use crate::net::{Message, ProtocolMessage};

use super::super::Guard;
use super::MultiShard;

/// Handle talking to multiple servers for cross-shard queries.
#[derive(Debug)]
pub(crate) struct MultiBinding {
    servers: Vec<Guard>,
    state: Box<MultiShard>,
}

impl From<Vec<Guard>> for MultiBinding {
    fn from(value: Vec<Guard>) -> Self {
        Self {
            servers: value,
            state: Box::default(),
        }
    }
}

impl MultiBinding {
    /// Create new multi-shard binding.
    pub(crate) fn new(servers: Vec<Guard>, shard_indices: Vec<usize>, route: &Route) -> Self {
        Self {
            state: Box::new(MultiShard::new(shard_indices, route)),
            servers,
        }
    }

    /// Read-only handle to internal state.
    pub(crate) fn state(&self) -> &MultiShard {
        &self.state
    }

    /// Write-handle to internal state.
    ///
    /// BUG: This should not exist.
    pub(crate) fn state_mut(&mut self) -> &mut MultiShard {
        &mut self.state
    }

    /// Read 1 message from one of the shards.
    pub(crate) async fn read(&mut self) -> Result<Option<Message>, Error> {
        loop {
            // Return all sorted data rows if any.
            if let Some(message) = self.state.get_server_message() {
                return Ok(Some(message));
            }

            let mut read = false;
            for server in &mut self.servers {
                if !server.has_more_messages() {
                    continue;
                }

                let message = server.read().await?;
                read = true;

                if let Some(message) = self.state.handle_server_message(message)? {
                    return Ok(Some(message));
                }
            }

            if !read {
                break;
            }
        }

        Ok(None)
    }

    /// Send client request to the shards it should go to.
    pub(crate) async fn send(&mut self, client_request: &ClientRequest) -> Result<(), Error> {
        let mut shards_sent = self.servers.len();
        let mut futures = Vec::new();

        for (position, server) in self.servers.iter_mut().enumerate() {
            // Map positional index to actual shard number.
            // When only a subset of shards is connected (Shard::Multi binding),
            // positional indices don't match actual shard numbers.
            let shard = self.state.shard_number(position);
            let send = match client_request.route().shard() {
                Shard::Direct(s) => {
                    shards_sent = 1;
                    *s == shard
                }
                Shard::Multi(shards) => {
                    shards_sent = shards.len();
                    shards.contains(&shard)
                }
                Shard::All => true,
            };

            if send {
                futures.push(server.send(client_request));
            }
        }

        let results = join_all(futures).await;

        for result in results {
            result?;
        }

        // For Sync-only requests, update shards count but don't reset counters.
        // Sync needs correct shards for ReadyForQuery counting, but we must
        // preserve buffered CommandComplete from previous queries.
        if client_request.is_sync_only() {
            self.state.update_shards(shards_sent);
        } else {
            self.state.update(shards_sent, client_request.route());
        }

        Ok(())
    }

    /// Send a message the reply for which we will ignore to the shard(s) it needs to go to.
    pub(crate) async fn send_ignore(
        &mut self,
        message: &ProtocolMessage,
        route: &Route,
    ) -> Result<(), Error> {
        if self.servers.is_empty() {
            return Ok(());
        }

        let mut futures = Vec::new();
        for (position, server) in self.servers.iter_mut().enumerate() {
            let shard = self.state.shard_number(position);
            let send = match route.shard() {
                Shard::Direct(s) => *s == shard,
                Shard::Multi(shards) => shards.contains(&shard),
                Shard::All => true,
            };
            if send {
                futures.push(server.send_ignore(message));
            }
        }
        let results = join_all(futures).await;

        for result in results {
            result?;
        }

        Ok(())
    }

    /// Send COPY rows to all shards.
    pub(crate) async fn send_copy(&mut self, rows: Vec<CopyRow>) -> Result<(), Error> {
        for row in rows {
            for (position, server) in self.servers.iter_mut().enumerate() {
                let shard = self.state.shard_number(position);
                match row.shard() {
                    Shard::Direct(row_shard) => {
                        if shard == *row_shard {
                            server
                                .send_one(&ProtocolMessage::from(row.message()))
                                .await?;
                        }
                    }

                    Shard::All => {
                        server
                            .send_one(&ProtocolMessage::from(row.message()))
                            .await?;
                    }

                    Shard::Multi(multi) => {
                        if multi.contains(&shard) {
                            server
                                .send_one(&ProtocolMessage::from(row.message()))
                                .await?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl Deref for MultiBinding {
    type Target = Vec<Guard>;

    fn deref(&self) -> &Self::Target {
        &self.servers
    }
}

impl DerefMut for MultiBinding {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.servers
    }
}
