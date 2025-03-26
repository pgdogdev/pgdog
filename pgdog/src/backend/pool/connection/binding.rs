//! Binding between frontend client and a connection on the backend.

use crate::net::parameter::Parameters;

use super::*;

/// The server(s) the client is connected to.
#[derive(Debug)]
pub(super) enum Binding {
    Server(Option<Guard>),
    Admin(Backend),
    MultiShard(Vec<Guard>, MultiShard),
    Replication(Option<Guard>, Buffer),
}

impl Default for Binding {
    fn default() -> Self {
        Binding::Server(None)
    }
}

impl Binding {
    pub(super) fn disconnect(&mut self) {
        match self {
            Binding::Server(guard) => drop(guard.take()),
            Binding::Admin(_) => (),
            Binding::MultiShard(guards, _) => guards.clear(),
            Binding::Replication(guard, _) => drop(guard.take()),
        }
    }

    pub(super) fn connected(&self) -> bool {
        match self {
            Binding::Server(server) => server.is_some(),
            Binding::MultiShard(servers, _) => !servers.is_empty(),
            Binding::Admin(_) => true,
            Binding::Replication(server, _) => server.is_some(),
        }
    }

    pub(super) async fn read(&mut self) -> Result<Message, Error> {
        match self {
            Binding::Server(guard) => {
                if let Some(guard) = guard.as_mut() {
                    guard.read().await
                } else {
                    loop {
                        sleep(Duration::MAX).await
                    }
                }
            }

            Binding::Admin(backend) => Ok(backend.read().await?),
            Binding::MultiShard(shards, state) => {
                if shards.is_empty() {
                    loop {
                        sleep(Duration::MAX).await;
                    }
                } else {
                    // Loop until we read a message from a shard
                    // or there are no more messages to be read.
                    loop {
                        // Return all sorted data rows if any.
                        if let Some(message) = state.message() {
                            return Ok(message);
                        }

                        let pending = shards
                            .iter_mut()
                            .filter(|s| s.has_more_messages())
                            .collect::<Vec<_>>();

                        if pending.is_empty() {
                            break;
                        }

                        for shard in pending {
                            let message = shard.read().await?;
                            if let Some(message) = state.forward(message)? {
                                return Ok(message);
                            }
                        }
                    }

                    loop {
                        *state = state.new_reset();
                        sleep(Duration::MAX).await;
                    }
                }
            }

            Binding::Replication(guard, buffer) => {
                if let Some(message) = buffer.message() {
                    return Ok(message);
                }

                if let Some(server) = guard {
                    loop {
                        let message = server.read().await?;
                        buffer.handle(message)?;

                        if let Some(message) = buffer.message() {
                            return Ok(message);
                        }
                    }
                } else {
                    loop {
                        sleep(Duration::MAX).await
                    }
                }
            }
        }
    }

    pub(super) async fn send(&mut self, messages: Vec<impl Protocol>) -> Result<(), Error> {
        match self {
            Binding::Server(server) => {
                if let Some(server) = server {
                    server.send(messages).await
                } else {
                    Err(Error::NotConnected)
                }
            }

            Binding::Admin(backend) => Ok(backend.send(messages).await?),
            Binding::MultiShard(servers, _state) => {
                for server in servers.iter_mut() {
                    let messages = messages.iter().map(|m| m.message().unwrap()).collect();
                    server.send(messages).await?;
                }

                Ok(())
            }
            Binding::Replication(server, _) => {
                if let Some(server) = server {
                    server.send(messages).await
                } else {
                    Err(Error::NotConnected)
                }
            }
        }
    }

    /// Send copy messages to shards they are destined to go.
    pub(super) async fn send_copy(&mut self, rows: Vec<CopyRow>) -> Result<(), Error> {
        match self {
            Binding::MultiShard(servers, _state) => {
                for row in rows {
                    for (shard, server) in servers.iter_mut().enumerate() {
                        if let Shard::Direct(row_shard) = row.shard() {
                            if shard == *row_shard {
                                server.send_one(row.message()).await?;
                            }
                        } else {
                            server.send_one(row.message()).await?;
                        }
                    }
                }
                Ok(())
            }

            _ => Err(Error::CopyNotConnected),
        }
    }

    pub(super) fn done(&self) -> bool {
        match self {
            Binding::Admin(_) => true,
            Binding::Server(Some(server)) => server.done(),
            Binding::MultiShard(servers, _state) => servers.iter().all(|s| s.done()),
            Binding::Replication(Some(server), _) => server.done(),
            _ => true,
        }
    }

    /// Execute a query on all servers.
    pub(super) async fn execute(&mut self, query: &str) -> Result<(), Error> {
        match self {
            Binding::Server(Some(ref mut server)) => {
                server.execute(query).await?;
            }

            Binding::MultiShard(ref mut servers, _) => {
                for server in servers {
                    server.execute(query).await?;
                }
            }

            Binding::Replication(Some(ref mut server), _) => {
                server.execute(query).await?;
            }

            _ => (),
        }

        Ok(())
    }

    pub(super) async fn sync_params(&mut self, params: &Parameters) -> Result<(), Error> {
        match self {
            Binding::Server(Some(ref mut server)) => server.sync_params(params).await,
            Binding::MultiShard(ref mut servers, _) => {
                for server in servers {
                    server.sync_params(params).await?;
                }
                Ok(())
            }
            Binding::Replication(Some(ref mut server), _) => server.sync_params(params).await,

            _ => Ok(()),
        }
    }

    pub(super) fn changed_params(&mut self) -> Parameters {
        match self {
            Binding::Server(Some(ref mut server)) => server.changed_params(),
            Binding::MultiShard(ref mut servers, _) => {
                let mut params = Parameters::default();
                for server in servers {
                    server.changed_params().merge(&mut params);
                }
                params
            }
            Binding::Replication(Some(ref mut server), _) => server.changed_params(),
            _ => Parameters::default(),
        }
    }
}
