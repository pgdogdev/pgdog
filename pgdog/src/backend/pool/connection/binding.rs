//! Binding between frontend client and a connection on the backend.

use crate::{backend::ProtocolMessage, net::parameter::Parameters, state::State};

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

    pub(super) fn force_close(&mut self) {
        match self {
            Binding::Server(Some(ref mut guard)) => guard.stats_mut().state(State::ForceClose),
            Binding::MultiShard(ref mut guards, _) => {
                for guard in guards {
                    guard.stats_mut().state(State::ForceClose);
                }
            }
            Binding::Replication(Some(ref mut guard), _) => {
                guard.stats_mut().state(State::ForceClose);
            }
            _ => (),
        }

        self.disconnect();
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
                        debug!("binding suspended");
                        sleep(Duration::MAX).await
                    }
                }
            }

            Binding::Admin(backend) => Ok(backend.read().await?),
            Binding::MultiShard(shards, state) => {
                if shards.is_empty() {
                    loop {
                        debug!("multi-shard binding suspended");
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
                        let mut read = false;
                        for server in shards.iter_mut() {
                            if !server.has_more_messages() {
                                continue;
                            }

                            let message = server.read().await?;
                            read = true;
                            if let Some(message) = state.forward(message)? {
                                return Ok(message);
                            }
                        }

                        if !read {
                            break;
                        }
                    }

                    loop {
                        state.reset();
                        debug!("multi-shard binding done");
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

    pub(super) async fn send(&mut self, messages: &crate::frontend::Buffer) -> Result<(), Error> {
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

            _ => Err(Error::CopyNotConnected),
        }
    }

    pub(super) fn done(&self) -> bool {
        match self {
            Binding::Admin(admin) => admin.done(),
            Binding::Server(Some(server)) => server.done(),
            Binding::MultiShard(servers, _state) => servers.iter().all(|s| s.done()),
            Binding::Replication(Some(server), _) => server.done(),
            _ => true,
        }
    }

    pub(super) fn state_check(&self, state: State) -> bool {
        match self {
            Binding::Server(Some(server)) => {
                debug!(
                    "server is in \"{}\" state [{}]",
                    server.stats().state,
                    server.addr()
                );
                server.stats().state == state
            }
            Binding::MultiShard(servers, _) => servers.iter().all(|s| {
                debug!("server is in \"{}\" state [{}]", s.stats().state, s.addr());
                s.stats().state == state
            }),
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

    pub(super) async fn link_client(&mut self, params: &Parameters) -> Result<usize, Error> {
        match self {
            Binding::Server(Some(ref mut server)) => server.link_client(params).await,
            Binding::MultiShard(ref mut servers, _) => {
                let mut max = 0;
                for server in servers {
                    let synced = server.link_client(params).await?;
                    if max < synced {
                        max = synced;
                    }
                }
                Ok(max)
            }
            Binding::Replication(Some(ref mut server), _) => server.link_client(params).await,

            _ => Ok(0),
        }
    }

    pub(super) fn changed_params(&mut self) -> Parameters {
        match self {
            Binding::Server(Some(ref mut server)) => server.changed_params().clone(),
            Binding::MultiShard(ref mut servers, _) => {
                if let Some(first) = servers.first() {
                    first.changed_params().clone()
                } else {
                    Parameters::default()
                }
            }
            Binding::Replication(Some(ref mut server), _) => server.changed_params().clone(),
            _ => Parameters::default(),
        }
    }
}
