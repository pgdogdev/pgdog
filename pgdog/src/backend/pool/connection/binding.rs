//! Binding between frontend client and a connection on the backend.

use crate::{
    frontend::ClientRequest,
    net::{parameter::Parameters, ProtocolMessage},
    state::State,
};

use super::*;

/// The server(s) the client is connected to.
#[derive(Debug)]
pub(crate) enum Binding {
    /// Direct-to-shard transaction.
    Direct(Option<Guard>),
    /// Admin database connection.
    Admin(AdminServer),
    /// Multi-shard transaction.
    MultiShard(Vec<Guard>, Box<MultiShard>),
}

impl Default for Binding {
    fn default() -> Self {
        Binding::Direct(None)
    }
}

impl Binding {
    /// Close all connections to all servers.
    pub(crate) fn disconnect(&mut self) {
        match self {
            Binding::Direct(guard) => drop(guard.take()),
            Binding::Admin(_) => (),
            Binding::MultiShard(guards, _) => guards.clear(),
        }
    }

    /// Close connections and indicate to servers that
    /// they are probably broken and should not be re-used.
    pub(crate) fn force_close(&mut self) {
        match self {
            Binding::Direct(Some(ref mut guard)) => guard.stats_mut().state(State::ForceClose),
            Binding::MultiShard(ref mut guards, _) => {
                for guard in guards {
                    guard.stats_mut().state(State::ForceClose);
                }
            }
            _ => (),
        }

        self.disconnect();
    }

    /// Are we connnected to a backend?
    pub(crate) fn connected(&self) -> bool {
        match self {
            Binding::Direct(server) => server.is_some(),
            Binding::MultiShard(servers, _) => !servers.is_empty(),
            Binding::Admin(_) => true,
        }
    }

    pub(super) async fn read(&mut self) -> Result<Message, Error> {
        match self {
            Binding::Direct(guard) => {
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
        }
    }

    /// Send an entire buffer of messages to the servers(s).
    pub(crate) async fn send(&mut self, client_request: &ClientRequest) -> Result<(), Error> {
        match self {
            Binding::Admin(backend) => Ok(backend.send(client_request).await?),

            Binding::Direct(server) => {
                if let Some(server) = server {
                    server.send(client_request).await
                } else {
                    Err(Error::NotConnected)
                }
            }

            Binding::MultiShard(servers, state) => {
                let mut shards_sent = servers.len();
                for (shard, server) in servers.iter_mut().enumerate() {
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
                        server.send(client_request).await?;
                    }
                }

                state.update(shards_sent, client_request.route());

                Ok(())
            }
        }
    }

    /// Send copy messages to shards they are destined to go.
    pub(crate) async fn send_copy(&mut self, rows: Vec<CopyRow>) -> Result<(), Error> {
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

            Binding::Direct(Some(ref mut server)) => {
                for row in rows {
                    server
                        .send_one(&ProtocolMessage::from(row.message()))
                        .await?;
                }

                Ok(())
            }

            _ => Err(Error::CopyNotConnected),
        }
    }

    pub(super) fn done(&self) -> bool {
        match self {
            Binding::Admin(admin) => admin.done(),
            Binding::Direct(Some(server)) => server.done(),
            Binding::MultiShard(servers, _state) => servers.iter().all(|s| s.done()),
            _ => true,
        }
    }

    pub(crate) fn has_more_messages(&self) -> bool {
        match self {
            Binding::Admin(admin) => !admin.done(),
            Binding::Direct(Some(server)) => server.has_more_messages(),
            Binding::MultiShard(servers, _state) => servers.iter().any(|s| s.has_more_messages()),
            _ => false,
        }
    }

    pub(super) fn state_check(&self, state: State) -> bool {
        match self {
            Binding::Direct(Some(server)) => {
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
    pub(crate) async fn execute(&mut self, query: &str) -> Result<(), Error> {
        match self {
            Binding::Direct(Some(ref mut server)) => {
                server.execute(query).await?;
            }

            Binding::MultiShard(ref mut servers, _) => {
                for server in servers {
                    server.execute(query).await?;
                }
            }

            _ => (),
        }

        Ok(())
    }

    pub(crate) async fn link_client(&mut self, params: &Parameters) -> Result<usize, Error> {
        match self {
            Binding::Direct(Some(ref mut server)) => server.link_client(params).await,
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

            _ => Ok(0),
        }
    }

    pub(crate) fn changed_params(&mut self) -> Parameters {
        match self {
            Binding::Direct(Some(ref mut server)) => server.changed_params().clone(),
            Binding::MultiShard(ref mut servers, _) => {
                if let Some(first) = servers.first() {
                    first.changed_params().clone()
                } else {
                    Parameters::default()
                }
            }
            _ => Parameters::default(),
        }
    }

    pub(super) fn dirty(&mut self) {
        match self {
            Binding::Direct(Some(ref mut server)) => server.mark_dirty(true),
            Binding::MultiShard(ref mut servers, _state) => {
                servers.iter_mut().for_each(|s| s.mark_dirty(true))
            }
            _ => (),
        }
    }

    #[cfg(test)]
    pub(crate) fn is_dirty(&self) -> bool {
        match self {
            Binding::Direct(Some(ref server)) => server.dirty(),
            Binding::MultiShard(ref servers, _state) => servers.iter().any(|s| s.dirty()),
            _ => false,
        }
    }

    pub(crate) fn copy_mode(&self) -> bool {
        match self {
            Binding::Admin(_) => false,
            Binding::MultiShard(ref servers, _state) => servers.iter().all(|s| s.copy_mode()),
            Binding::Direct(Some(ref server)) => server.copy_mode(),
            _ => false,
        }
    }
}
