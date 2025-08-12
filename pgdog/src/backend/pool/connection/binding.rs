//! Binding between frontend client and a connection on the backend.

use std::collections::HashMap;

use crate::{
    net::{parameter::Parameters, ProtocolMessage},
    state::State,
};

use super::*;

/// The server(s) the client is connected to.
#[derive(Debug)]
pub enum Binding {
    Server(Option<(usize, Guard)>),
    Admin(Backend),
    MultiShard(HashMap<usize, Guard>, MultiShard),
}

impl Default for Binding {
    fn default() -> Self {
        Binding::Server(None)
    }
}

impl Binding {
    /// Close all connections to all servers.
    pub fn disconnect(&mut self) {
        match self {
            Binding::Server(guard) => drop(guard.take()),
            Binding::Admin(_) => (),
            Binding::MultiShard(guards, _) => guards.clear(),
        }
    }

    /// Close connections and indicate to servers that
    /// they are probably broken and should not be re-used.
    pub fn force_close(&mut self) {
        match self {
            Binding::Server(Some(ref mut guard)) => guard.1.stats_mut().state(State::ForceClose),
            Binding::MultiShard(ref mut guards, _) => {
                for guard in guards.values_mut() {
                    guard.stats_mut().state(State::ForceClose);
                }
            }
            _ => (),
        }

        self.disconnect();
    }

    /// Are we connnected to a backend?
    pub fn connected(&self) -> bool {
        match self {
            Binding::Server(server) => server.is_some(),
            Binding::MultiShard(servers, _) => !servers.is_empty(),
            Binding::Admin(_) => true,
        }
    }

    pub(super) async fn read(&mut self) -> Result<Message, Error> {
        match self {
            Binding::Server(guard) => {
                if let Some(guard) = guard.as_mut() {
                    guard.1.read().await
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
                        for server in shards.values_mut() {
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
    pub async fn send(&mut self, request: &crate::frontend::ClientRequest) -> Result<(), Error> {
        match self {
            Binding::Server(server) => {
                if let Some(server) = server {
                    server.1.send(request).await
                } else {
                    Err(Error::NotConnected)
                }
            }

            Binding::Admin(backend) => Ok(backend.send(request).await?),
            Binding::MultiShard(servers, state) => {
                let route = request.route();
                match route.shard() {
                    Shard::All => {
                        state.set_state(servers.len(), route);

                        for server in servers.values_mut() {
                            server.send(request).await?;
                        }
                    }

                    Shard::Multi(shards) => {
                        let mut num_shards = 0;
                        for (number, server) in servers.values_mut().enumerate() {
                            if shards.contains(&number) {
                                server.send(request).await?;
                                num_shards += 1;
                            }
                        }
                        state.set_state(num_shards, route);
                    }

                    Shard::Direct(shard) => {
                        state.set_state(1, route);

                        if let Some(server) = servers.get_mut(shard) {
                            server.send(request).await?;
                        }
                    }
                }

                Ok(())
            }
        }
    }

    /// Send copy messages to shards they are destined to go.
    pub async fn send_copy(&mut self, rows: Vec<CopyRow>) -> Result<(), Error> {
        match self {
            Binding::MultiShard(servers, _state) => {
                for row in rows {
                    for (shard, server) in servers.values_mut().enumerate() {
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

            Binding::Server(Some(ref mut server)) => {
                for row in rows {
                    server
                        .1
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
            Binding::Server(Some(server)) => server.1.done(),
            Binding::MultiShard(servers, _state) => servers.values().all(|s| s.done()),
            _ => true,
        }
    }

    pub fn has_more_messages(&self) -> bool {
        match self {
            Binding::Admin(admin) => !admin.done(),
            Binding::Server(Some(server)) => server.1.has_more_messages(),
            Binding::MultiShard(servers, _state) => servers.values().any(|s| s.has_more_messages()),
            _ => false,
        }
    }

    pub(super) fn state_check(&self, state: State) -> bool {
        match self {
            Binding::Server(Some(server)) => {
                debug!(
                    "server is in \"{}\" state [{}]",
                    server.1.stats().state,
                    server.1.addr()
                );
                server.1.stats().state == state
            }
            Binding::MultiShard(servers, _) => servers.values().all(|s| {
                debug!("server is in \"{}\" state [{}]", s.stats().state, s.addr());
                s.stats().state == state
            }),
            _ => true,
        }
    }

    /// Execute a query on all servers.
    pub async fn execute(&mut self, query: &str) -> Result<(), Error> {
        match self {
            Binding::Server(Some(ref mut server)) => {
                server.1.execute(query).await?;
            }

            Binding::MultiShard(ref mut servers, _) => {
                for server in servers.values_mut() {
                    server.execute(query).await?;
                }
            }

            _ => (),
        }

        Ok(())
    }

    pub async fn link_client(&mut self, params: &Parameters) -> Result<usize, Error> {
        match self {
            Binding::Server(Some(ref mut server)) => server.1.link_client(params).await,
            Binding::MultiShard(ref mut servers, _) => {
                let mut max = 0;
                for server in servers.values_mut() {
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

    pub fn changed_params(&mut self) -> Parameters {
        match self {
            Binding::Server(Some(ref mut server)) => server.1.changed_params().clone(),
            Binding::MultiShard(ref mut servers, _) => {
                if let Some(first) = servers.values().next() {
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
            Binding::Server(Some(ref mut server)) => server.1.mark_dirty(true),
            Binding::MultiShard(ref mut servers, _state) => {
                servers.values_mut().for_each(|s| s.mark_dirty(true))
            }
            _ => (),
        }
    }

    #[cfg(test)]
    pub fn is_dirty(&self) -> bool {
        match self {
            Binding::Server(Some(ref server)) => server.1.dirty(),
            Binding::MultiShard(ref servers, _state) => servers.values().any(|s| s.dirty()),
            _ => false,
        }
    }

    pub fn copy_mode(&self) -> bool {
        match self {
            Binding::Admin(_) => false,
            Binding::MultiShard(ref servers, _state) => servers.values().all(|s| s.copy_mode()),
            Binding::Server(Some(ref server)) => server.1.copy_mode(),
            _ => false,
        }
    }
}
