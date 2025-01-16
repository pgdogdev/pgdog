//! Binding between frontend client and a connection on the backend.
use futures::stream::{FuturesUnordered, StreamExt};

use super::*;

/// The server(s) the client is connected to.
pub(super) enum Binding {
    Server(Option<Guard>),
    Admin(Backend),
    #[allow(dead_code)]
    MultiShard(Vec<Guard>, MultiShard),
}

impl Default for Binding {
    fn default() -> Self {
        Self::Server(None)
    }
}

impl Binding {
    pub(super) fn disconnect(&mut self) {
        match self {
            Self::Server(guard) => drop(guard.take()),
            Self::Admin(_) => (),
            Self::MultiShard(guards, _) => guards.clear(),
        }
    }

    pub(super) fn connected(&self) -> bool {
        match self {
            Self::Server(server) => server.is_some(),
            Self::MultiShard(servers, _) => !servers.is_empty(),
            Self::Admin(_) => true,
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
            Self::MultiShard(shards, _state) => {
                let mut futures = FuturesUnordered::from_iter(shards.iter_mut().map(|s| s.read()));
                if let Some(result) = futures.next().await {
                    result
                } else {
                    Err(Error::NotConnected)
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
        }
    }

    pub(super) fn done(&self) -> bool {
        match self {
            Self::Admin(_) => true,
            Self::Server(Some(server)) => server.done(),
            Self::MultiShard(servers, _state) => servers.iter().all(|s| s.done()),
            _ => true,
        }
    }
}
