//! Binding between frontend client and a connection on the backend.

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
            Self::MultiShard(shards, state) => {
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

                        let pending = shards.iter_mut().filter(|s| !s.done());
                        let mut read = false;

                        for shard in pending {
                            let message = shard.read().await?;
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
                        sleep(Duration::MAX).await;
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
