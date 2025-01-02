//! Connection listener. Handles all client connections.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::signal::ctrl_c;

use crate::net::messages::BackendKeyData;
use crate::net::messages::{hello::SslReply, Startup};
use crate::net::tls::acceptor;
use crate::net::Stream;

use tracing::{error, info};

use super::{Client, Error};

/// Connected clients.
type Clients = Arc<Mutex<HashMap<BackendKeyData, ()>>>;

/// Client connections listener and handler.
#[derive(Debug)]
pub struct Listener {
    addr: String,
    clients: Clients,
}

impl Listener {
    /// Create new client listener.
    pub fn new(addr: impl ToString) -> Self {
        Self {
            addr: addr.to_string(),
            clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Listen for client connections and handle them.
    pub async fn listen(&mut self) -> Result<(), Error> {
        info!("🐕 pgDog listening on {}", self.addr);

        let listener = TcpListener::bind(&self.addr).await?;

        // Load TLS cert, if any.
        let _ = acceptor().await?;

        loop {
            select! {
                connection = listener.accept() => {
                   let (stream, addr) = connection?;
                   let clients = self.clients.clone();

                   tokio::spawn(async move {
                       Self::handle_client(stream, addr, clients).await?;
                       Ok::<(), Error>(())
                   });
                }

                _ = ctrl_c() => {
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_client(
        stream: TcpStream,
        addr: SocketAddr,
        clients: Clients,
    ) -> Result<(), Error> {
        info!("client connected [{}]", addr);

        let mut stream = Stream::plain(stream);
        let tls = acceptor().await?;

        loop {
            let startup = Startup::from_stream(&mut stream).await?;

            match startup {
                Startup::Ssl => {
                    if let Some(ref tls) = tls {
                        stream.send_flush(SslReply::Yes).await?;
                        let plain = stream.take()?;
                        let cipher = tls.accept(plain).await?;
                        stream = Stream::tls(cipher);
                    } else {
                        stream.send_flush(SslReply::No).await?;
                    }
                }

                Startup::Startup { params } => {
                    let client = Client::new(stream, params).await?;
                    let id = client.id();

                    clients.lock().insert(id, ());

                    match client.spawn().await {
                        Ok(_) => info!("client disconnected [{}]", addr),
                        Err(err) => error!("client disconnected with error [{}]: {:?}", addr, err),
                    }

                    clients.lock().remove(&id);
                    break;
                }

                Startup::Cancel { pid, secret } => (),
            }
        }

        Ok(())
    }
}
