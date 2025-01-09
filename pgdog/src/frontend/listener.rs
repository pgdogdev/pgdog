//! Connection listener. Handles all client connections.

use std::net::SocketAddr;

use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::signal::ctrl_c;
use tokio_util::task::TaskTracker;

use crate::backend::databases::databases;
use crate::net::messages::BackendKeyData;
use crate::net::messages::{hello::SslReply, Startup};
use crate::net::tls::acceptor;
use crate::net::Stream;

use tracing::{error, info};

use super::{Client, Comms, Error};

/// Client connections listener and handler.
#[derive(Debug)]
pub struct Listener {
    addr: String,
    clients: TaskTracker,
}

impl Listener {
    /// Create new client listener.
    pub fn new(addr: impl ToString) -> Self {
        Self {
            addr: addr.to_string(),
            clients: TaskTracker::new(),
        }
    }

    /// Listen for client connections and handle them.
    pub async fn listen(&mut self) -> Result<(), Error> {
        let listener = TcpListener::bind(&self.addr).await?;
        let comms = Comms::new();
        info!("🐕 pgDog listening on {}", self.addr);

        loop {
            let comms = comms.clone();
            select! {
                connection = listener.accept() => {
                   let (stream, addr) = connection?;

                   self.clients.spawn(async move {
                       match Self::handle_client(stream, addr, comms).await {
                           Ok(_) => (),
                           Err(err) => {
                               error!("client crashed: {:?}", err);
                           }
                       }
                   });
                }

                _ = ctrl_c() => {
                    self.clients.close();
                    comms.shutdown();
                    info!("Waiting for clients to finish transactions...");
                    self.clients.wait().await;
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_client(stream: TcpStream, addr: SocketAddr, comms: Comms) -> Result<(), Error> {
        let mut stream = Stream::plain(stream);
        let tls = acceptor()?;

        loop {
            let startup = Startup::from_stream(&mut stream).await?;

            match startup {
                Startup::Ssl => {
                    if let Some(ref tls) = tls {
                        stream.send_flush(SslReply::Yes).await?;
                        let plain = stream.take()?;
                        let cipher = tls.accept(plain).await?;
                        stream = Stream::tls(tokio_rustls::TlsStream::Server(cipher));
                    } else {
                        stream.send_flush(SslReply::No).await?;
                    }
                }

                Startup::Startup { params } => {
                    Client::spawn(stream, params, addr, comms).await?;
                    break;
                }

                Startup::Cancel { pid, secret } => {
                    let id = BackendKeyData { pid, secret };
                    if let Err(_) = databases().cancel(&id).await {}
                }
            }
        }

        Ok(())
    }
}
