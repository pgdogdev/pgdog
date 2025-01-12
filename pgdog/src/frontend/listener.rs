//! Connection listener. Handles all client connections.

use std::net::SocketAddr;

use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::signal::ctrl_c;
use tokio::time::timeout;
use tokio_util::task::TaskTracker;

use crate::backend::databases::{databases, shutdown};
use crate::config::config;
use crate::net::messages::BackendKeyData;
use crate::net::messages::{hello::SslReply, Startup};
use crate::net::tls::acceptor;
use crate::net::Stream;

use tracing::{error, info, warn};

use super::{
    comms::{comms, Comms},
    Client, Error,
};

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
        let comms = comms();
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
                       };
                   });
                }

                _ = ctrl_c() => {
                    self.clients.close();
                    comms.shutdown();
                    shutdown();
                    break;
                }
            }
        }

        // Close the listener before
        // we wait for clients to shut down.
        //
        // TODO: allow admin connections here anyway
        // to debug clients refusing to shut down.
        drop(listener);

        let shutdown_timeout = config().config.general.shutdown_timeout();
        info!(
            "waiting up to {:.3}s for clients to finish transactions",
            shutdown_timeout.as_secs_f64()
        );
        if let Err(_) = timeout(shutdown_timeout, self.clients.wait()).await {
            warn!(
                "terminating {} client connections due to shutdown timeout",
                self.clients.len()
            );
        }

        Ok(())
    }

    async fn handle_client(stream: TcpStream, addr: SocketAddr, comms: Comms) -> Result<(), Error> {
        let mut stream = Stream::plain(stream);
        let tls = acceptor();

        loop {
            let startup = Startup::from_stream(&mut stream).await?;

            match startup {
                Startup::Ssl => {
                    if let Some(tls) = tls {
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
                    let _ = databases().cancel(&id).await;
                    break;
                }
            }
        }

        Ok(())
    }
}
