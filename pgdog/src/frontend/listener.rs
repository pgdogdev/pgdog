//! Connection listener. Handles all client connections.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::signal::ctrl_c;
use tokio::sync::Notify;
use tokio::time::timeout;
use tokio::{select, spawn};
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
#[derive(Debug, Clone)]
pub struct Listener {
    addr: String,
    clients: TaskTracker,
    shutdown: Arc<Notify>,
}

impl Listener {
    /// Create new client listener.
    pub fn new(addr: impl ToString) -> Self {
        Self {
            addr: addr.to_string(),
            clients: TaskTracker::new(),
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Listen for client connections and handle them.
    pub async fn listen(&mut self) -> Result<(), Error> {
        let listener = TcpListener::bind(&self.addr).await?;
        info!("🐕 PgDog listening on {}", self.addr);
        let comms = comms();

        loop {
            let comms = comms.clone();

            select! {
                connection = listener.accept() => {
                   let (stream, addr) = connection?;
                   let offline = comms.offline();

                   // Disable the Nagle algorithm.
                   stream.set_nodelay(true)?;

                   let future = async move {
                       match Self::handle_client(stream, addr, comms).await {
                           Ok(_) => (),
                           Err(err) => {
                               error!("client crashed: {:?}", err);
                           }
                       };
                   };

                   if offline {
                       spawn(future);
                   } else {
                       self.clients.spawn(future);
                   }
                }

                _ = ctrl_c() => {
                    self.clients.close();
                    comms.shutdown();
                    shutdown();

                    let listener = self.clone();
                    spawn(async move {
                        listener.shutdown().await;
                    });
                }

                _ = self.shutdown.notified() => {
                    break;
                }
            }
        }

        Ok(())
    }

    async fn shutdown(&self) {
        let shutdown_timeout = config().config.general.shutdown_timeout();

        info!(
            "waiting up to {:.3}s for clients to finish transactions",
            shutdown_timeout.as_secs_f64()
        );

        if timeout(shutdown_timeout, self.clients.wait())
            .await
            .is_err()
        {
            warn!(
                "terminating {} client connections due to shutdown timeout",
                self.clients.len()
            );
        }

        self.shutdown.notify_waiters();
    }

    async fn handle_client(stream: TcpStream, addr: SocketAddr, comms: Comms) -> Result<(), Error> {
        let mut stream = Stream::plain(stream);
        let tls = acceptor();

        loop {
            let startup = Startup::from_stream(&mut stream).await?;

            match startup {
                Startup::Ssl => {
                    if let Some(tls) = tls {
                        stream.send_flush(&SslReply::Yes).await?;
                        let plain = stream.take()?;
                        let cipher = tls.accept(plain).await?;
                        stream = Stream::tls(tokio_rustls::TlsStream::Server(cipher));
                    } else {
                        stream.send_flush(&SslReply::No).await?;
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
