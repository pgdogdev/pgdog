//! Connection listener. Handles all client connections.

use std::net::SocketAddr;
use std::sync::Arc;

use crate::backend::databases::{databases, reload, shutdown};
use crate::config::config;
use crate::net::messages::BackendKeyData;
use crate::net::messages::{hello::SslReply, Startup};
use crate::net::tls::acceptor;
use crate::net::{tweak, Stream};
use crate::sighup::Sighup;
use tokio::net::{TcpListener, TcpStream};
use tokio::signal::ctrl_c;
use tokio::sync::Notify;
use tokio::time::timeout;
use tokio::{select, spawn};

use tracing::{error, info, warn};

use super::{
    comms::{comms, Comms},
    Client, Error,
};

/// Client connections listener and handler.
#[derive(Debug, Clone)]
pub struct Listener {
    addr: String,
    shutdown: Arc<Notify>,
}

impl Listener {
    /// Create new client listener.
    pub fn new(addr: impl ToString) -> Self {
        Self {
            addr: addr.to_string(),
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Listen for client connections and handle them.
    pub async fn listen(&mut self) -> Result<(), Error> {
        info!("🐕 PgDog listening on {}", self.addr);
        let listener = TcpListener::bind(&self.addr).await?;
        let comms = comms();
        let shutdown_signal = comms.shutting_down();
        let mut sighup = Sighup::new()?;

        loop {
            let comms = comms.clone();

            select! {
                connection = listener.accept() => {
                   let (stream, addr) = connection?;
                   let offline = comms.offline();

                   let client_comms = comms.clone();
                   let future = async move {
                       match Self::handle_client(stream, addr, client_comms).await {
                           Ok(_) => (),
                           Err(err) => if !err.disconnect() {
                               error!("client crashed: {:?}", err);
                           } else {
                               info!("client disconnected [{}]", addr);
                           }
                       };
                   };

                   if offline {
                       spawn(future);
                   } else {
                       comms.tracker().spawn(future);
                   }
                }

                _ = shutdown_signal.notified() => {
                    self.start_shutdown();
                }

                _ = ctrl_c() => {
                    self.start_shutdown();
                }

                _ = sighup.listen() => {
                    if let Err(err) = reload() {
                        error!("configuration reload error: {}", err);
                    }
                }

                _ = self.shutdown.notified() => {
                    break;
                }
            }
        }

        Ok(())
    }

    /// Shutdown this listener.
    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }

    fn start_shutdown(&self) {
        shutdown();
        comms().shutdown();

        let listener = self.clone();
        spawn(async move {
            listener.execute_shutdown().await;
        });
    }

    async fn execute_shutdown(&self) {
        let shutdown_timeout = config().config.general.shutdown_timeout();

        info!(
            "waiting up to {:.3}s for clients to finish transactions",
            shutdown_timeout.as_secs_f64()
        );

        let comms = comms();

        if timeout(shutdown_timeout, comms.tracker().wait())
            .await
            .is_err()
        {
            warn!(
                "terminating {} client connections due to shutdown timeout",
                comms.tracker().len()
            );
        }

        self.shutdown.notify_waiters();
    }

    async fn handle_client(stream: TcpStream, addr: SocketAddr, comms: Comms) -> Result<(), Error> {
        tweak(&stream)?;

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
