//! Connection listener. Handles all client connections.

use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::backend::databases::{databases, reload, shutdown};
use crate::backend::fdw::PostgresLauncher;
use crate::config::config;
use crate::frontend::client::query_engine::two_pc::Manager;
use crate::net::messages::BackendKeyData;
use crate::net::messages::{hello::SslReply, Startup};
use crate::net::{self, tls::acceptor};
use crate::net::{tweak, Stream};
use crate::sighup::Sighup;
use tokio::net::{TcpListener, TcpStream};
use tokio::signal::ctrl_c;
use tokio::sync::Notify;
use tokio::time::timeout;
use tokio::{select, spawn};

use tracing::{error, info, warn};

use super::{comms::comms, Client, Error};

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
        let shutdown_signal = comms().shutting_down();
        let mut sighup = Sighup::new()?;

        loop {
            select! {
                connection = listener.accept() => {
                   let comms = comms();
                   let (stream, addr) = connection?;
                   let offline = comms.offline();

                   let future = async move {
                       match Self::handle_client(stream, addr).await {
                           Ok(_) => (),
                           Err(err) => if !err.disconnect() {
                               error!("client crashed: {:?}", err);
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
        comms().shutdown();

        let listener = self.clone();
        spawn(async move {
            listener.execute_shutdown().await;
            Manager::get().shutdown().await; // wait for 2pc to flush
            shutdown();
        });
    }

    async fn execute_shutdown(&self) {
        let shutdown_timeout = config().config.general.shutdown_timeout();

        info!(
            "waiting up to {:.3}s for {} clients to finish transactions",
            shutdown_timeout.as_secs_f64(),
            comms().tracker().len(),
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

            // If a shutdown termination timeout is configured, enforce it here.
            // This will ensure that we don't wait indefinitely for databases to respond.
            if let Some(termination_timeout) =
                config().config.general.shutdown_termination_timeout()
            {
                // Shutdown timeout elapsed; cancel any still-running queries before tearing pools down.
                let cancel_futures = comms.clients().into_keys().map(|id| async move {
                    if let Err(err) = databases().cancel(&id).await {
                        error!(?id, "cancel request failed during shutdown: {err}");
                    }
                });
                let cancel_all = futures::future::join_all(cancel_futures);

                if timeout(termination_timeout, cancel_all).await.is_err() {
                    error!(
                        "forced shutdown: abandoning {} outstanding cancel requests after waiting {:.3}s" ,
                        comms.clients().len(),
                        termination_timeout.as_secs_f64()
                    );
                }
            }
        }

        if let Err(_) = timeout(shutdown_timeout, PostgresLauncher::get().shutdown_wait()).await {
            error!("[fdw] graceful shutdown failed");
        }

        self.shutdown.notify_waiters();
    }

    async fn handle_client(stream: TcpStream, addr: SocketAddr) -> Result<(), Error> {
        tweak(&stream)?;
        let config = config();

        let mut stream = Stream::plain(stream, config.config.memory.net_buffer);

        let tls = acceptor();

        loop {
            let startup = match Startup::from_stream(&mut stream).await {
                Ok(startup) => startup,
                Err(net::Error::Io(io_err)) => {
                    // Load balancers like AWS ELB use TCP to health check
                    // targets and abruptly disconnect.
                    if io_err.kind() == ErrorKind::ConnectionReset {
                        return Ok(());
                    } else {
                        return Err(net::Error::Io(io_err).into());
                    }
                }
                Err(err) => return Err(err.into()),
            };

            match startup {
                Startup::Ssl => {
                    if let Some(tls) = tls.as_ref() {
                        stream.send_flush(&SslReply::Yes).await?;
                        let plain = stream.take()?;
                        let cipher = tls.accept(plain).await?;
                        stream = Stream::tls(
                            tokio_rustls::TlsStream::Server(cipher),
                            config.config.memory.net_buffer,
                        );
                    } else {
                        stream.send_flush(&SslReply::No).await?;
                    }
                }

                Startup::GssEnc => {
                    // GSS encryption is not yet supported; reject and wait for a normal startup.
                    stream.send_flush(&SslReply::No).await?;
                }

                Startup::Startup { params } => {
                    Client::spawn(stream, params, addr, config).await?;
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
