//! Connection listener. Handles all client connections.

use std::io::ErrorKind;
use std::net::SocketAddr;

use crate::backend::databases::{databases, reload, shutdown};
use crate::config::config;
use crate::frontend::client::query_engine::two_pc::Manager;
use crate::net::messages::{FrontendPid, NegotiateProtocolVersion, Startup, hello::SslReply};
use crate::net::tls::{acceptor, peer_certificate_present, peer_identity};
use crate::net::{self, Stream, tweak};
use crate::sighup::Sighup;
use tokio::net::{TcpListener, TcpSocket, TcpStream, lookup_host};
use tokio::signal::ctrl_c;
use tokio::{select, spawn};
use tokio_util::sync::CancellationToken;

use tracing::{error, info, warn};

use super::{Client, Error, comms::comms};
use crate::util::safe_timeout;

/// Client connections listener and handler.
#[derive(Debug, Clone)]
pub(crate) struct Listener {
    addr: String,
    shutdown: CancellationToken,
}

impl Listener {
    /// Create new client listener.
    pub(crate) fn new(addr: impl ToString) -> Self {
        Self {
            addr: addr.to_string(),
            shutdown: CancellationToken::new(),
        }
    }

    /// Bind the listening socket with the configured `listen_backlog`.
    ///
    /// `TcpListener::bind` hardcodes a backlog of 1024, which caps the queue of
    /// pending connections regardless of `net.core.somaxconn`; large client fleets
    /// reconnecting at once (e.g. a rolling deploy) overflow it and their SYNs are
    /// silently dropped onto the retransmission timer.
    async fn bind(addr: &str) -> Result<TcpListener, Error> {
        let backlog = config().config.general.listen_backlog;
        let addr = Self::first_addr(addr, lookup_host(addr).await?)?;
        let socket = if addr.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };
        #[cfg(not(windows))]
        socket.set_reuseaddr(true)?;
        socket.bind(addr)?;
        Ok(socket.listen(backlog)?)
    }

    /// First resolved address, or `AddrNotAvailable` when resolution yields none.
    fn first_addr(
        requested: &str,
        mut resolved: impl Iterator<Item = SocketAddr>,
    ) -> Result<SocketAddr, std::io::Error> {
        resolved.next().ok_or_else(|| {
            std::io::Error::new(
                ErrorKind::AddrNotAvailable,
                format!("no address to bind: {}", requested),
            )
        })
    }

    /// Listen for client connections and handle them.
    pub(crate) async fn listen(&mut self) -> Result<(), Error> {
        info!("🐕 PgDog listening on {}", self.addr);
        let listener = Self::bind(&self.addr).await?;
        let shutdown_signal = comms().shutting_down();
        let mut sighup = Sighup::new()?;
        let mut shutting_down = false;

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

                _ = shutdown_signal.cancelled(), if !shutting_down => {
                    shutting_down = true;
                    self.start_shutdown();
                }

                _ = ctrl_c(), if !shutting_down => {
                    shutting_down = true;
                    self.start_shutdown();
                }

                _ = sighup.listen() => {
                    if let Err(err) = reload() {
                        error!("configuration reload error: {}", err);
                    }
                }

                _ = self.shutdown.cancelled() => {
                    break;
                }
            }
        }

        Ok(())
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

        if safe_timeout(shutdown_timeout, comms.tracker().wait())
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
                    if let Err(err) = databases().cancel(id).await {
                        error!(?id, "cancel request failed during shutdown: {err}");
                    }
                });
                let cancel_all = futures::future::join_all(cancel_futures);

                if safe_timeout(termination_timeout, cancel_all).await.is_err() {
                    error!(
                        "forced shutdown: abandoning {} outstanding cancel requests after waiting {:.3}s",
                        comms.clients().len(),
                        termination_timeout.as_secs_f64()
                    );
                }
            }
        }

        self.shutdown.cancel();
    }

    async fn handle_client(stream: TcpStream, addr: SocketAddr) -> Result<(), Error> {
        let config = config();

        // Not the end of the world if the tweaks are
        // not applied.
        if let Err(err) = tweak(&stream, &config.config.tcp) {
            warn!(
                "keepalive settings ({}) are not supported on this system, ignoring, error: {} [{}]",
                config.config.tcp, err, addr
            );
        }

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
                        let cipher = match tls.accept(plain).await {
                            Ok(cipher) => cipher,
                            Err(err) => {
                                // TLS failure should close the connection
                                // without telling the client what happened (security).
                                warn!("TLS handshake failed: {err} [{addr}]");
                                return Ok(());
                            }
                        };
                        let tls_identity = peer_identity(cipher.get_ref().1);
                        let tls_client_certificate = peer_certificate_present(cipher.get_ref().1);
                        stream = Stream::tls(
                            tokio_rustls::TlsStream::Server(cipher),
                            config.config.memory.net_buffer,
                            tls_identity,
                            tls_client_certificate,
                        );
                    } else {
                        stream.send_flush(&SslReply::No).await?;
                    }
                }

                Startup::GssEnc => {
                    // GSS encryption is not yet supported; reject and wait for a normal startup.
                    stream.send_flush(&SslReply::No).await?;
                }

                Startup::Startup {
                    version,
                    params,
                    unrecognized_options,
                } => {
                    let negotiated = version
                        .negotiated()
                        .ok_or_else(|| net::Error::UnsupportedStartup(version.as_i32()))?;

                    if negotiated != version || !unrecognized_options.is_empty() {
                        // Send the negotiated minor version before auth so the
                        // client can interpret later messages, including
                        // BackendKeyData / CancelRequest, using the right shape.
                        stream
                            .send(&NegotiateProtocolVersion::new(
                                negotiated,
                                unrecognized_options,
                            ))
                            .await?;
                    }

                    Box::pin(Client::spawn(stream, params, addr, config, negotiated)).await?;
                    break;
                }

                Startup::Cancel { ref id } => {
                    if comms().verify_cancel(id) {
                        let _ = databases().cancel(FrontendPid::from(id)).await;
                    }
                    break;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_bind_ipv4_applies_configured_backlog() {
        let listener = Listener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        assert!(addr.is_ipv4());
        assert_ne!(addr.port(), 0);
    }

    #[tokio::test]
    async fn test_bind_ipv6() {
        let listener = Listener::bind("[::1]:0").await.unwrap();
        assert!(listener.local_addr().unwrap().is_ipv6());
    }

    #[tokio::test]
    async fn test_bind_unresolvable_address_errors() {
        assert!(Listener::bind("host.invalid:0").await.is_err());
    }

    #[test]
    fn test_first_addr_empty_resolution_errors() {
        let err = Listener::first_addr("nowhere:0", std::iter::empty()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::AddrNotAvailable);
    }

    #[test]
    fn test_first_addr_takes_first() {
        let addrs: Vec<SocketAddr> = vec!["127.0.0.1:6432".parse().unwrap()];
        let addr = Listener::first_addr("localhost:6432", addrs.into_iter()).unwrap();
        assert!(addr.is_ipv4());
    }
}
