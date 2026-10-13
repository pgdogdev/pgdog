//! Listening sockets.

use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::time::sleep;
use tracing::error;

const ACCEPT_ERROR_DELAY: Duration = Duration::from_secs(1);

/// Accept the next connection, making sure it never fails.
///
/// This is based on [axum approach](https://github.com/tokio-rs/axum/blob/e57b547eb95de74b73b34719dd5926254a085634/axum/src/serve/listener.rs#L245-L262).
/// In short: some errors are probably temporary and we should sleep or ignore them instead of failing
/// the whole server.
pub(crate) async fn accept(listener: &TcpListener) -> (TcpStream, SocketAddr) {
    loop {
        match listener.accept().await {
            Ok(connection) => return connection,
            Err(err) if is_connection_error(&err) => continue,
            Err(err) => {
                error!("accept error: {}", err);
                sleep(ACCEPT_ERROR_DELAY).await;
            }
        }
    }
}

fn is_connection_error(err: &Error) -> bool {
    matches!(
        err.kind(),
        ErrorKind::ConnectionRefused | ErrorKind::ConnectionAborted | ErrorKind::ConnectionReset
    )
}
