//! TLS configuration.

use once_cell::sync::OnceCell;
use tokio::fs::read_to_string;
use tokio_native_tls::{
    native_tls::{Identity, TlsAcceptor, TlsConnector},
    TlsAcceptor as TlsAcceptorAsync, TlsConnector as TlsConnectorAsync,
};
use tracing::info;

use super::Error;

static ACCEPTOR: OnceCell<TlsAcceptorAsync> = OnceCell::new();
static CONNECTOR: OnceCell<TlsConnectorAsync> = OnceCell::new();

/// Create a new TLS acceptor from the cert and key.
pub async fn acceptor() -> Result<Option<TlsAcceptorAsync>, Error> {
    if let Some(acceptor) = ACCEPTOR.get() {
        return Ok(Some(acceptor.clone()));
    }

    let pem = read_to_string("tests/cert.pem").await?;
    let key = read_to_string("tests/key.pem").await?;

    let identity = Identity::from_pkcs8(pem.as_bytes(), key.as_bytes()).unwrap();
    let acceptor = TlsAcceptor::new(identity).unwrap();

    info!("🔑 TLS on");

    let acceptor = TlsAcceptorAsync::from(acceptor);

    // A bit of a race, but it's not a big deal unless this is called
    // with different certificate/secret key.
    let _ = ACCEPTOR.set(acceptor.clone());

    Ok(Some(acceptor))
}

/// Create new TLS connector.
pub fn connector() -> Result<TlsConnectorAsync, Error> {
    if let Some(connector) = CONNECTOR.get() {
        return Ok(connector.clone());
    }
    let connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .build()
        .unwrap();
    let connector = TlsConnectorAsync::from(connector);

    let _ = CONNECTOR.set(connector.clone());

    Ok(connector)
}
