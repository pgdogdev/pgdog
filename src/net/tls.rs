//! TLS configuration.

use tokio::fs::read_to_string;
use tokio_native_tls::native_tls::{Identity, TlsAcceptor};
use tracing::info;

use super::Error;

pub async fn acceptor() -> Result<TlsAcceptor, Error> {
    let pem = read_to_string("tests/cert.pem").await?;
    let key = read_to_string("tests/key.pem").await?;

    let identity = Identity::from_pkcs8(pem.as_bytes(), key.as_bytes()).unwrap();
    let acceptor = TlsAcceptor::new(identity).unwrap();

    info!("🔑 TLS on");

    Ok(acceptor)
}
