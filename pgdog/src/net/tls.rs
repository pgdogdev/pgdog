//! TLS configuration.

use std::{path::PathBuf, sync::Arc};

use once_cell::sync::OnceCell;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{
    self,
    client::danger::{ServerCertVerified, ServerCertVerifier},
    pki_types::pem::PemObject,
    server::danger::ClientCertVerifier,
    ClientConfig,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::info;

use crate::config::config;

use super::Error;

static ACCEPTOR: OnceCell<Option<TlsAcceptor>> = OnceCell::new();
static CONNECTOR: OnceCell<TlsConnector> = OnceCell::new();

/// Get preloaded TLS acceptor.
pub fn acceptor() -> Option<&'static TlsAcceptor> {
    if let Some(Some(acceptor)) = ACCEPTOR.get() {
        return Some(acceptor);
    }

    None
}

/// Create a new TLS acceptor from the cert and key.
///
/// This is not atomic, so call it on startup only.
pub fn load_acceptor(cert: &PathBuf, key: &PathBuf) -> Result<Option<TlsAcceptor>, Error> {
    if let Some(acceptor) = ACCEPTOR.get() {
        return Ok(acceptor.clone());
    }

    let pem = if let Ok(pem) = CertificateDer::from_pem_file(cert) {
        pem
    } else {
        let _ = ACCEPTOR.set(None);
        return Ok(None);
    };

    let key = if let Ok(key) = PrivateKeyDer::from_pem_file(key) {
        key
    } else {
        let _ = ACCEPTOR.set(None);
        return Ok(None);
    };

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![pem], key)?;

    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));

    info!("🔑 TLS on");

    // A bit of a race, but it's not a big deal unless this is called
    // with different certificate/secret key.
    let _ = ACCEPTOR.set(Some(acceptor.clone()));

    Ok(Some(acceptor))
}

/// Create new TLS connector.
pub fn connector() -> Result<TlsConnector, Error> {
    if let Some(connector) = CONNECTOR.get() {
        return Ok(connector.clone());
    }

    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().expect("load native certs") {
        roots.add(cert)?;
    }

    let verifier = rustls::server::WebPkiClientVerifier::builder(roots.clone().into())
        .build()
        .unwrap();
    let verifier = CertificateVerifyer { verifier };

    let mut config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    config
        .dangerous()
        .set_certificate_verifier(Arc::new(verifier));

    let connector = TlsConnector::from(Arc::new(config));

    let _ = CONNECTOR.set(connector.clone());

    Ok(connector)
}

/// Preload TLS at startup.
pub fn load() -> Result<(), Error> {
    let config = config();

    if let Some((cert, key)) = config.config.general.tls() {
        load_acceptor(cert, key)?;
    }

    connector()?;

    Ok(())
}

#[derive(Debug)]
struct CertificateVerifyer {
    verifier: Arc<dyn ClientCertVerifier>,
}

impl ServerCertVerifier for CertificateVerifyer {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        // Accept self-signed certs or certs signed by any CA.
        // Doesn't protect against MITM attacks.
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.verifier.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.verifier.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.verifier.supported_verify_schemes()
    }
}
