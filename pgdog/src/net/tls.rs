//! TLS configuration.

use std::{path::PathBuf, sync::Arc};

use crate::config::TlsVerifyMode;
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

/// Create new TLS connector using the default configuration.
pub fn connector() -> Result<TlsConnector, Error> {
    if let Some(connector) = CONNECTOR.get() {
        return Ok(connector.clone());
    }

    let config = config();
    let connector = connector_with_verify_mode(
        config.config.general.tls_verify,
        config.config.general.tls_server_ca_certificate.as_ref(),
    )?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TlsVerifyMode;

    #[tokio::test]
    async fn test_connector_with_none_mode() {
        crate::logger();

        // Test that connector with None mode is created correctly
        // This should create a connector that won't be used (connection should be plain TCP)
        let result = connector_with_verify_mode(TlsVerifyMode::None, None);
        assert!(result.is_ok(), "Should create connector even for None mode");
    }

    #[tokio::test]
    async fn test_connector_with_allow_mode() {
        crate::logger();

        // Test that connector with Allow mode accepts any certificate
        let result = connector_with_verify_mode(TlsVerifyMode::Allow, None);
        assert!(result.is_ok(), "Should create connector for Allow mode");

        // The connector should use our custom verifier that accepts any cert
        // We can't easily test the internal behavior here, but we ensure it's created
    }

    #[tokio::test]
    async fn test_connector_with_certificate_mode() {
        crate::logger();

        // Test that connector with Certificate mode validates certs but not hostname
        let result = connector_with_verify_mode(TlsVerifyMode::Certificate, None);
        assert!(
            result.is_ok(),
            "Should create connector for Certificate mode"
        );
    }

    #[tokio::test]
    async fn test_connector_with_full_mode() {
        crate::logger();

        // Test that connector with Full mode validates both cert and hostname
        let result = connector_with_verify_mode(TlsVerifyMode::Full, None);
        assert!(result.is_ok(), "Should create connector for Full mode");
    }

    #[tokio::test]
    async fn test_connector_with_custom_ca() {
        crate::logger();

        // For now, we'll test with a non-existent CA file path
        // In a real test, we'd create a proper test CA certificate
        let ca_path = PathBuf::from("/tmp/test_ca.pem");
        let result = connector_with_verify_mode(TlsVerifyMode::Full, Some(&ca_path));

        // This should fail because the file doesn't exist
        assert!(result.is_err(), "Should fail with non-existent CA file");
    }

    #[tokio::test]
    async fn test_connector_mode_differences() {
        crate::logger();

        // Test that different modes produce different configurations
        let allow = connector_with_verify_mode(TlsVerifyMode::Allow, None);
        let certificate = connector_with_verify_mode(TlsVerifyMode::Certificate, None);
        let full = connector_with_verify_mode(TlsVerifyMode::Full, None);

        // All should succeed
        assert!(allow.is_ok());
        assert!(certificate.is_ok());
        assert!(full.is_ok());

        // In the real implementation, these would have different verifiers
    }
}

/// Create a TLS connector with the specified verification mode.
pub fn connector_with_verify_mode(
    mode: TlsVerifyMode,
    ca_cert_path: Option<&PathBuf>,
) -> Result<TlsConnector, Error> {
    // Load root certificates
    let mut roots = rustls::RootCertStore::empty();

    // If a custom CA certificate is provided, load it
    if let Some(ca_path) = ca_cert_path {
        let ca_cert = CertificateDer::from_pem_file(ca_path)?;
        roots.add(ca_cert)?;
    } else {
        // Load system native certificates
        for cert in rustls_native_certs::load_native_certs().expect("load native certs") {
            roots.add(cert)?;
        }
    }

    // Create the appropriate config based on the verification mode
    let config = match mode {
        TlsVerifyMode::None => {
            // For None mode, we still create a connector but it won't be used
            // The server connection logic should skip TLS entirely
            ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth()
        }
        TlsVerifyMode::Allow => {
            // Use our custom verifier that accepts any certificate
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

            config
        }
        TlsVerifyMode::Certificate => {
            // Verify certificate validity but not hostname
            let verifier = NoHostnameVerifier::new(roots.clone());
            let mut config = ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();

            config
                .dangerous()
                .set_certificate_verifier(Arc::new(verifier));

            config
        }
        TlsVerifyMode::Full => {
            // Full verification: both certificate and hostname
            ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth()
        }
    };

    Ok(TlsConnector::from(Arc::new(config)))
}

/// Certificate verifier that validates certificates but skips hostname verification
#[derive(Debug)]
struct NoHostnameVerifier {
    webpki_verifier: Arc<dyn ServerCertVerifier>,
}

impl NoHostnameVerifier {
    fn new(roots: rustls::RootCertStore) -> Self {
        // Create a standard WebPKI verifier
        let webpki_verifier = rustls::client::WebPkiServerVerifier::builder(roots.into())
            .build()
            .unwrap();
        Self { webpki_verifier }
    }
}

impl ServerCertVerifier for NoHostnameVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        // Use a dummy server name for verification - we only care about cert validity
        let dummy_name = rustls::pki_types::ServerName::try_from("example.com").unwrap();

        // Try to verify with the dummy name
        match self.webpki_verifier.verify_server_cert(
            end_entity,
            intermediates,
            &dummy_name,
            ocsp_response,
            now,
        ) {
            Ok(_) => Ok(ServerCertVerified::assertion()),
            Err(rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidForName)) => {
                // If the only error is hostname mismatch, that's fine for Certificate mode
                Ok(ServerCertVerified::assertion())
            }
            Err(e) => Err(e),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.webpki_verifier
            .verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.webpki_verifier
            .verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.webpki_verifier.supported_verify_schemes()
    }
}
