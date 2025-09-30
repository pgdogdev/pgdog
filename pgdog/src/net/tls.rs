//! TLS configuration.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::config::TlsVerifyMode;
use arc_swap::ArcSwapOption;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{
    self,
    client::danger::{ServerCertVerified, ServerCertVerifier},
    pki_types::pem::PemObject,
    ClientConfig,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::{debug, info, warn};

use crate::config::config;

use super::Error;

static ACCEPTOR: ArcSwapOption<TlsAcceptor> = ArcSwapOption::const_empty();

/// Get the current TLS acceptor snapshot, if TLS is enabled.
pub fn acceptor() -> Option<Arc<TlsAcceptor>> {
    ACCEPTOR.load_full()
}

/// Create new TLS connector using the current configuration.
pub fn connector() -> Result<TlsConnector, Error> {
    let config = config();
    connector_with_verify_mode(
        config.config.general.tls_verify,
        config.config.general.tls_server_ca_certificate.as_ref(),
    )
}

/// Preload TLS at startup.
pub fn load() -> Result<(), Error> {
    reload()
}

/// Rebuild TLS primitives according to the current configuration.
///
/// This validates the new settings and swaps them in atomically. If validation
/// fails, the existing TLS acceptor remains active.
pub fn reload() -> Result<(), Error> {
    debug!("reloading TLS configuration");

    let config = config();
    let general = &config.config.general;

    // Always validate upstream TLS settings so we surface CA issues early.
    let _ = connector_with_verify_mode(
        general.tls_verify,
        general.tls_server_ca_certificate.as_ref(),
    )?;

    let tls_paths = general.tls();
    let new_acceptor = tls_paths
        .map(|(cert, key)| build_acceptor(cert, key))
        .transpose()?;

    match (new_acceptor, tls_paths) {
        (Some(acceptor), Some((cert, _))) => {
            let acceptor = Arc::new(acceptor);
            let previous = ACCEPTOR.swap(Some(acceptor));

            if previous.is_none() {
                info!(cert = %cert.display(), "🔑 TLS enabled");
            } else {
                info!(cert = %cert.display(), "🔁 TLS certificate reloaded");
            }
        }
        (None, _) => {
            let previous = ACCEPTOR.swap(None);
            if previous.is_some() {
                info!("🔓 TLS disabled");
            }
        }
        // This state should be unreachable because `new_acceptor` is `Some`
        // iff `tls_paths` is `Some`.
        (Some(_), None) => {
            warn!("TLS acceptor built without configuration; keeping previous value");
        }
    }

    Ok(())
}

fn build_acceptor(cert: &Path, key: &Path) -> Result<TlsAcceptor, Error> {
    let pem = CertificateDer::from_pem_file(cert)?;
    let key = PrivateKeyDer::from_pem_file(key)?;

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![pem], key)?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

#[derive(Debug)]
struct AllowAllVerifier;

impl ServerCertVerifier for AllowAllVerifier {
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
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA1,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::ED448,
        ]
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
        debug!("loading CA certificate from: {}", ca_path.display());

        let certs = CertificateDer::pem_file_iter(ca_path)
            .map_err(|e| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to read CA certificate file: {}", e),
                ))
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to parse CA certificates: {}", e),
                ))
            })?;

        if certs.is_empty() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "No valid certificates found in CA file",
            )));
        }

        let (added, _ignored) = roots.add_parsable_certificates(certs);
        debug!("added {} CA certificates from file", added);

        if added == 0 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "No valid certificates could be added from CA file",
            )));
        }
    } else if mode == TlsVerifyMode::VerifyCa || mode == TlsVerifyMode::VerifyFull {
        // For Certificate and Full modes, we need CA certificates
        // Load system native certificates as fallback
        debug!("no custom CA certificate provided, loading system certificates");
        let result = rustls_native_certs::load_native_certs();
        for cert in result.certs {
            roots.add(cert)?;
        }
        if !result.errors.is_empty() {
            debug!(
                "some system certificates could not be loaded: {:?}",
                result.errors
            );
        }
        debug!("loaded {} system CA certificates", roots.len());
    }

    // Create the appropriate config based on the verification mode
    let config = match mode {
        TlsVerifyMode::Disabled => {
            // For Disabled mode, we still create a connector but it won't be used
            // The server connection logic should skip TLS entirely
            ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth()
        }
        TlsVerifyMode::Prefer => {
            let verifier = AllowAllVerifier;
            ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(verifier))
                .with_no_client_auth()
        }
        TlsVerifyMode::VerifyCa => {
            let verifier = NoHostnameVerifier::new(roots.clone());
            let mut config = ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();

            config
                .dangerous()
                .set_certificate_verifier(Arc::new(verifier));

            config
        }
        TlsVerifyMode::VerifyFull => ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
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
        server_name: &rustls::pki_types::ServerName<'_>,
        ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        debug!(
            "certificate verification (Certificate mode): validating certificate for {:?}",
            server_name
        );

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
            Ok(_) => {
                debug!("certificate validation successful (ignoring hostname)");
                Ok(ServerCertVerified::assertion())
            }
            Err(rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidForName)) => {
                // If the only error is hostname mismatch, that's fine for Certificate mode
                debug!("certificate validation successful (hostname mismatch ignored)");
                Ok(ServerCertVerified::assertion())
            }
            Err(e) => {
                debug!("certificate validation failed: {:?}", e);
                Err(e)
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TlsVerifyMode;

    #[tokio::test]
    async fn test_connector_with_verify_mode() {
        crate::logger();

        let prefer = connector_with_verify_mode(TlsVerifyMode::Prefer, None);
        let certificate = connector_with_verify_mode(TlsVerifyMode::VerifyCa, None);
        let full = connector_with_verify_mode(TlsVerifyMode::VerifyFull, None);

        // All should succeed
        assert!(prefer.is_ok());
        assert!(certificate.is_ok());
        assert!(full.is_ok());
    }

    #[tokio::test]
    async fn test_connector_with_verify_mode_missing_ca_file() {
        crate::logger();

        let bad_ca_path = PathBuf::from("/tmp/test_ca.pem");
        let result = connector_with_verify_mode(TlsVerifyMode::VerifyFull, Some(&bad_ca_path));

        // This should fail because the file doesn't exist
        assert!(result.is_err(), "Should fail with non-existent cert file");
    }

    #[tokio::test]
    async fn test_connector_with_verify_mode_good_ca_file() {
        crate::logger();

        let good_ca_path = PathBuf::from("tests/tls/cert.pem");

        info!("Using test CA file: {}", good_ca_path.display());
        // check that the file exists
        assert!(good_ca_path.exists(), "Test CA file should exist");

        let result = connector_with_verify_mode(TlsVerifyMode::VerifyFull, Some(&good_ca_path));

        assert!(result.is_ok(), "Should succeed with valid cert file");
    }
}
