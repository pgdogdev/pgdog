//! TLS configuration.

use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::config::TlsVerifyMode;
use arc_swap::ArcSwapOption;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{
    self, ClientConfig,
    client::danger::{ServerCertVerified, ServerCertVerifier},
    pki_types::pem::PemObject,
    server::{ServerConnection, WebPkiClientVerifier, danger::ClientCertVerifier},
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::{debug, info, warn};
use x509_parser::prelude::FromDer;

use crate::config::config;

use super::Error;

static ACCEPTOR: ArcSwapOption<TlsAcceptor> = ArcSwapOption::const_empty();
static ACCEPTOR_BUILD_COUNT: AtomicUsize = AtomicUsize::new(0);

static CONNECTOR: ArcSwapOption<ConnectorCacheEntry> = ArcSwapOption::const_empty();

#[derive(Clone, Debug, PartialEq)]
struct ConnectorConfigKey {
    mode: TlsVerifyMode,
    ca_path: Option<PathBuf>,
}

impl ConnectorConfigKey {
    fn new(mode: TlsVerifyMode, ca_path: Option<&PathBuf>) -> Self {
        Self {
            mode,
            ca_path: ca_path.cloned(),
        }
    }
}

struct ConnectorCacheEntry {
    key: ConnectorConfigKey,
    config: Arc<ClientConfig>,
}

impl ConnectorCacheEntry {
    fn new(key: ConnectorConfigKey, config: Arc<ClientConfig>) -> Arc<Self> {
        Arc::new(Self { key, config })
    }

    fn connector(&self) -> TlsConnector {
        TlsConnector::from(self.config.clone())
    }
}

#[cfg(test)]
static CONNECTOR_BUILD_COUNT: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
fn increment_connector_build_count() {
    CONNECTOR_BUILD_COUNT.fetch_add(1, Ordering::SeqCst);
}

#[cfg(not(test))]
fn increment_connector_build_count() {}

/// Get the current TLS acceptor snapshot, if TLS is enabled.
pub fn acceptor() -> Option<Arc<TlsAcceptor>> {
    ACCEPTOR.load_full()
}

/// Extract the hostname identity from the peer's TLS certificate, if present.
pub fn peer_identity(conn: &ServerConnection) -> Option<String> {
    identity_from_certs(conn.peer_certificates()?)
}

/// Extract a hostname identity from the first certificate in the chain.
///
/// Prefers the first `dNSName` in the Subject Alternative Name extension and
/// falls back to the Subject CN. RFC 6125 deprecated CN for hostname identity,
/// and modern certificates often publish identity only via SAN.
pub(crate) fn identity_from_certs(certs: &[CertificateDer<'_>]) -> Option<String> {
    use x509_parser::certificate::X509Certificate;
    use x509_parser::extensions::GeneralName;

    let cert_der = certs.first()?;
    let (_, cert) = X509Certificate::from_der(cert_der).ok()?;

    if let Ok(Some(san)) = cert.subject_alternative_name() {
        let dns_name = san.value.general_names.iter().find_map(|gn| match gn {
            GeneralName::DNSName(name) => Some((*name).to_string()),
            _ => None,
        });
        if dns_name.is_some() {
            return dns_name;
        }
    }

    cert.subject()
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok())
        .map(String::from)
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
    let client_ca = general.tls_client_ca_certificate.as_deref();
    let new_acceptor = tls_paths
        .map(|(cert, key)| build_acceptor(cert, key, client_ca))
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

fn build_acceptor(cert: &Path, key: &Path, client_ca: Option<&Path>) -> Result<TlsAcceptor, Error> {
    // Read the whole PEM file so intermediate certificates in a chain are kept.
    let certs = CertificateDer::pem_file_iter(cert)
        .map_err(|e| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to read TLS certificate file: {}", e),
            ))
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to parse TLS certificates: {}", e),
            ))
        })?;

    if certs.is_empty() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "No valid certificates found in TLS certificate file",
        )));
    }

    let key = PrivateKeyDer::from_pem_file(key)?;

    let builder = rustls::ServerConfig::builder();
    let config = match client_ca {
        Some(path) => {
            let verifier = build_client_cert_verifier(path)?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    }
    .with_single_cert(certs, key)?;

    ACCEPTOR_BUILD_COUNT.fetch_add(1, Ordering::SeqCst);

    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn build_client_cert_verifier(ca_path: &Path) -> Result<Arc<dyn ClientCertVerifier>, Error> {
    let roots = load_ca_bundle(ca_path, "client CA")?;

    WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|e| invalid_data(format!("failed to build client certificate verifier: {e}")))
}

/// Load a PEM bundle from `path` and turn it into a `RootCertStore`. Every PEM block
/// in the file is added as a trust anchor, so a single file can carry a root CA
/// together with one or more intermediate CAs that signed leaf client certificates.
fn load_ca_bundle(path: &Path, label: &str) -> Result<rustls::RootCertStore, Error> {
    debug!("loading {label} bundle from {}", path.display());

    let certs = CertificateDer::pem_file_iter(path)
        .map_err(|e| {
            invalid_data(format!(
                "failed to read {label} file {}: {e}",
                path.display()
            ))
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            invalid_data(format!(
                "failed to parse {label} from {}: {e}",
                path.display()
            ))
        })?;

    if certs.is_empty() {
        return Err(invalid_data(format!(
            "no PEM certificates found in {label} file {}",
            path.display()
        )));
    }

    let total = certs.len();
    let mut roots = rustls::RootCertStore::empty();
    let (added, ignored) = roots.add_parsable_certificates(certs);

    if ignored > 0 {
        return Err(invalid_data(format!(
            "{ignored} of {total} certificates in {label} bundle {} could not be loaded as trust anchors",
            path.display()
        )));
    }

    if added == 0 {
        return Err(invalid_data(format!(
            "no valid trust anchors in {label} bundle {}",
            path.display()
        )));
    }

    info!(
        path = %path.display(),
        certs = added,
        "🔐 loaded {label} bundle"
    );

    Ok(roots)
}

fn invalid_data(msg: impl Into<String>) -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        msg.into(),
    ))
}

fn build_connector(config_key: &ConnectorConfigKey) -> Result<Arc<ClientConfig>, Error> {
    let roots = if let Some(ca_path) = config_key.ca_path.as_ref() {
        load_ca_bundle(ca_path, "server CA")?
    } else if matches!(
        config_key.mode,
        TlsVerifyMode::VerifyCa | TlsVerifyMode::VerifyFull
    ) {
        debug!("no custom CA certificate provided, loading system certificates");
        let mut roots = rustls::RootCertStore::empty();
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
        roots
    } else {
        rustls::RootCertStore::empty()
    };

    let config = match config_key.mode {
        TlsVerifyMode::Disabled => ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
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

    increment_connector_build_count();

    Ok(Arc::new(config))
}

#[cfg_attr(not(test), allow(dead_code))]
#[doc(hidden)]
pub fn test_acceptor_build_count() -> usize {
    ACCEPTOR_BUILD_COUNT.load(Ordering::SeqCst)
}

#[cfg_attr(not(test), allow(dead_code))]
#[doc(hidden)]
pub fn test_reset_acceptor() {
    ACCEPTOR.store(None);
    ACCEPTOR_BUILD_COUNT.store(0, Ordering::SeqCst);
}

#[cfg(test)]
#[doc(hidden)]
pub fn test_connector_build_count() -> usize {
    CONNECTOR_BUILD_COUNT.load(Ordering::SeqCst)
}

#[cfg(test)]
#[doc(hidden)]
pub fn test_reset_connector() {
    CONNECTOR.store(None);
    CONNECTOR_BUILD_COUNT.store(0, Ordering::SeqCst);
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
    let config_key = ConnectorConfigKey::new(mode, ca_cert_path);

    if let Some(entry) = CONNECTOR.load_full()
        && entry.key == config_key
    {
        return Ok(entry.connector());
    }

    let client_config = build_connector(&config_key)?;
    let connector = TlsConnector::from(client_config.clone());
    CONNECTOR.store(Some(ConnectorCacheEntry::new(config_key, client_config)));

    Ok(connector)
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
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::NotValidForNameContext { .. },
            )) => {
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
    use std::sync::Arc;

    #[test]
    fn acceptor_reuse_snapshot() {
        crate::logger();

        super::test_reset_acceptor();

        let cert = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/tls/cert.pem");
        let key = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/tls/key.pem");

        let mut cfg = crate::config::ConfigAndUsers::default();
        cfg.config.general.tls_certificate = Some(cert.clone());
        cfg.config.general.tls_private_key = Some(key.clone());

        crate::config::set(cfg).unwrap();

        super::reload().unwrap();

        assert_eq!(super::test_acceptor_build_count(), 1, "acceptor built once");

        let first = super::acceptor().expect("acceptor initialized");
        let second = super::acceptor().expect("acceptor initialized");

        assert!(Arc::ptr_eq(&first, &second), "cached acceptor reused");
        assert_eq!(
            super::test_acceptor_build_count(),
            1,
            "no additional builds"
        );

        super::test_reset_acceptor();

        crate::config::set(crate::config::ConfigAndUsers::default()).unwrap();
    }

    #[test]
    fn acceptor_with_client_ca_builds() {
        crate::logger();

        super::test_reset_acceptor();

        let cert = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/tls/cert.pem");
        let key = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/tls/key.pem");
        let client_ca = cert.clone();

        let mut cfg = crate::config::ConfigAndUsers::default();
        cfg.config.general.tls_certificate = Some(cert.clone());
        cfg.config.general.tls_private_key = Some(key.clone());
        cfg.config.general.tls_client_ca_certificate = Some(client_ca);

        crate::config::set(cfg.clone()).unwrap();
        super::reload().expect("acceptor with client CA builds");

        let acceptor = super::acceptor().expect("acceptor installed");
        assert_eq!(super::test_acceptor_build_count(), 1);

        // Point to a non-existent client CA.
        cfg.config.general.tls_client_ca_certificate = Some(PathBuf::from("/tmp/test_ca.pem"));
        crate::config::set(cfg).unwrap();

        assert!(
            super::reload().is_err(),
            "reload should fail with bad client CA"
        );

        // The existing acceptor should remain in place.
        assert!(Arc::ptr_eq(&acceptor, &super::acceptor().unwrap()));

        super::test_reset_acceptor();
        crate::config::set(crate::config::ConfigAndUsers::default()).unwrap();
    }

    #[test]
    fn acceptor_loads_certificate_chain() {
        crate::logger();

        super::test_reset_acceptor();

        let cert = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/tls/chain_cert.pem");
        let key = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/tls/chain_key.pem");

        let mut cfg = crate::config::ConfigAndUsers::default();
        cfg.config.general.tls_certificate = Some(cert);
        cfg.config.general.tls_private_key = Some(key);

        crate::config::set(cfg).unwrap();
        super::reload().expect("acceptor builds from a certificate chain");

        super::acceptor().expect("acceptor installed");
        assert_eq!(super::test_acceptor_build_count(), 1);

        super::test_reset_acceptor();
        crate::config::set(crate::config::ConfigAndUsers::default()).unwrap();
    }

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
    async fn connector_reuses_cached_config() {
        crate::logger();

        super::test_reset_acceptor();
        super::test_reset_connector();

        let cert_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/tls/cert.pem");
        let key_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/tls/key.pem");
        let ca_path = cert_path.clone();

        let mut cfg = crate::config::ConfigAndUsers::default();
        cfg.config.general.tls_certificate = Some(cert_path.clone());
        cfg.config.general.tls_private_key = Some(key_path.clone());
        cfg.config.general.tls_server_ca_certificate = Some(ca_path.clone());
        cfg.config.general.tls_verify = TlsVerifyMode::VerifyFull;

        crate::config::set(cfg).unwrap();

        let _first = super::connector_with_verify_mode(TlsVerifyMode::VerifyFull, Some(&ca_path))
            .expect("first connector builds");
        let first_cache = super::CONNECTOR
            .load_full()
            .expect("connector cached after first build");

        let _second = super::connector_with_verify_mode(TlsVerifyMode::VerifyFull, Some(&ca_path))
            .expect("second connector reuses cache");
        let second_cache = super::CONNECTOR
            .load_full()
            .expect("connector cached after second build");

        assert_eq!(
            super::test_connector_build_count(),
            1,
            "connector built once"
        );
        assert!(
            Arc::ptr_eq(&first_cache, &second_cache),
            "cache entry reused"
        );
        assert!(
            Arc::ptr_eq(&first_cache.config, &second_cache.config),
            "client config reused"
        );

        super::reload().expect("reload succeeds");

        let post_reload_cache = super::CONNECTOR
            .load_full()
            .expect("connector cached after reload");

        assert_eq!(
            super::test_connector_build_count(),
            1,
            "reload does not rebuild connector"
        );
        assert!(
            Arc::ptr_eq(&second_cache, &post_reload_cache),
            "reload retains cache entry"
        );
        assert!(
            Arc::ptr_eq(&second_cache.config, &post_reload_cache.config),
            "reload retains client config"
        );

        let _third = super::connector_with_verify_mode(TlsVerifyMode::VerifyFull, Some(&ca_path))
            .expect("third connector still reuses cache");
        let third_cache = super::CONNECTOR
            .load_full()
            .expect("connector cached after third build");

        assert_eq!(
            super::test_connector_build_count(),
            1,
            "additional calls reuse existing connector"
        );
        assert!(
            Arc::ptr_eq(&post_reload_cache, &third_cache),
            "cache entry unchanged"
        );

        super::test_reset_connector();
        super::test_reset_acceptor();
        crate::config::set(crate::config::ConfigAndUsers::default()).unwrap();
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

    #[test]
    fn identity_from_test_cert() {
        let pem = include_str!("../../tests/tls/cert.pem");
        let certs: Vec<CertificateDer<'static>> =
            rustls_pki_types::CertificateDer::pem_slice_iter(pem.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .expect("parse PEM");

        let identity = identity_from_certs(&certs);
        assert_eq!(identity.as_deref(), Some("CommonNameOrHostname"));
    }

    #[test]
    fn identity_from_empty_certs() {
        assert_eq!(identity_from_certs(&[]), None);
    }

    #[test]
    fn san_dns_preferred_over_cn() {
        let pem = include_str!("../../tests/tls/cert_with_san.pem");
        let certs: Vec<CertificateDer<'static>> =
            rustls_pki_types::CertificateDer::pem_slice_iter(pem.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .expect("parse PEM");

        // Subject CN is "fallback-cn.example" but SAN dNSName comes first.
        assert_eq!(
            identity_from_certs(&certs).as_deref(),
            Some("primary.san.example")
        );
    }

    #[test]
    fn san_dns_used_when_no_cn() {
        let pem = include_str!("../../tests/tls/cert_san_only.pem");
        let certs: Vec<CertificateDer<'static>> =
            rustls_pki_types::CertificateDer::pem_slice_iter(pem.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .expect("parse PEM");

        assert_eq!(
            identity_from_certs(&certs).as_deref(),
            Some("only-via-san.example")
        );
    }

    #[test]
    fn load_ca_bundle_loads_full_chain() {
        crate::logger();

        let chain = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/tls/ca_chain.pem");
        let roots = super::load_ca_bundle(&chain, "client CA")
            .expect("ca_chain.pem bundles root + intermediate");

        assert_eq!(roots.len(), 2, "every PEM block becomes a trust anchor");
    }

    #[test]
    fn load_ca_bundle_errors_on_missing_file() {
        crate::logger();
        let missing = PathBuf::from("/tmp/pgdog_nonexistent_ca.pem");
        assert!(super::load_ca_bundle(&missing, "client CA").is_err());
    }

    #[test]
    fn client_cert_verifier_accepts_intermediate_signed_cert() {
        crate::logger();

        let chain = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/tls/ca_chain.pem");
        let verifier =
            super::build_client_cert_verifier(&chain).expect("verifier builds from chain bundle");

        let client_pem = include_str!("../../tests/tls/client_signed_by_intermediate.pem");
        let leaf: CertificateDer<'static> =
            rustls_pki_types::CertificateDer::pem_slice_iter(client_pem.as_bytes())
                .next()
                .expect("client cert PEM has one block")
                .expect("client cert parses");

        // Use the cert's notBefore as "now" so the test does not drift if the fixture
        // gets regenerated with a non-current validity window.
        use x509_parser::certificate::X509Certificate;
        let (_, parsed) = X509Certificate::from_der(&leaf).expect("parse leaf cert");
        let now = rustls::pki_types::UnixTime::since_unix_epoch(std::time::Duration::from_secs(
            parsed.validity().not_before.timestamp() as u64 + 60,
        ));

        verifier
            .verify_client_cert(&leaf, &[], now)
            .expect("intermediate trust anchor accepts leaf signed by it");
    }

    #[test]
    fn client_cert_verifier_rejects_unknown_signer_when_only_root_loaded() {
        crate::logger();

        // Trust store contains only the root; client presents only the leaf
        // (no intermediate in the handshake), so webpki cannot build the chain.
        let root_only = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/tls/ca_root.pem");
        let verifier = super::build_client_cert_verifier(&root_only)
            .expect("verifier builds from root-only bundle");

        let client_pem = include_str!("../../tests/tls/client_signed_by_intermediate.pem");
        let leaf: CertificateDer<'static> =
            rustls_pki_types::CertificateDer::pem_slice_iter(client_pem.as_bytes())
                .next()
                .expect("client cert PEM has one block")
                .expect("client cert parses");

        use x509_parser::certificate::X509Certificate;
        let (_, parsed) = X509Certificate::from_der(&leaf).expect("parse leaf cert");
        let now = rustls::pki_types::UnixTime::since_unix_epoch(std::time::Duration::from_secs(
            parsed.validity().not_before.timestamp() as u64 + 60,
        ));

        assert!(
            verifier.verify_client_cert(&leaf, &[], now).is_err(),
            "leaf signed by missing intermediate must be rejected"
        );
    }
}
