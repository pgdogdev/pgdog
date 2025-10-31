use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use rust::setup::admin_tokio;
use serial_test::serial;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::sleep,
};
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::{TlsConnector, rustls};

const HOST: &str = "127.0.0.1";
const PORT: u16 = 6432;
const INITIAL_CERT_PLACEHOLDER: &str = "integration/tls/cert.pem";
const INITIAL_KEY_PLACEHOLDER: &str = "integration/tls/key.pem";

fn asset_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data/tls")
        .join(name)
}

fn rotated_cert_path() -> PathBuf {
    asset_path("rotated_cert.pem")
}

fn rotated_key_path() -> PathBuf {
    asset_path("rotated_key.pem")
}

#[tokio::test]
#[serial]
async fn tls_acceptor_swaps_after_sighup() {
    let mut guard = ConfigGuard::new().expect("config guard");

    let initial_cert = fetch_server_cert_der().await.expect("initial cert");
    let rotated_cert = load_cert_der(&rotated_cert_path()).expect("rotated cert load");
    assert_ne!(
        initial_cert.as_ref(),
        rotated_cert.as_ref(),
        "test requires distinct certificates"
    );

    guard
        .rewrite_config(&rotated_cert_path(), &rotated_key_path())
        .expect("rewrite config for rotated cert");

    guard.signal_sighup().expect("send sighup");
    sleep(Duration::from_millis(500)).await;

    let reloaded_cert = fetch_server_cert_der().await.expect("cert after sighup");

    assert_eq!(
        reloaded_cert.as_ref(),
        rotated_cert.as_ref(),
        "TLS acceptor should switch to the rotated certificate after SIGHUP"
    );
}

#[tokio::test]
#[serial]
async fn tls_acceptor_swaps_after_admin_reload() {
    let mut guard = ConfigGuard::new().expect("config guard");

    let rotated_cert = load_cert_der(&rotated_cert_path()).expect("rotated cert load");

    guard
        .rewrite_config(&rotated_cert_path(), &rotated_key_path())
        .expect("rewrite config for rotated cert");

    let admin = admin_tokio().await;
    admin.simple_query("RELOAD").await.expect("admin reload");
    sleep(Duration::from_millis(500)).await;

    let reloaded_cert = fetch_server_cert_der().await.expect("cert after RELOAD");

    assert_eq!(
        reloaded_cert.as_ref(),
        rotated_cert.as_ref(),
        "TLS acceptor should switch to the rotated certificate after RELOAD"
    );
}

fn load_cert_der(
    path: &Path,
) -> Result<rustls::pki_types::CertificateDer<'static>, std::io::Error> {
    rustls::pki_types::CertificateDer::from_pem_file(path)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
}

async fn fetch_server_cert_der()
-> Result<rustls::pki_types::CertificateDer<'static>, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect((HOST, PORT)).await?;

    let mut request = Vec::with_capacity(8);
    request.extend_from_slice(&8u32.to_be_bytes());
    request.extend_from_slice(&80877103u32.to_be_bytes());
    stream.write_all(&request).await?;
    stream.flush().await?;

    let mut reply = [0u8; 1];
    stream.read_exact(&mut reply).await?;
    if reply[0] != b'S' {
        return Err("server rejected TLS negotiation".into());
    }

    let verifier = Arc::new(AllowAllVerifier);
    let config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();

    let connector = TlsConnector::from(Arc::new(config));
    let tls_stream = connector
        .connect(
            rustls::pki_types::ServerName::try_from("localhost")?,
            stream,
        )
        .await?;

    let (_, connection) = tls_stream.get_ref();
    let presented = connection
        .peer_certificates()
        .and_then(|certs| certs.first())
        .ok_or("server did not present a certificate")?;

    Ok(presented.clone().into_owned())
}

struct ConfigGuard {
    path: PathBuf,
    original: String,
    pid: libc::pid_t,
}

impl ConfigGuard {
    fn new() -> Result<Self, std::io::Error> {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        println!("base: {}", manifest_dir.display());
        let config_path = manifest_dir.join("../pgdog.toml").canonicalize()?;
        let original = fs::read_to_string(&config_path)?;
        let pid_path = manifest_dir.join("../pgdog.pid").canonicalize()?;
        let pid_contents = fs::read_to_string(pid_path)?;
        let pid: libc::pid_t = pid_contents.trim().parse().map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid pid: {err}"),
            )
        })?;

        Ok(Self {
            path: config_path,
            original,
            pid,
        })
    }

    fn rewrite_config(&mut self, cert: &Path, key: &Path) -> Result<(), std::io::Error> {
        let mut updated = self.original.clone();
        let cert_str = cert.to_string_lossy();
        let key_str = key.to_string_lossy();

        updated = updated.replace(INITIAL_CERT_PLACEHOLDER, cert_str.as_ref());
        updated = updated.replace(INITIAL_KEY_PLACEHOLDER, key_str.as_ref());
        fs::write(&self.path, updated)
    }

    fn signal_sighup(&self) -> Result<(), std::io::Error> {
        let res = unsafe { libc::kill(self.pid, libc::SIGHUP) };
        if res != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
}

impl Drop for ConfigGuard {
    fn drop(&mut self) {
        let _ = fs::write(&self.path, &self.original);
        let _ = unsafe { libc::kill(self.pid, libc::SIGHUP) };
        std::thread::sleep(Duration::from_millis(500));
    }
}

#[derive(Debug)]
struct AllowAllVerifier;

impl rustls::client::danger::ServerCertVerifier for AllowAllVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
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
