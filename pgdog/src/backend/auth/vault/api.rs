//! Vault HTTP API client built on `reqwest`.

use std::fs::read;

use serde::Deserialize;
use serde_json::json;

use pgdog_config::{VaultConfig, VaultTlsVerify};

use super::Error;

// ── public types ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct VaultToken {
    pub client_token: String,
    pub lease_duration: u64,
    pub renewable: bool,
}

#[derive(Debug, Clone)]
pub struct VaultCredential {
    pub username: String,
    pub password: String,
    pub lease_duration: u64,
}

// ── Vault JSON shapes ─────────────────────────────────────────────────────────

/// Both AppRole and Kubernetes login responses share the same `auth` wrapper.
#[derive(Deserialize)]
struct LoginResponse {
    auth: AuthData,
}

#[derive(Deserialize)]
struct AuthData {
    client_token: String,
    lease_duration: u64,
    renewable: bool,
}

#[derive(Deserialize)]
struct CredentialResponse {
    data: CredentialData,
    lease_duration: u64,
}

#[derive(Deserialize)]
struct CredentialData {
    username: String,
    password: String,
}

// ── client construction ───────────────────────────────────────────────────────

/// Build a `reqwest::Client` configured with the TLS settings from `cfg`.
/// Call once per vault config (at startup and in the renewal task) and reuse
/// the resulting client for all API calls within that lifecycle.
pub fn build_client(cfg: &VaultConfig) -> Result<reqwest::Client, Error> {
    let mut builder = reqwest::Client::builder();

    builder = match cfg.tls_verify {
        VaultTlsVerify::Disable => builder.danger_accept_invalid_certs(true),
        VaultTlsVerify::VerifyCa => builder.danger_accept_invalid_hostnames(true),
        VaultTlsVerify::VerifyFull => builder,
    };

    if let Some(ref ca_path) = cfg.tls_server_ca_certificate {
        let pem = read(ca_path).map_err(|e| {
            Error::Http(format!(
                "[vault] failed to read tls_server_ca_certificate {}: {e}",
                ca_path.display()
            ))
        })?;
        let cert = reqwest::Certificate::from_pem(&pem)
            .map_err(|e| Error::Http(format!("[vault] invalid tls_server_ca_certificate: {e}")))?;
        builder = builder.add_root_certificate(cert);
    }

    builder
        .build()
        .map_err(|e| Error::Http(format!("[vault] failed to build HTTP client: {e}")))
}

// ── public API ────────────────────────────────────────────────────────────────

pub struct AppRoleLogin<'a> {
    pub client: &'a reqwest::Client,
    pub addr: &'a str,
    pub role_id: &'a str,
    pub secret_id: &'a str,
}

impl AppRoleLogin<'_> {
    pub async fn call(self) -> Result<VaultToken, Error> {
        #[cfg(test)]
        if let Some(result) = test_support::login_override() {
            return result;
        }

        let url = format!("{}/v1/auth/approle/login", self.addr.trim_end_matches('/'));
        let body = json!({ "role_id": self.role_id, "secret_id": self.secret_id });
        post_login(self.client, &url, &body).await
    }
}

pub struct KubernetesLogin<'a> {
    pub client: &'a reqwest::Client,
    pub addr: &'a str,
    /// Vault auth mount path (default: `"kubernetes"`).
    pub mount_path: &'a str,
    pub role: &'a str,
    /// Contents of the pod's service account token file.
    pub jwt: &'a str,
}

impl KubernetesLogin<'_> {
    pub async fn call(self) -> Result<VaultToken, Error> {
        #[cfg(test)]
        if let Some(result) = test_support::login_override() {
            return result;
        }

        let url = format!(
            "{}/v1/auth/{}/login",
            self.addr.trim_end_matches('/'),
            self.mount_path.trim_matches('/')
        );
        let body = json!({ "role": self.role, "jwt": self.jwt });
        post_login(self.client, &url, &body).await
    }
}

pub struct FetchCredential<'a> {
    pub client: &'a reqwest::Client,
    pub addr: &'a str,
    pub token: &'a str,
    /// Vault path, e.g. `database/creds/dml-role`.
    pub path: &'a str,
}

impl FetchCredential<'_> {
    pub async fn call(self) -> Result<VaultCredential, Error> {
        #[cfg(test)]
        if let Some(result) = test_support::credential_override() {
            return result;
        }

        let url = format!(
            "{}/v1/{}",
            self.addr.trim_end_matches('/'),
            self.path.trim_start_matches('/')
        );

        let response = self
            .client
            .get(&url)
            .header("X-Vault-Token", self.token)
            .send()
            .await
            .map_err(|e| Error::Http(e.to_string()))?;

        let response = check_status(response).await?;

        let parsed: CredentialResponse = response
            .json()
            .await
            .map_err(|e| Error::Parse(e.to_string()))?;

        Ok(VaultCredential {
            username: parsed.data.username,
            password: parsed.data.password,
            lease_duration: parsed.lease_duration,
        })
    }
}

async fn post_login(
    client: &reqwest::Client,
    url: &str,
    body: &serde_json::Value,
) -> Result<VaultToken, Error> {
    let response = client
        .post(url)
        .json(body)
        .send()
        .await
        .map_err(|e| Error::Http(e.to_string()))?;

    let response = check_status(response).await?;

    let parsed: LoginResponse = response
        .json()
        .await
        .map_err(|e| Error::Parse(e.to_string()))?;

    Ok(VaultToken {
        client_token: parsed.auth.client_token,
        lease_duration: parsed.auth.lease_duration,
        renewable: parsed.auth.renewable,
    })
}

async fn check_status(response: reqwest::Response) -> Result<reqwest::Response, Error> {
    let status = response.status().as_u16();
    if !(200u16..300).contains(&status) {
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unreadable>".into());
        return Err(Error::VaultStatus { status, body });
    }
    Ok(response)
}

// ── test support (override hooks) ─────────────────────────────────────────────
//
// Thread-local storage so parallel `#[tokio::test]` runs don't interfere.
// Each test gets a current-thread Tokio runtime on its own thread, so
// thread_local! gives perfect isolation without any locking.

#[cfg(test)]
pub mod test_support {
    use super::{Error, VaultCredential, VaultToken};
    use std::cell::RefCell;

    thread_local! {
        static LOGIN: RefCell<Option<Result<VaultToken, Error>>> = const { RefCell::new(None) };
        static CREDENTIAL: RefCell<Option<Result<VaultCredential, Error>>> = const { RefCell::new(None) };
    }

    pub fn set_login(result: Option<Result<VaultToken, Error>>) {
        LOGIN.with(|c| *c.borrow_mut() = result);
    }

    pub fn set_credential(result: Option<Result<VaultCredential, Error>>) {
        CREDENTIAL.with(|c| *c.borrow_mut() = result);
    }

    pub(super) fn login_override() -> Option<Result<VaultToken, Error>> {
        LOGIN.with(|c| c.borrow_mut().take())
    }

    pub(super) fn credential_override() -> Option<Result<VaultCredential, Error>> {
        CREDENTIAL.with(|c| c.borrow_mut().take())
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use once_cell::sync::Lazy;
    use tokio_rustls::rustls::crypto::aws_lc_rs;

    use pgdog_config::VaultAuthMethod;

    use super::*;

    // reqwest with rustls-tls needs a process-level CryptoProvider.
    static RING: Lazy<()> = Lazy::new(|| {
        let _ = aws_lc_rs::default_provider().install_default();
    });

    fn test_client() -> reqwest::Client {
        let _ = *RING;
        reqwest::Client::new()
    }

    #[test]
    fn test_parse_login_response() {
        let json = r#"{
            "auth": {
                "client_token": "s.abc123",
                "lease_duration": 86400,
                "renewable": true,
                "accessor": "ignored",
                "policies": []
            }
        }"#;
        let parsed: LoginResponse = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.auth.client_token, "s.abc123");
        assert_eq!(parsed.auth.lease_duration, 86400);
        assert!(parsed.auth.renewable);
    }

    #[tokio::test]
    async fn test_kubernetes_login_uses_override() {
        test_support::set_login(Some(Ok(VaultToken {
            client_token: "k8s-token".into(),
            lease_duration: 7200,
            renewable: true,
        })));
        let token = KubernetesLogin {
            client: &test_client(),
            addr: "http://irrelevant",
            mount_path: "kubernetes",
            role: "pgdog",
            jwt: "eyJhbGciOiJSUzI1NiJ9.stub",
        }
        .call()
        .await
        .unwrap();
        assert_eq!(token.client_token, "k8s-token");
        assert_eq!(token.lease_duration, 7200);
    }

    #[tokio::test]
    async fn test_kubernetes_login_propagates_error() {
        test_support::set_login(Some(Err(Error::VaultStatus {
            status: 403,
            body: "".into(),
        })));
        let err = KubernetesLogin {
            client: &test_client(),
            addr: "http://irrelevant",
            mount_path: "kubernetes",
            role: "pgdog",
            jwt: "jwt",
        }
        .call()
        .await
        .unwrap_err();
        assert!(matches!(err, Error::VaultStatus { status: 403, .. }));
    }

    #[test]
    fn test_parse_credential_response() {
        let json = r#"{
            "data": {
                "username": "v-approle-dml-AbCdEf",
                "password": "s3cr3t-pass"
            },
            "lease_duration": 3600,
            "lease_id": "database/creds/dml-role/xyz",
            "renewable": true
        }"#;
        let parsed: CredentialResponse = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.data.username, "v-approle-dml-AbCdEf");
        assert_eq!(parsed.data.password, "s3cr3t-pass");
        assert_eq!(parsed.lease_duration, 3600);
    }

    #[tokio::test]
    async fn test_approle_login_uses_override() {
        test_support::set_login(Some(Ok(VaultToken {
            client_token: "test-token".into(),
            lease_duration: 3600,
            renewable: true,
        })));
        let token = AppRoleLogin {
            client: &test_client(),
            addr: "http://irrelevant",
            role_id: "role",
            secret_id: "secret",
        }
        .call()
        .await
        .unwrap();
        assert_eq!(token.client_token, "test-token");
        assert_eq!(token.lease_duration, 3600);
    }

    #[tokio::test]
    async fn test_fetch_credential_uses_override() {
        test_support::set_credential(Some(Ok(VaultCredential {
            username: "v-approle-dml-XyZ".into(),
            password: "pw".into(),
            lease_duration: 86400,
        })));
        let cred = FetchCredential {
            client: &test_client(),
            addr: "http://irrelevant",
            token: "tok",
            path: "database/creds/dml-role",
        }
        .call()
        .await
        .unwrap();
        assert_eq!(cred.username, "v-approle-dml-XyZ");
        assert_eq!(cred.lease_duration, 86400);
    }

    #[tokio::test]
    async fn test_approle_login_propagates_error_override() {
        test_support::set_login(Some(Err(Error::VaultStatus {
            status: 403,
            body: "".into(),
        })));
        let err = AppRoleLogin {
            client: &test_client(),
            addr: "http://irrelevant",
            role_id: "role",
            secret_id: "bad",
        }
        .call()
        .await
        .unwrap_err();
        assert!(matches!(err, Error::VaultStatus { status: 403, .. }));
    }

    // ── build_client TLS ─────────────────────────────────────────────────────

    fn tls_cfg(tls_verify: VaultTlsVerify, ca: Option<&str>) -> VaultConfig {
        VaultConfig {
            address: "https://vault.example.com".into(),
            auth_method: VaultAuthMethod::AppRole,
            pre_rotation_pct: 75,
            role_id: None,
            secret_id: None,
            secret_id_file: None,
            kubernetes_role: None,
            kubernetes_jwt_path: VaultConfig::default_kubernetes_jwt_path(),
            kubernetes_mount_path: VaultConfig::default_kubernetes_mount_path(),
            tls_verify,
            tls_server_ca_certificate: ca.map(PathBuf::from),
        }
    }

    #[test]
    fn test_build_client_verify_full_default() {
        assert!(build_client(&tls_cfg(VaultTlsVerify::VerifyFull, None)).is_ok());
    }

    #[test]
    fn test_build_client_disable() {
        assert!(build_client(&tls_cfg(VaultTlsVerify::Disable, None)).is_ok());
    }

    #[test]
    fn test_build_client_verify_ca() {
        assert!(build_client(&tls_cfg(VaultTlsVerify::VerifyCa, None)).is_ok());
    }

    #[test]
    fn test_build_client_invalid_ca_path_errors() {
        assert!(build_client(&tls_cfg(
            VaultTlsVerify::VerifyFull,
            Some("/nonexistent/ca.pem")
        ))
        .is_err());
    }
}
