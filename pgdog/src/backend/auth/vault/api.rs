//! Vault HTTP API client built on `reqwest`.

use serde::Deserialize;

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
        let pem = std::fs::read(ca_path).map_err(|e| {
            Error::Http(format!(
                "vault: failed to read tls_server_ca_certificate {ca_path}: {e}"
            ))
        })?;
        let cert = reqwest::Certificate::from_pem(&pem).map_err(|e| {
            Error::Http(format!("vault: invalid tls_server_ca_certificate: {e}"))
        })?;
        builder = builder.add_root_certificate(cert);
    }

    builder
        .build()
        .map_err(|e| Error::Http(format!("vault: failed to build HTTP client: {e}")))
}

// ── public API ────────────────────────────────────────────────────────────────

/// Authenticate to Vault via AppRole and return a client token.
pub async fn approle_login(
    client: &reqwest::Client,
    addr: &str,
    role_id: &str,
    secret_id: &str,
) -> Result<VaultToken, Error> {
    #[cfg(test)]
    if let Some(result) = test_support::login_override() {
        return result;
    }

    let url = format!("{}/v1/auth/approle/login", addr.trim_end_matches('/'));
    let body = serde_json::json!({ "role_id": role_id, "secret_id": secret_id });

    post_login(client, &url, &body).await
}

/// Authenticate to Vault via Kubernetes service account JWT and return a client token.
///
/// `mount_path` is the Vault auth mount (default: `"kubernetes"`).
/// `role` is the Vault role name configured for this cluster.
/// `jwt` is the contents of the pod's service account token file.
pub async fn kubernetes_login(
    client: &reqwest::Client,
    addr: &str,
    mount_path: &str,
    role: &str,
    jwt: &str,
) -> Result<VaultToken, Error> {
    #[cfg(test)]
    if let Some(result) = test_support::login_override() {
        return result;
    }

    let url = format!(
        "{}/v1/auth/{}/login",
        addr.trim_end_matches('/'),
        mount_path.trim_matches('/')
    );
    let body = serde_json::json!({ "role": role, "jwt": jwt });

    post_login(client, &url, &body).await
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

/// Fetch dynamic PostgreSQL credentials from `path` (e.g. `database/creds/dml-role`).
pub async fn fetch_credential(
    client: &reqwest::Client,
    addr: &str,
    token: &str,
    path: &str,
) -> Result<VaultCredential, Error> {
    #[cfg(test)]
    if let Some(result) = test_support::credential_override() {
        return result;
    }

    let url = format!(
        "{}/v1/{}",
        addr.trim_end_matches('/'),
        path.trim_start_matches('/')
    );

    let response = client
        .get(&url)
        .header("X-Vault-Token", token)
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

async fn check_status(response: reqwest::Response) -> Result<reqwest::Response, Error> {
    let status = response.status().as_u16();
    if !(200..300).contains(&(status as usize)) {
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
    use super::*;
    use once_cell::sync::Lazy;

    // reqwest with rustls-tls needs a process-level CryptoProvider.
    static RING: Lazy<()> = Lazy::new(|| {
        let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
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
        let token = kubernetes_login(
            &test_client(),
            "http://irrelevant",
            "kubernetes",
            "pgdog",
            "eyJhbGciOiJSUzI1NiJ9.stub",
        )
        .await
        .unwrap();
        assert_eq!(token.client_token, "k8s-token");
        assert_eq!(token.lease_duration, 7200);
    }

    #[tokio::test]
    async fn test_kubernetes_login_propagates_error() {
        test_support::set_login(Some(Err(Error::VaultStatus { status: 403, body: "".into() })));
        let err = kubernetes_login(&test_client(), "http://irrelevant", "kubernetes", "pgdog", "jwt")
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
        let token = approle_login(&test_client(), "http://irrelevant", "role", "secret")
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
        let cred = fetch_credential(
            &test_client(),
            "http://irrelevant",
            "tok",
            "database/creds/dml-role",
        )
        .await
        .unwrap();
        assert_eq!(cred.username, "v-approle-dml-XyZ");
        assert_eq!(cred.lease_duration, 86400);
    }

    #[tokio::test]
    async fn test_approle_login_propagates_error_override() {
        test_support::set_login(Some(Err(Error::VaultStatus { status: 403, body: "".into() })));
        let err = approle_login(&test_client(), "http://irrelevant", "role", "bad")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::VaultStatus { status: 403, .. }));
    }

    // ── build_client TLS ─────────────────────────────────────────────────────

    fn tls_cfg(tls_verify: VaultTlsVerify, ca: Option<&str>) -> VaultConfig {
        VaultConfig {
            address: "https://vault.example.com".into(),
            auth_method: pgdog_config::VaultAuthMethod::AppRole,
            pre_rotation_pct: 75,
            role_id: None,
            secret_id: None,
            secret_id_file: None,
            kubernetes_role: None,
            kubernetes_jwt_path: VaultConfig::default_kubernetes_jwt_path(),
            kubernetes_mount_path: VaultConfig::default_kubernetes_mount_path(),
            tls_verify,
            tls_server_ca_certificate: ca.map(String::from),
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
        assert!(build_client(&tls_cfg(VaultTlsVerify::VerifyFull, Some("/nonexistent/ca.pem"))).is_err());
    }
}
