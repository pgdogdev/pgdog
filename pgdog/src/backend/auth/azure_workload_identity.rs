use crate::backend::{pool::Address, Error};
use azure_core::credentials::TokenCredential;
use azure_identity::WorkloadIdentityCredential;

pub async fn token(addr: &Address) -> Result<String, Error> {
    #[cfg(test)]
    if let Some(token) = test_token_override() {
        return Ok(token);
    }

    let credential = WorkloadIdentityCredential::new(None).map_err(|error| {
        Error::AzureWorkloadIdentityToken(format!(
            "failed to build workload identity credential for {}@{}:{}: {}",
            addr.user, addr.host, addr.port, error
        ))
    })?;

    credential
        .get_token(
            &["https://ossrdbms-aad.database.windows.net/.default"],
            None,
        )
        .await
        .map(|token| token.token.secret().to_string())
        .map_err(|error| {
            Error::AzureWorkloadIdentityToken(format!(
                "failed to get Azure AD token for {}@{}:{}: {}",
                addr.user, addr.host, addr.port, error
            ))
        })
}

#[cfg(test)]
fn test_token_override() -> Option<String> {
    TEST_TOKEN_OVERRIDE.lock().clone()
}

#[cfg(test)]
pub(crate) fn set_test_token_override(token: Option<String>) {
    *TEST_TOKEN_OVERRIDE.lock() = token;
}

#[cfg(test)]
static TEST_TOKEN_OVERRIDE: once_cell::sync::Lazy<parking_lot::Mutex<Option<String>>> =
    once_cell::sync::Lazy::new(|| parking_lot::Mutex::new(None));

#[cfg(test)]
mod tests {
    use crate::backend::pool::Address;
    use crate::config::ServerAuth;
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use std::env;

    use super::*;

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = env::var(key).ok();
            env::set_var(key, value);
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(previous) = self.previous.take() {
                env::set_var(self.key, previous);
            } else {
                env::remove_var(self.key);
            }
        }
    }

    #[tokio::test]
    #[ignore = "requires AKS environment with Workload Identity injection"]
    async fn test_token_contains_expected_query_fields() {
        let _azure_client_id = EnvVarGuard::set("AZURE_CLIENT_ID", "EXAMPLE");
        let _azure_tenant_id = EnvVarGuard::set("AZURE_TENANT_ID", "EXAMPLE");
        let _azure_token_file_path = EnvVarGuard::set("AZURE_FEDERATED_TOKEN_FILE", "/tmp/example");

        let addr = Address {
            host: "my-awesome-db.postgres.database.azure.com".into(),
            port: 5432,
            database_name: "postgres".into(),
            user: "db_user".into(),
            passwords: vec![String::new()],
            database_number: 0,
            server_auth: ServerAuth::AzureWorkloadIdentity,
            server_iam_region: None,
        };

        let b64_token = token(&addr).await.unwrap();

        // Use functional chaining to extract and decode
        let token = b64_token
            .split('.')
            .nth(1)
            .map(|payload| URL_SAFE_NO_PAD.decode(payload))
            .transpose()
            .expect("Invalid JWT format") // Converts Option<Result<T, E>> to Result<Option<T>, E>
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .expect("Failed to parse JWT payload as valid UTF-8 JSON");

        assert!(token.contains("https://sts.windows.net/"));
        assert!(token.contains("https://management.azure.com"));
        assert!(token.contains("appid"));
    }
}
