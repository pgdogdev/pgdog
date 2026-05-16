use std::time::SystemTime;

use azure_core::credentials::TokenCredential;
use azure_identity::WorkloadIdentityCredential;

use crate::backend::{pool::Address, Error};

/// Fetch a fresh Azure Workload Identity token for `addr`.
///
/// This is the raw fetcher passed to [`TokenCache::get_or_fetch`] and
/// called by the monitor's refresh loop. Callers should never invoke it
/// directly — go through [`TokenCache::global`] instead.
pub(crate) async fn token(addr: Address) -> Result<(String, SystemTime), Error> {
    let credential = WorkloadIdentityCredential::new(None).map_err(|error| {
        Error::AzureWorkloadIdentityToken(format!(
            "failed to build workload identity credential for {}@{}:{}: {}",
            addr.user, addr.host, addr.port, error
        ))
    })?;

    let access_token = credential
        .get_token(
            &["https://ossrdbms-aad.database.windows.net/.default"],
            None,
        )
        .await
        .map_err(|error| {
            Error::AzureWorkloadIdentityToken(format!(
                "failed to get Azure AD token for {}@{}:{}: {}",
                addr.user, addr.host, addr.port, error
            ))
        })?;

    let expires_at = SystemTime::from(access_token.expires_on);
    Ok((access_token.token.secret().to_string(), expires_at))
}

#[cfg(test)]
mod tests {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use std::env;

    use super::*;
    use crate::config::ServerAuth;
    use pgdog_config::Role;

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
            match self.previous.take() {
                Some(v) => env::set_var(self.key, v),
                None => env::remove_var(self.key),
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
            passwords: vec![],
            database_number: 0,
            server_auth: ServerAuth::AzureWorkloadIdentity,
            server_iam_region: None,
            configured_role: Role::Auto,
        };

        let (b64_token, expires_at) = token(addr).await.unwrap();

        assert!(expires_at > std::time::SystemTime::now());

        let payload = b64_token
            .split('.')
            .nth(1)
            .map(|p| URL_SAFE_NO_PAD.decode(p))
            .transpose()
            .expect("invalid JWT format")
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .expect("failed to parse JWT payload as UTF-8 JSON");

        assert!(payload.contains("https://sts.windows.net/"));
        assert!(payload.contains("https://management.azure.com"));
        assert!(payload.contains("appid"));
    }
}
