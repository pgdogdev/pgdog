use super::token_cache::{self, CacheKey};
use aws_config::{BehaviorVersion, Region};
use aws_sdk_rds::auth_token::{AuthTokenGenerator, Config as AuthTokenConfig};

use crate::backend::{pool::Address, Error};

fn infer_region_from_rds_host(host: &str) -> Option<String> {
    let host = host.to_ascii_lowercase();
    let labels = host.split('.').collect::<Vec<_>>();
    let rds = labels.iter().position(|label| *label == "rds")?;

    if rds == 0 {
        return None;
    }

    match labels.get(rds + 1..) {
        // *.region.rds.amazonaws.com
        Some(["amazonaws", "com"]) => {}
        // *.region.rds.amazonaws.com.cn
        Some(["amazonaws", "com", "cn"]) => {}
        _ => return None,
    }

    let region = labels.get(rds - 1)?;
    if region.is_empty() {
        return None;
    }

    Some((*region).to_owned())
}

fn resolve_region(addr: &Address) -> Result<String, Error> {
    if let Some(region) = addr.server_iam_region.as_ref() {
        if !region.is_empty() {
            return Ok(region.clone());
        }
    }

    infer_region_from_rds_host(&addr.host).ok_or_else(|| {
        Error::RdsIamToken(format!(
            "unable to infer AWS region from host \"{}\"; set \"server_iam_region\"",
            addr.host
        ))
    })
}

pub async fn token(addr: &Address) -> Result<String, Error> {
    #[cfg(test)]
    if let Some(token) = test_token_override() {
        return Ok(token);
    }

    let key = CacheKey::from(addr);

    if let Some(cached) = token_cache::get(&key) {
        return Ok(cached);
    }

    let (token, expires_at) = fetch_token(addr).await?;
    token_cache::set(key, token.clone(), expires_at);

    Ok(token)
}

async fn fetch_token(addr: &Address) -> Result<(String, std::time::SystemTime), Error> {
    let region = resolve_region(addr)?;
    let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;

    let config = AuthTokenConfig::builder()
        .hostname(addr.host.as_str())
        .port(addr.port.into())
        .username(addr.user.as_str())
        .region(Region::new(region.clone()))
        .build()
        .map_err(|error| {
            Error::RdsIamToken(format!(
                "failed to build RDS IAM token config for {}@{}:{} in region {}: {}",
                addr.user, addr.host, addr.port, region, error
            ))
        })?;

    let token = AuthTokenGenerator::new(config)
        .auth_token(&sdk_config)
        .await
        .map(|token| token.to_string())
        .map_err(|error| {
            Error::RdsIamToken(format!(
                "failed to generate RDS IAM token for {}@{}:{} in region {}: {}",
                addr.user, addr.host, addr.port, region, error
            ))
        })?;

    // RDS IAM tokens are valid for 15 minutes
    let expires_at = std::time::SystemTime::now() + std::time::Duration::from_secs(900);

    Ok((token, expires_at))
}

// ── test helpers ─────────────────────────────────────────────────────────────

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
    use std::env;
    use std::time::{Duration, SystemTime};

    use pgdog_config::Role;

    use crate::backend::pool::Address;
    use crate::config::ServerAuth;
    use token_cache::{CacheKey, CachedToken, TOKEN_CACHE};

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

    #[test]
    fn test_infer_region_commercial_endpoint() {
        let region = infer_region_from_rds_host("db.cluster-abc123.us-east-1.rds.amazonaws.com");
        assert_eq!(region.as_deref(), Some("us-east-1"));
    }

    #[test]
    fn test_infer_region_china_endpoint() {
        let region =
            infer_region_from_rds_host("db.cluster-abc123.cn-north-1.rds.amazonaws.com.cn");
        assert_eq!(region.as_deref(), Some("cn-north-1"));
    }

    #[test]
    fn test_infer_region_fails_for_custom_hostname() {
        let region = infer_region_from_rds_host("postgres.internal.example.com");
        assert!(region.is_none());
    }

    #[test]
    fn token_override_bypasses_cache() {
        set_test_token_override(Some("override-token".into()));
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(token(&make_addr()))
            .unwrap();
        assert_eq!(result, "override-token");
        set_test_token_override(None);
    }

    #[test]
    fn cache_returns_same_token_on_second_call() {
        let addr = make_addr();
        let key = CacheKey::from(&addr);
        let sentinel = "cached-sentinel-token".to_string();
        TOKEN_CACHE.lock().insert(
            key,
            CachedToken::new(
                sentinel.clone(),
                SystemTime::now() + Duration::from_secs(3600),
            ),
        );

        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(token(&addr))
            .unwrap();

        assert_eq!(result, sentinel);
    }

    #[tokio::test]
    async fn test_token_contains_expected_query_fields() {
        let _access_key = EnvVarGuard::set("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE");
        let _secret_key = EnvVarGuard::set("AWS_SECRET_ACCESS_KEY", "SECRETEXAMPLE");
        let _session = EnvVarGuard::set("AWS_SESSION_TOKEN", "SESSIONEXAMPLE");

        let addr = Address {
            host: "db.cluster-abc123.us-east-1.rds.amazonaws.com".into(),
            port: 5432,
            database_name: "postgres".into(),
            user: "db_user".into(),
            passwords: vec![String::new()],
            database_number: 0,
            server_auth: ServerAuth::RdsIam,
            server_iam_region: Some("us-east-1".into()),
            configured_role: Role::Auto,
        };

        let token = token(&addr).await.unwrap();
        assert!(token.starts_with(
            "db.cluster-abc123.us-east-1.rds.amazonaws.com:5432/?Action=connect&DBUser=db_user"
        ));
        assert!(token.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
        assert!(token.contains("X-Amz-Credential="));
        assert!(token.contains("X-Amz-Signature="));
    }

    fn make_addr() -> Address {
        Address {
            host: "db.cluster-abc123.us-east-1.rds.amazonaws.com".into(),
            port: 5432,
            database_name: "postgres".into(),
            user: "db_user".into(),
            passwords: vec![String::new()],
            database_number: 0,
            server_auth: ServerAuth::RdsIam,
            server_iam_region: Some("us-east-1".into()),
            configured_role: Role::Auto,
        }
    }
}
