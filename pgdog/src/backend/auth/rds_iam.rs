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

    AuthTokenGenerator::new(config)
        .auth_token(&sdk_config)
        .await
        .map(|token| token.to_string())
        .map_err(|error| {
            Error::RdsIamToken(format!(
                "failed to generate RDS IAM token for {}@{}:{} in region {}: {}",
                addr.user, addr.host, addr.port, region, error
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
    use std::env;

    use crate::backend::pool::Address;
    use crate::config::ServerAuth;

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
            password: String::new(),
            database_number: 0,
            server_auth: ServerAuth::RdsIam,
            server_iam_region: Some("us-east-1".into()),
        };

        let token = token(&addr).await.unwrap();
        assert!(token.starts_with(
            "db.cluster-abc123.us-east-1.rds.amazonaws.com:5432/?Action=connect&DBUser=db_user"
        ));
        assert!(token.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
        assert!(token.contains("X-Amz-Credential="));
        assert!(token.contains("X-Amz-Signature="));
    }
}
