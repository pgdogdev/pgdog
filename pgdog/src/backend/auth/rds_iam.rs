use std::time::{Duration, SystemTime};

use aws_config::sts::AssumeRoleProvider;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_rds::auth_token::{AuthTokenGenerator, Config as AuthTokenConfig};

use crate::backend::{Error, pool::Address};

/// STS session name used when PgDog assumes a role to mint a cross-account RDS
/// IAM token.
const IAM_ASSUME_ROLE_SESSION_NAME: &str = "pgdog-rds-connect";

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
    if let Some(region) = addr.server_iam_region.as_ref()
        && !region.is_empty()
    {
        return Ok(region.clone());
    }

    infer_region_from_rds_host(&addr.host).ok_or_else(|| {
        Error::RdsIamToken(format!(
            "unable to infer AWS region from host \"{}\"; set \"server_iam_region\"",
            addr.host
        ))
    })
}

/// Parameters for the STS AssumeRole that precedes token generation.
#[derive(Debug, Clone, PartialEq, Eq)]
struct AssumeRolePlan {
    role_arn: String,
    region: String,
    session_name: String,
}

impl AssumeRolePlan {
    /// The plan to sign `addr`'s RDS IAM token with an assumed role (cross-account
    /// RDS IAM), or `None` for the normal same-account path where the token is
    /// signed with PgDog's ambient identity. Pure so the account-spanning decision
    /// is unit-testable without AWS.
    fn new(addr: &Address, region: &str) -> Option<Self> {
        let role_arn = addr.server_iam_assume_role.as_deref()?;
        if role_arn.is_empty() {
            return None;
        }
        Some(Self {
            role_arn: role_arn.to_owned(),
            region: region.to_owned(),
            session_name: IAM_ASSUME_ROLE_SESSION_NAME.to_owned(),
        })
    }
}

/// Build the AWS config used to sign the RDS IAM token.
///
/// Same-account: PgDog's ambient identity (`load_defaults`). Cross-account: the
/// ambient identity is used only as the *source* credentials to assume
/// `server_iam_assume_role` in the database's account, and the token is signed
/// with those assumed credentials.
async fn build_aws_sdk_config(addr: &Address, region: &str) -> SdkConfig {
    match AssumeRolePlan::new(addr, region) {
        None => aws_config::load_defaults(BehaviorVersion::latest()).await,
        Some(plan) => {
            let base = aws_config::load_defaults(BehaviorVersion::latest()).await;
            let provider = AssumeRoleProvider::builder(plan.role_arn)
                .session_name(plan.session_name)
                .region(Region::new(plan.region.clone()))
                .configure(&base)
                .build()
                .await;
            aws_config::defaults(BehaviorVersion::latest())
                .region(Region::new(plan.region))
                .credentials_provider(provider)
                .load()
                .await
        }
    }
}

/// Fetch a fresh RDS IAM token for `addr`.
///
/// This is the raw fetcher passed to [`TokenCache::get_or_fetch`] and
/// called by the monitor's refresh loop. Callers should never invoke it
/// directly — go through [`TokenCache::global`] instead.
pub(crate) async fn token(addr: Address) -> Result<(String, SystemTime), Error> {
    let region = resolve_region(&addr)?;
    let sdk_config = build_aws_sdk_config(&addr, &region).await;

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

    // RDS IAM tokens are valid for 15 minutes.
    let expires_at = SystemTime::now() + Duration::from_secs(900);
    Ok((token, expires_at))
}

#[cfg(test)]
mod tests {
    use pgdog_config::Role;

    use super::*;
    use crate::config::ServerAuth;
    use crate::test_utils::set_env_var;

    fn make_addr() -> Address {
        Address {
            host: "db.cluster-abc123.us-east-1.rds.amazonaws.com".into(),
            port: 5432,
            database_name: "postgres".into(),
            user: "db_user".into(),
            passwords: vec![],
            database_number: 0,
            server_auth: ServerAuth::RdsIam,
            server_iam_region: Some("us-east-1".into()),
            server_iam_assume_role: None,
            vault_path: Default::default(),
            vault_refresh_percent: None,
            configured_role: Role::Auto,
            ..Default::default()
        }
    }

    // ── AssumeRolePlan::new (cross-account RDS IAM) ──────────────────────────

    #[test]
    fn test_assume_role_plan_none_when_unset() {
        // Same-account path: no assume-role, token signed with ambient identity.
        let addr = make_addr();
        assert_eq!(AssumeRolePlan::new(&addr, "us-east-1"), None);
    }

    #[test]
    fn test_assume_role_plan_none_when_empty() {
        let mut addr = make_addr();
        addr.server_iam_assume_role = Some(String::new());
        assert_eq!(AssumeRolePlan::new(&addr, "us-east-1"), None);
    }

    #[test]
    fn test_assume_role_plan_set() {
        let mut addr = make_addr();
        addr.server_iam_assume_role =
            Some("arn:aws:iam::111122223333:role/pgdog-rds-connect".into());

        let plan = AssumeRolePlan::new(&addr, "us-west-2").expect("cross-account plan");
        assert_eq!(
            plan.role_arn,
            "arn:aws:iam::111122223333:role/pgdog-rds-connect"
        );
        // Region flows from the resolved token region, not a hard-coded default.
        assert_eq!(plan.region, "us-west-2");
        assert_eq!(plan.session_name, IAM_ASSUME_ROLE_SESSION_NAME);
    }

    // ── infer_region_from_rds_host ───────────────────────────────────────────

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
    fn test_infer_region_fails_when_rds_is_first_label() {
        let region = infer_region_from_rds_host("rds.amazonaws.com");
        assert!(region.is_none());
    }

    #[test]
    fn test_infer_region_fails_for_unknown_tld() {
        let region = infer_region_from_rds_host("db.us-east-1.rds.example.com");
        assert!(region.is_none());
    }

    // ── resolve_region ───────────────────────────────────────────────────────

    #[test]
    fn resolve_region_prefers_explicit_override() {
        let mut addr = make_addr();
        addr.server_iam_region = Some("eu-west-1".into());
        // Host implies us-east-1, but the explicit override must win.
        assert_eq!(resolve_region(&addr).unwrap(), "eu-west-1");
    }

    #[test]
    fn resolve_region_falls_back_to_host_inference() {
        let mut addr = make_addr();
        addr.server_iam_region = None;
        assert_eq!(resolve_region(&addr).unwrap(), "us-east-1");
    }

    #[test]
    fn resolve_region_treats_empty_override_as_absent() {
        let mut addr = make_addr();
        addr.server_iam_region = Some("".into());
        // Empty string must fall through to host inference.
        assert_eq!(resolve_region(&addr).unwrap(), "us-east-1");
    }

    #[test]
    fn resolve_region_errors_when_neither_override_nor_inference() {
        let addr = Address {
            host: "postgres.internal.example.com".into(),
            port: 5432,
            user: "u".into(),
            server_iam_region: None,
            ..Default::default()
        };
        assert!(resolve_region(&addr).is_err());
    }

    #[tokio::test]
    async fn test_token_contains_expected_query_fields() {
        let _access_key = set_env_var("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE");
        let _secret_key = set_env_var("AWS_SECRET_ACCESS_KEY", "SECRETEXAMPLE");
        let _session = set_env_var("AWS_SESSION_TOKEN", "SESSIONEXAMPLE");

        let addr = make_addr();
        let (token, expires_at) = token(addr).await.unwrap();

        assert!(expires_at > SystemTime::now());
        assert!(token.starts_with(
            "db.cluster-abc123.us-east-1.rds.amazonaws.com:5432/?Action=connect&DBUser=db_user"
        ));
        assert!(token.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
        assert!(token.contains("X-Amz-Credential="));
        assert!(token.contains("X-Amz-Signature="));
    }
}
