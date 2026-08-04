use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Database-role specific configuration.
#[derive(
    Serialize, Deserialize, Debug, Clone, PartialEq, Default, JsonSchema, PartialOrd, Eq, Ord,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct RoleConfig {
    /// Maximum pool size for the primary database in the cluster.
    pub pool_size_primary: Option<usize>,
    /// Maximum pool size for the replica databases in the cluster.
    pub pool_size_replica: Option<usize>,
    /// Minimum pool size for the primary database in the cluster.
    pub min_pool_size_primary: Option<usize>,
    /// Minimum pool size for the replica databases in the cluster.
    pub min_pool_size_replica: Option<usize>,
    /// Idle connection timeout for the primary database in the cluster.
    pub idle_timeout_primary: Option<u64>,
    /// Idle connection timeout for the replica databases in the cluster.
    pub idle_timeout_replica: Option<u64>,
}

#[cfg(test)]
mod test {
    use super::super::{Config, Users};

    #[test]
    fn test_role_config_with_databases() {
        let config = r#"
[[databases]]
name = "prod"
host = "127.0.0.1"
pool_size_primary = 10
pool_size_replica = 20
min_pool_size_primary = 2
min_pool_size_replica = 4
idle_timeout_primary = 500
idle_timeout_replica = 1_000
            "#;

        let config: Config = toml::from_str(config).unwrap();
        let role_config = &config.databases[0].role_config;

        assert_eq!(role_config.pool_size_primary, Some(10));
        assert_eq!(role_config.pool_size_replica, Some(20));
        assert_eq!(role_config.min_pool_size_primary, Some(2));
        assert_eq!(role_config.min_pool_size_replica, Some(4));
        assert_eq!(role_config.idle_timeout_primary, Some(500));
        assert_eq!(role_config.idle_timeout_replica, Some(1_000));
    }

    #[test]
    fn test_role_config_with_users() {
        let config = r#"
[[users]]
name = "pgdog"
database = "prod"
pool_size_primary = 10
min_pool_size_primary = 2
idle_timeout_primary = 500
idle_timeout_replica = 1_000
            "#;

        let users: Users = toml::from_str(&config).unwrap();
        assert_eq!(users.users[0].role_config.pool_size_primary, Some(10));
        assert_eq!(users.users[0].role_config.min_pool_size_primary, Some(2));
        assert_eq!(users.users[0].role_config.idle_timeout_primary, Some(500));
        assert_eq!(users.users[0].role_config.idle_timeout_replica, Some(1_000));
    }
}
