//! Connection pool settings, resolved from the configuration.

use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::{DurationMilliSeconds, serde_as};

use crate::{
    Database, EnumeratedDatabase, General, MAX_DURATION, PoolerMode, Role, User,
    pooling::ConnectionRecovery, prepared_statements::PreparedStatementsConfig,
};

/// Pool configuration.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, JsonSchema)]
pub struct PoolConfig {
    /// Minimum connections that should be in the pool.
    pub min: usize,
    /// Minimum connections that should be in the pool if it's a primary.
    pub min_primary: Option<usize>,
    /// Minimum connections that should be in the pool if it's a replica.
    pub min_replica: Option<usize>,
    /// Maximum connections allowed in the pool.
    pub max: usize,
    /// Maximum connection allowed in the pool if its a primary.
    pub max_primary: Option<usize>,
    /// Maximum connections allowed in the pool if its a replica.
    pub max_replica: Option<usize>,
    /// How long to wait for a connection before giving up.
    #[serde_as(as = "DurationMilliSeconds")]
    pub checkout_timeout: Duration, // ms
    /// Interval duration of DNS cache refresh.
    #[serde_as(as = "DurationMilliSeconds")]
    pub dns_ttl: Duration, // ms
    /// Close connections that have been idle for longer than this.
    #[serde_as(as = "DurationMilliSeconds")]
    pub idle_timeout: Duration, // ms
    /// Close primary connections that have been idle for longer than this.
    #[serde_as(as = "Option<DurationMilliSeconds>")]
    pub idle_timeout_primary: Option<Duration>, // ms
    /// Close replica connections that have been idle for longer than this.
    #[serde_as(as = "Option<DurationMilliSeconds>")]
    pub idle_timeout_replica: Option<Duration>, // ms
    /// How long to wait for connections to be created.
    #[serde_as(as = "DurationMilliSeconds")]
    pub connect_timeout: Duration, // ms
    /// How many times to attempt a connection before returning an error.
    pub connect_attempts: u64,
    /// How long to wait between connection attempts.
    #[serde_as(as = "DurationMilliSeconds")]
    pub connect_attempt_delay: Duration,
    /// How long a connection can be open.
    #[serde_as(as = "DurationMilliSeconds")]
    pub max_age: Duration,
    /// Maximum random adjustment applied to `max_age` per connection.
    /// Each connection samples a per-connection offset uniformly from
    /// `[-max_age_jitter, +max_age_jitter]` once at creation, breaking
    /// up synchronized retirement of cohorts that connect together.
    #[serde_as(as = "DurationMilliSeconds")]
    pub max_age_jitter: Duration,
    /// Can this pool be banned from serving traffic?
    pub bannable: bool,
    /// Healtheck timeout.
    #[serde_as(as = "DurationMilliSeconds")]
    pub healthcheck_timeout: Duration, // ms
    /// Healtcheck interval.
    #[serde_as(as = "DurationMilliSeconds")]
    pub healthcheck_interval: Duration, // ms
    /// Idle healthcheck interval.
    #[serde_as(as = "DurationMilliSeconds")]
    pub idle_healthcheck_interval: Duration, // ms
    /// Idle healthcheck delay.
    #[serde_as(as = "DurationMilliSeconds")]
    pub idle_healthcheck_delay: Duration, // ms
    /// Should new servers on config reload wait for a successful health
    /// check to be added to the load balancer?
    pub require_healthcheck_on_discovery: bool,
    /// Read timeout (dangerous).
    #[serde_as(as = "DurationMilliSeconds")]
    pub read_timeout: Duration, // ms
    /// Write timeout (dangerous).
    #[serde_as(as = "DurationMilliSeconds")]
    pub write_timeout: Duration, // ms
    /// Query timeout (dangerous).
    #[serde_as(as = "DurationMilliSeconds")]
    pub query_timeout: Duration, // ms
    /// Max ban duration.
    #[serde_as(as = "DurationMilliSeconds")]
    pub ban_timeout: Duration, // ms
    /// Rollback timeout for dirty connections.
    #[serde_as(as = "DurationMilliSeconds")]
    pub rollback_timeout: Duration,
    /// Statement timeout
    #[serde_as(as = "Option<DurationMilliSeconds>")]
    pub statement_timeout: Option<Duration>,
    /// Lock timeout
    #[serde_as(as = "Option<DurationMilliSeconds>")]
    pub lock_timeout: Option<Duration>,
    /// Replication mode.
    pub replication_mode: bool,
    /// Pooler mode.
    pub pooler_mode: PoolerMode,
    /// Read only mode.
    pub read_only: bool,
    /// Prepared statements config.
    pub prepared_statements: PreparedStatementsConfig,
    /// Stats averaging period.
    #[serde_as(as = "DurationMilliSeconds")]
    pub stats_period: Duration,
    /// Recovery algo.
    pub connection_recovery: ConnectionRecovery,
    /// LSN check interval.
    #[serde_as(as = "DurationMilliSeconds")]
    pub lsn_check_interval: Duration,
    /// LSN check timeout.
    #[serde_as(as = "DurationMilliSeconds")]
    pub lsn_check_timeout: Duration,
    /// LSN check delay.
    #[serde_as(as = "DurationMilliSeconds")]
    pub lsn_check_delay: Duration,
    /// Automatic role detection enabled.
    pub role_detection: bool,
    /// Used for resharding only.
    pub resharding_only: bool,
    /// LB weight.
    pub lb_weight: u8,
}

impl PoolConfig {
    /// Resolve the settings of one connection pool: the `database` entry of
    /// `shard`, as `user` sees it.
    ///
    /// Precedence is user, then database, then general.
    pub fn resolve(
        general: &General,
        shard: &ShardNodes<'_>,
        database: &Database,
        user: &User,
    ) -> Self {
        Self {
            min: user
                .min_pool_size
                .unwrap_or(database.min_pool_size.unwrap_or(general.min_pool_size)),
            min_primary: user
                .role_config
                .min_pool_size_primary
                .or(database.role_config.min_pool_size_primary),
            min_replica: user
                .role_config
                .min_pool_size_replica
                .or(database.role_config.min_pool_size_replica),
            max: user
                .pool_size
                .unwrap_or(database.pool_size.unwrap_or(general.default_pool_size)),
            max_primary: user
                .role_config
                .pool_size_primary
                .or(database.role_config.pool_size_primary),
            max_replica: user
                .role_config
                .pool_size_replica
                .or(database.role_config.pool_size_replica),
            max_age: Duration::from_millis(
                user.server_lifetime
                    .unwrap_or(database.server_lifetime.unwrap_or(general.server_lifetime)),
            ),
            max_age_jitter: Duration::from_millis(
                user.server_lifetime_jitter.unwrap_or(
                    database
                        .server_lifetime_jitter
                        .unwrap_or(general.server_lifetime_jitter),
                ),
            ),
            healthcheck_interval: Duration::from_millis(general.healthcheck_interval),
            idle_healthcheck_interval: Duration::from_millis(general.idle_healthcheck_interval),
            idle_healthcheck_delay: Duration::from_millis(general.idle_healthcheck_delay),
            healthcheck_timeout: Duration::from_millis(general.healthcheck_timeout),
            ban_timeout: Duration::from_millis(general.ban_timeout),
            rollback_timeout: Duration::from_millis(general.rollback_timeout),
            statement_timeout: user
                .statement_timeout
                .or(database.statement_timeout)
                .map(Duration::from_millis),
            lock_timeout: user
                .lock_timeout
                .or(database.lock_timeout)
                .map(Duration::from_millis),
            replication_mode: user.replication_mode,
            pooler_mode: user
                .pooler_mode
                .unwrap_or(database.pooler_mode.unwrap_or(general.pooler_mode)),
            connect_timeout: Duration::from_millis(general.connect_timeout),
            connect_attempts: general.connect_attempts,
            connect_attempt_delay: general.connect_attempt_delay(),
            query_timeout: Duration::from_millis(general.query_timeout),
            checkout_timeout: Duration::from_millis(general.checkout_timeout),
            idle_timeout: Duration::from_millis(
                user.idle_timeout
                    .unwrap_or(database.idle_timeout.unwrap_or(general.idle_timeout)),
            ),
            idle_timeout_primary: user
                .role_config
                .idle_timeout_primary
                .or(database.role_config.idle_timeout_primary)
                .map(Duration::from_millis),
            idle_timeout_replica: user
                .role_config
                .idle_timeout_replica
                .or(database.role_config.idle_timeout_replica)
                .map(Duration::from_millis),
            require_healthcheck_on_discovery: general.require_healthcheck_on_discovery,
            read_only: user
                .read_only
                .unwrap_or(database.read_only.unwrap_or_default()),
            prepared_statements: PreparedStatementsConfig {
                level: general.prepared_statements,
                limit: general.prepared_statements_limit,
                ttl: general.prepared_statements_ttl(),
                ttl_jitter: general.prepared_statements_ttl_jitter(),
            },
            stats_period: Duration::from_millis(general.stats_period),
            bannable: shard.bannable(),
            connection_recovery: general.connection_recovery,
            lsn_check_interval: Duration::from_millis(general.lsn_check_interval),
            lsn_check_timeout: Duration::from_millis(general.lsn_check_timeout),
            lsn_check_delay: Duration::from_millis(general.lsn_check_delay),
            role_detection: database.is_role_auto(),
            resharding_only: database.resharding_only,
            lb_weight: database.lb_weight,
            ..Default::default()
        }
    }
}

pub struct RoleSpecificConfig<T: Copy> {
    pub value: T,
    pub value_primary: Option<T>,
    pub value_replica: Option<T>,
}

impl<T: Copy> RoleSpecificConfig<T> {
    /// Get value given role.
    pub fn value(&self, role: Role) -> T {
        match role {
            Role::Auto | Role::Replica => self.value_replica.unwrap_or(self.value),
            Role::Primary => self.value_primary.unwrap_or(self.value),
        }
    }
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min: 1,
            min_primary: None,
            min_replica: None,
            max: 10,
            max_primary: None,
            max_replica: None,
            checkout_timeout: Duration::from_millis(5_000),
            idle_timeout: Duration::from_millis(60_000),
            idle_timeout_primary: None,
            idle_timeout_replica: None,
            connect_timeout: Duration::from_millis(5_000),
            connect_attempts: 1,
            connect_attempt_delay: Duration::from_millis(10),
            max_age: Duration::from_millis(24 * 3600 * 1000),
            max_age_jitter: Duration::ZERO,
            bannable: true,
            healthcheck_timeout: Duration::from_millis(5_000),
            healthcheck_interval: Duration::from_millis(30_000),
            idle_healthcheck_interval: Duration::from_millis(5_000),
            idle_healthcheck_delay: Duration::from_millis(5_000),
            require_healthcheck_on_discovery: false,
            read_timeout: MAX_DURATION,
            write_timeout: MAX_DURATION,
            query_timeout: MAX_DURATION,
            ban_timeout: Duration::from_secs(300),
            rollback_timeout: Duration::from_secs(5),
            statement_timeout: None,
            lock_timeout: None,
            replication_mode: false,
            pooler_mode: PoolerMode::default(),
            read_only: false,
            prepared_statements: PreparedStatementsConfig::default(),
            stats_period: Duration::from_millis(15_000),
            dns_ttl: Duration::from_millis(60_000),
            connection_recovery: ConnectionRecovery::Recover,
            lsn_check_interval: Duration::from_millis(5_000),
            lsn_check_timeout: Duration::from_millis(5_000),
            lsn_check_delay: Duration::from_millis(5_000),
            role_detection: false,
            resharding_only: false,
            lb_weight: 255,
        }
    }
}

/// The `[[databases]]` entries of one shard, in the order PgDog pools them.
///
/// Build it from one element of a cluster of [`crate::Config::databases`].
#[derive(Debug, Clone, Copy)]
pub struct ShardNodes<'a> {
    entries: &'a [EnumeratedDatabase],
}

impl<'a> ShardNodes<'a> {
    pub fn new(entries: &'a [EnumeratedDatabase]) -> Self {
        Self { entries }
    }

    /// The first `primary` entry. PgDog ignores any later one.
    pub fn primary(&self) -> Option<&'a EnumeratedDatabase> {
        self.entries.iter().find(|node| node.role == Role::Primary)
    }

    /// The `replica` entries, plus every `auto` entry.
    pub fn replicas(&self) -> impl Iterator<Item = &'a EnumeratedDatabase> {
        self.entries
            .iter()
            .filter(|node| matches!(node.role, Role::Replica | Role::Auto))
    }

    /// Every entry PgDog builds a pool for, the primary first.
    pub fn pools(&self) -> impl Iterator<Item = &'a EnumeratedDatabase> {
        self.primary().into_iter().chain(self.replicas())
    }

    /// Banning the only node of a shard would leave nothing to serve traffic.
    /// An ignored second primary still counts: it can take over on failover.
    pub fn bannable(&self) -> bool {
        self.entries.len() > 1
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn settings_render_durations_as_milliseconds() {
        let config = PoolConfig::default();
        let value = serde_json::to_value(config).unwrap();

        assert_eq!(value["checkout_timeout"], 5000);
        assert_eq!(value["idle_timeout_primary"], serde_json::Value::Null);
        assert_eq!(value["statement_timeout"], serde_json::Value::Null);
        assert_eq!(
            value["query_timeout"],
            serde_json::json!(MAX_DURATION.as_millis() as u64)
        );
        assert_eq!(value["prepared_statements"]["ttl_jitter"], 0);
        assert_eq!(value["prepared_statements"]["ttl"], serde_json::Value::Null);

        let parsed: PoolConfig = serde_json::from_value(value).unwrap();
        assert_eq!(parsed, config);
    }

    fn database(role: Role) -> Database {
        Database {
            name: "test".into(),
            role,
            host: "localhost".into(),
            port: 5432,
            ..Default::default()
        }
    }

    /// Resolve one entry of a two-node shard, so `bannable` stays true.
    fn resolve(general: &General, database: &Database, user: &User) -> PoolConfig {
        let entries = vec![
            EnumeratedDatabase {
                number: 0,
                database: database.clone(),
            },
            EnumeratedDatabase {
                number: 1,
                database: database.clone(),
            },
        ];

        PoolConfig::resolve(general, &ShardNodes::new(&entries), database, user)
    }

    #[test]
    fn role_auto_enables_role_detection() {
        let config = resolve(&General::default(), &database(Role::Auto), &User::default());
        assert!(config.role_detection);
    }

    #[test]
    fn role_primary_disables_role_detection() {
        let config = resolve(
            &General::default(),
            &database(Role::Primary),
            &User::default(),
        );
        assert!(!config.role_detection);
    }

    #[test]
    fn role_replica_disables_role_detection() {
        let config = resolve(
            &General::default(),
            &database(Role::Replica),
            &User::default(),
        );
        assert!(!config.role_detection);
    }

    #[test]
    fn prepared_statements_come_from_general() {
        let general = General {
            prepared_statements_ttl: Some(60_000),
            prepared_statements_limit: 10,
            ..Default::default()
        };

        let config = resolve(&general, &database(Role::Primary), &User::default());

        assert_eq!(
            config.prepared_statements.ttl,
            Some(Duration::from_millis(60_000))
        );
        assert_eq!(config.prepared_statements.limit, 10);
        assert_eq!(
            config.prepared_statements.level,
            general.prepared_statements
        );
    }

    #[test]
    fn user_takes_precedence_over_database() {
        let user = User {
            pool_size: Some(5),
            min_pool_size: Some(5),
            server_lifetime: Some(5),
            server_lifetime_jitter: Some(1),
            statement_timeout: Some(5),
            lock_timeout: Some(7),
            pooler_mode: Some(PoolerMode::Session),
            idle_timeout: Some(5),
            read_only: Some(true),
            ..Default::default()
        };

        let database = Database {
            pool_size: Some(10),
            min_pool_size: Some(10),
            server_lifetime: Some(10),
            server_lifetime_jitter: Some(2),
            statement_timeout: Some(10),
            lock_timeout: Some(11),
            pooler_mode: Some(PoolerMode::Transaction),
            idle_timeout: Some(10),
            read_only: Some(false),
            ..Default::default()
        };

        let config = resolve(&General::default(), &database, &user);

        assert_eq!(5, config.max);
        assert_eq!(5, config.min);
        assert_eq!(Duration::from_millis(5), config.max_age);
        assert_eq!(Duration::from_millis(1), config.max_age_jitter);
        assert_eq!(Some(Duration::from_millis(5)), config.statement_timeout);
        assert_eq!(Some(Duration::from_millis(7)), config.lock_timeout);
        assert_eq!(PoolerMode::Session, config.pooler_mode);
        assert_eq!(Duration::from_millis(5), config.idle_timeout);
        assert!(config.read_only);
    }

    #[test]
    fn role_specific_settings_prefer_the_user() {
        let mut user = User::default();
        user.role_config.min_pool_size_primary = Some(2);
        user.role_config.idle_timeout_primary = Some(20);

        let mut database = Database::default();
        database.role_config.min_pool_size_primary = Some(3);
        database.role_config.min_pool_size_replica = Some(4);
        database.role_config.idle_timeout_primary = Some(30);
        database.role_config.idle_timeout_replica = Some(40);

        let config = resolve(&General::default(), &database, &user);

        assert_eq!(config.min_primary, Some(2));
        assert_eq!(config.min_replica, Some(4));
        assert_eq!(config.idle_timeout_primary, Some(Duration::from_millis(20)));
        assert_eq!(config.idle_timeout_replica, Some(Duration::from_millis(40)));
    }

    #[test]
    fn jitter_falls_through_general_to_database_to_user() {
        let general = General {
            server_lifetime_jitter: 100,
            ..General::default()
        };

        let config = resolve(&general, &Database::default(), &User::default());
        assert_eq!(Duration::from_millis(100), config.max_age_jitter);

        let database = Database {
            server_lifetime_jitter: Some(200),
            ..Default::default()
        };
        let config = resolve(&general, &database, &User::default());
        assert_eq!(Duration::from_millis(200), config.max_age_jitter);

        let user = User {
            server_lifetime_jitter: Some(300),
            ..Default::default()
        };
        let config = resolve(&general, &database, &user);
        assert_eq!(Duration::from_millis(300), config.max_age_jitter);
    }

    #[test]
    fn jitter_defaults_to_zero() {
        let config = resolve(&General::default(), &Database::default(), &User::default());
        assert_eq!(Duration::ZERO, config.max_age_jitter);
    }

    fn cluster(shards: Vec<Vec<Database>>) -> Vec<Vec<EnumeratedDatabase>> {
        let mut number = 0;
        shards
            .into_iter()
            .map(|shard| {
                shard
                    .into_iter()
                    .map(|database| {
                        number += 1;
                        EnumeratedDatabase {
                            number: number - 1,
                            database,
                        }
                    })
                    .collect()
            })
            .collect()
    }

    #[test]
    fn shard_nodes_put_the_primary_first() {
        let shards = cluster(vec![vec![
            database(Role::Replica),
            database(Role::Primary),
            database(Role::Auto),
        ]]);
        let shard = ShardNodes::new(&shards[0]);

        let roles: Vec<Role> = shard.pools().map(|node| node.role).collect();
        assert_eq!(roles, vec![Role::Primary, Role::Replica, Role::Auto]);
    }

    #[test]
    fn shard_nodes_ignore_a_second_primary() {
        let shards = cluster(vec![vec![
            database(Role::Primary),
            database(Role::Primary),
            database(Role::Replica),
        ]]);
        let shard = ShardNodes::new(&shards[0]);

        let roles: Vec<Role> = shard.pools().map(|node| node.role).collect();
        assert_eq!(roles, vec![Role::Primary, Role::Replica]);
    }

    #[test]
    fn shard_nodes_keep_a_shard_without_a_primary() {
        let shards = cluster(vec![vec![database(Role::Replica)]]);
        let shard = ShardNodes::new(&shards[0]);

        assert!(shard.primary().is_none());
        assert_eq!(shard.replicas().count(), 1);
    }

    #[test]
    fn a_pool_is_bannable_only_when_the_shard_has_another_node() {
        let alone = cluster(vec![vec![database(Role::Primary)]]);
        let pair = cluster(vec![vec![database(Role::Primary), database(Role::Replica)]]);
        let general = General::default();
        let user = User::default();

        let shard = ShardNodes::new(&alone[0]);
        let node = shard.primary().unwrap();
        assert!(!PoolConfig::resolve(&general, &shard, &node.database, &user).bannable);

        let shard = ShardNodes::new(&pair[0]);
        let node = shard.primary().unwrap();
        assert!(PoolConfig::resolve(&general, &shard, &node.database, &user).bannable);
    }

    #[test]
    fn each_shard_resolves_on_its_own() {
        let mut slow = database(Role::Primary);
        slow.pool_size = Some(40);
        let shards = cluster(vec![vec![database(Role::Primary)], vec![slow]]);
        let general = General::default();
        let user = User::default();

        let sizes: Vec<usize> = shards
            .iter()
            .map(|entries| {
                let shard = ShardNodes::new(entries);
                let node = shard.primary().unwrap();
                PoolConfig::resolve(&general, &shard, &node.database, &user).max
            })
            .collect();

        assert_eq!(sizes, vec![10, 40]);
    }
}
