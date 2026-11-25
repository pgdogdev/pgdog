use pgdog_config::{Config, ConfigAndUsers, ShardedMappingConfig, Users};

const CONFIG: &str = r#"
[general]
port = 6433

[admin]
password = "admin"

[[databases]]
name = "prod"
host = "127.0.0.1"
tls_verify = "disabled"
pool_size_primary = 5

[[sharded_tables]]
database = "prod"
column = "tenant_id"

[[mirroring]]
source_db = "prod"
destination_db = "prod"
"#;

const CONFIG_WITH_UNKNOWN_FIELDS: &str = r#"
unknown_top_level = 1

[general]
port = 6433
unknown_general = true

[admin]
password = "admin"
unknown_admin = 1

[unknown_section]
key = 1

[[databases]]
name = "prod"
host = "127.0.0.1"
tls_verify = "disabled"
pool_size_primary = 5
unknown_database = "x"

[[sharded_tables]]
database = "prod"
column = "tenant_id"
unknown_sharded_table = 1

[[mirroring]]
source_db = "prod"
destination_db = "prod"
unknown_mirroring = 1
"#;

const USERS: &str = r#"
[[users]]
name = "alice"
database = "prod"
password = "secret"
pool_size_primary = 5
"#;

const USERS_WITH_UNKNOWN_FIELDS: &str = r#"
unknown_top_level = 1

[[users]]
name = "alice"
database = "prod"
password = "secret"
pool_size_primary = 5
unknown_user = 1
"#;

#[test]
fn toml_config_ignores_unknown_fields() {
    let expected: Config = toml::from_str(CONFIG).unwrap();
    let config: Config = toml::from_str(CONFIG_WITH_UNKNOWN_FIELDS).unwrap();

    assert_eq!(config, expected);
    assert_eq!(config.general.port, 6433);
    assert_eq!(config.databases[0].role_config.pool_size_primary, Some(5));
}

#[test]
fn toml_users_ignore_unknown_fields() {
    let expected: Users = toml::from_str(USERS).unwrap();
    let users: Users = toml::from_str(USERS_WITH_UNKNOWN_FIELDS).unwrap();

    assert_eq!(users, expected);
    assert_eq!(users.users[0].role_config.pool_size_primary, Some(5));
}

#[test]
fn json_config_and_users_ignore_unknown_fields() {
    let source = r#"{
        "config_path": "pgdog.toml",
        "users_path": "users.toml",
        "config": {
            "general": { "port": 6433 },
            "databases": [{
                "name": "prod",
                "host": "127.0.0.1",
                "tls_verify": "disabled",
                "pool_size_primary": 5
            }]
        },
        "users": {
            "users": [{ "name": "alice", "database": "prod", "pool_size_primary": 5 }]
        }
    }"#;
    let source_with_unknown_fields = r#"{
        "unknown_top_level": 1,
        "config_path": "pgdog.toml",
        "users_path": "users.toml",
        "config": {
            "unknown_config": 1,
            "unknown_section": { "key": 1 },
            "general": { "port": 6433, "unknown_general": true },
            "databases": [{
                "name": "prod",
                "host": "127.0.0.1",
                "tls_verify": "disabled",
                "pool_size_primary": 5,
                "unknown_database": "x"
            }]
        },
        "users": {
            "unknown_users": 1,
            "users": [{
                "name": "alice",
                "database": "prod",
                "pool_size_primary": 5,
                "unknown_user": 1
            }]
        }
    }"#;

    let expected: ConfigAndUsers = serde_json::from_str(source).unwrap();
    let config: ConfigAndUsers = serde_json::from_str(source_with_unknown_fields).unwrap();

    assert_eq!(config.config, expected.config);
    assert_eq!(config.users, expected.users);
    assert_eq!(config.config_path, expected.config_path);
    assert_eq!(config.users_path, expected.users_path);
}

#[test]
fn sharded_mapping_rules_keep_their_variant() {
    let source = r#"
[[sharded_tables]]
database = "prod"
column = "tenant_id"

[[sharded_tables.mapping]]
values = [1, 2]
shard = 0

[[sharded_tables.mapping]]
start = 10
end = 20
shard = 1

[[sharded_tables.mapping]]
shard = 2
"#;
    let config: Config = toml::from_str(source).unwrap();
    let mapping = config.sharded_tables[0].mapping.as_ref().unwrap();

    assert!(matches!(mapping[0], ShardedMappingConfig::List(_)));
    assert!(matches!(mapping[1], ShardedMappingConfig::Range(_)));
    assert!(matches!(mapping[2], ShardedMappingConfig::Default { .. }));
}
