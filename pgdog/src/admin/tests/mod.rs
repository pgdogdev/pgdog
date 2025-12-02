use crate::admin::Command;
use crate::backend::databases::{databases, from_config, replace_databases, Databases};
use crate::backend::pool::cluster_stats::MirrorStats;
use crate::config::{self, ConfigAndUsers, Database, Role, User as ConfigUser};
use crate::net::messages::{DataRow, DataType, FromBytes, Protocol, RowDescription};

use super::show_client_memory::ShowClientMemory;
use super::show_config::ShowConfig;
use super::show_lists::ShowLists;
use super::show_mirrors::ShowMirrors;
use super::show_pools::ShowPools;
use super::show_server_memory::ShowServerMemory;

#[derive(Clone)]
struct SavedState {
    config: ConfigAndUsers,
    databases: Databases,
}

/// Minimal harness to snapshot and restore admin singletons touched by SHOW commands.
pub(crate) struct TestAdminContext {
    original: SavedState,
}

impl TestAdminContext {
    pub(crate) fn new() -> Self {
        let config = (*config::config()).clone();
        let databases = (*databases()).clone();

        Self {
            original: SavedState { config, databases },
        }
    }

    pub(crate) fn set_config(&self, config: ConfigAndUsers) {
        // Update the live config first so downstream readers pick up test values.
        config::set(config.clone()).expect("failed to install test config");

        // Rebuild the in-process database registry from the supplied config.
        replace_databases(from_config(&config), false);
    }
}

impl Default for TestAdminContext {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TestAdminContext {
    fn drop(&mut self) {
        // Restore the original configuration and database registry so other tests remain isolated.
        let _ = config::set(self.original.config.clone());
        replace_databases(self.original.databases.clone(), false);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn show_pools_reports_schema_admin_flag() {
    let context = TestAdminContext::new();

    let mut config = ConfigAndUsers::default();
    config.config.databases.push(Database {
        name: "app".into(),
        host: "127.0.0.1".into(),
        role: Role::Primary,
        shard: 0,
        ..Default::default()
    });
    config.users.users.push(ConfigUser {
        name: "alice".into(),
        database: "app".into(),
        password: Some("secret".into()),
        schema_admin: true,
        ..Default::default()
    });

    context.set_config(config);

    let command = ShowPools;
    let messages = command
        .execute()
        .await
        .expect("show pools execution failed");

    assert!(
        messages.len() >= 2,
        "expected row description plus data row"
    );

    let row_description = RowDescription::from_bytes(messages[0].payload())
        .expect("row description message should parse");
    let actual_names: Vec<&str> = row_description
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect();
    let expected_names = vec![
        "id",
        "database",
        "user",
        "addr",
        "port",
        "shard",
        "role",
        "cl_waiting",
        "sv_idle",
        "sv_active",
        "sv_idle_xact",
        "sv_total",
        "maxwait",
        "maxwait_us",
        "pool_mode",
        "paused",
        "banned",
        "healthy",
        "errors",
        "re_synced",
        "out_of_sync",
        "force_closed",
        "online",
        "schema_admin",
    ];
    assert_eq!(actual_names, expected_names);

    let schema_admin_field = row_description
        .field_index("schema_admin")
        .and_then(|idx| row_description.field(idx))
        .expect("schema_admin field present");
    assert_eq!(schema_admin_field.data_type(), DataType::Bool);

    let data_row = DataRow::from_bytes(messages[1].payload()).expect("data row should parse");
    let schema_admin_index = row_description
        .field_index("schema_admin")
        .expect("schema_admin column index");
    let schema_admin_value = data_row
        .get_text(schema_admin_index)
        .expect("schema_admin value should be textual");
    assert_eq!(schema_admin_value.as_str(), "t");
}

#[tokio::test(flavor = "current_thread")]
async fn show_config_pretty_prints_general_settings() {
    let context = TestAdminContext::new();

    let mut config = ConfigAndUsers::default();
    config.config.general.default_pool_size = 42;
    config.config.general.connect_timeout = 2_000;

    context.set_config(config);

    let command = ShowConfig;
    let messages = command
        .execute()
        .await
        .expect("show config execution failed");

    assert!(messages.len() > 1, "expected row description and rows");

    let row_description = RowDescription::from_bytes(messages[0].payload())
        .expect("row description message should parse");
    let column_names: Vec<&str> = row_description
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect();
    assert_eq!(column_names, vec!["name", "value"]);

    for field in row_description.fields.iter() {
        assert_eq!(field.data_type(), DataType::Text);
    }

    let rows: Vec<(String, String)> = messages
        .iter()
        .skip(1)
        .map(|message| {
            let row =
                DataRow::from_bytes(message.payload()).expect("data row should parse into columns");
            let name = row.get_text(0).expect("name column should be text");
            let value = row.get_text(1).expect("value column should be text");
            (name, value)
        })
        .collect();

    let pool_size = rows
        .iter()
        .find(|(name, _)| name == "default_pool_size")
        .map(|(_, value)| value)
        .expect("default_pool_size row should be present");
    assert_eq!(pool_size, "42");

    let connect_timeout = rows
        .iter()
        .find(|(name, _)| name == "connect_timeout")
        .map(|(_, value)| value)
        .expect("connect_timeout row should be present");
    assert_eq!(connect_timeout, "2s");
}

#[tokio::test(flavor = "current_thread")]
async fn show_mirrors_reports_counts() {
    let context = TestAdminContext::new();

    let mut config = ConfigAndUsers::default();
    config.config.databases.push(Database {
        name: "app".into(),
        host: "127.0.0.1".into(),
        role: Role::Primary,
        shard: 0,
        ..Default::default()
    });
    config.users.users.push(ConfigUser {
        name: "alice".into(),
        database: "app".into(),
        password: Some("secret".into()),
        ..Default::default()
    });

    context.set_config(config);

    // Seed synthetic mirror stats for the configured cluster.
    let databases = databases();
    let (user, cluster) = databases.all().iter().next().expect("cluster should exist");
    assert_eq!(user.database, "app");
    assert_eq!(user.user, "alice");
    {
        let cluster_stats = cluster.stats();
        let mut stats = cluster_stats.lock();
        stats.mirrors = MirrorStats {
            total_count: 5,
            mirrored_count: 4,
            dropped_count: 1,
            error_count: 2,
            queue_length: 3,
        };
    }

    let command = ShowMirrors;
    let messages = command
        .execute()
        .await
        .expect("show mirrors execution failed");

    assert_eq!(messages[0].code(), 'T');
    let row_description =
        RowDescription::from_bytes(messages[0].payload()).expect("row description should parse");
    let expected_columns = [
        "database",
        "user",
        "total_count",
        "mirrored_count",
        "dropped_count",
        "error_count",
        "queue_length",
    ];
    let actual_columns: Vec<&str> = row_description
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect();
    assert_eq!(actual_columns, expected_columns);

    assert!(messages.len() > 1, "expected at least one data row");
    let data_row = DataRow::from_bytes(messages[1].payload()).expect("data row should parse");
    assert_eq!(data_row.get_text(0).as_deref(), Some("app"));
    assert_eq!(data_row.get_text(1).as_deref(), Some("alice"));
    assert_eq!(data_row.get_int(2, true), Some(5));
    assert_eq!(data_row.get_int(3, true), Some(4));
    assert_eq!(data_row.get_int(4, true), Some(1));
    assert_eq!(data_row.get_int(5, true), Some(2));
    assert_eq!(data_row.get_int(6, true), Some(3));
}

#[tokio::test(flavor = "current_thread")]
async fn show_lists_reports_basic_counts() {
    let context = TestAdminContext::new();

    let mut config = ConfigAndUsers::default();
    config.config.databases.push(Database {
        name: "app".into(),
        host: "127.0.0.1".into(),
        role: Role::Primary,
        shard: 0,
        ..Default::default()
    });
    config.users.users.push(ConfigUser {
        name: "alice".into(),
        database: "app".into(),
        password: Some("secret".into()),
        ..Default::default()
    });

    context.set_config(config.clone());

    let command = ShowLists;
    let messages = command
        .execute()
        .await
        .expect("show lists execution failed");

    assert_eq!(messages.len(), 2, "expected row description plus one row");

    let row_description =
        RowDescription::from_bytes(messages[0].payload()).expect("row description should parse");
    let column_names: Vec<&str> = row_description
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect();
    let expected_columns = vec![
        "databases",
        "users",
        "pools",
        "used_clients",
        "used_clients",
        "free_servers",
        "used_servers",
    ];
    assert_eq!(column_names, expected_columns);

    for field in row_description.fields.iter() {
        assert_eq!(field.data_type(), DataType::Numeric);
    }

    let data_row = DataRow::from_bytes(messages[1].payload()).expect("data row should parse");

    let database_count = data_row.get_int(0, true).expect("databases value");
    let user_count = data_row.get_int(1, true).expect("users value");
    let pool_count = data_row.get_int(2, true).expect("pools value");
    let used_clients_placeholder = data_row.get_int(3, true).expect("used clients placeholder");
    let used_clients = data_row.get_int(4, true).expect("used clients value");
    let free_servers = data_row.get_int(5, true).expect("free servers value");
    let used_servers = data_row.get_int(6, true).expect("used servers value");

    assert_eq!(database_count, config.config.databases.len() as i64);
    assert_eq!(user_count, config.users.users.len() as i64);

    let expected_pools: usize = databases()
        .all()
        .values()
        .map(|cluster| {
            cluster
                .shards()
                .iter()
                .map(|shard| shard.pools().len())
                .sum::<usize>()
        })
        .sum();
    assert_eq!(pool_count, expected_pools as i64);

    assert_eq!(used_clients_placeholder, 0);
    assert_eq!(used_clients, 0);
    assert_eq!(free_servers, 0);
    assert_eq!(used_servers, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn show_server_memory_reports_memory_stats() {
    let command = ShowServerMemory;
    let messages = command
        .execute()
        .await
        .expect("show server memory execution failed");

    assert!(messages.len() >= 1, "expected at least row description");

    let row_description = RowDescription::from_bytes(messages[0].payload())
        .expect("row description message should parse");
    let actual_names: Vec<&str> = row_description
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect();
    let expected_names = vec![
        "pool_id",
        "database",
        "user",
        "addr",
        "port",
        "remote_pid",
        "buffer_reallocs",
        "buffer_reclaims",
        "buffer_bytes_used",
        "buffer_bytes_alloc",
        "prepared_statements_bytes",
        "net_buffer_bytes",
        "total_bytes",
    ];
    assert_eq!(actual_names, expected_names);

    for (idx, expected_type) in [
        DataType::Bigint,
        DataType::Text,
        DataType::Text,
        DataType::Text,
        DataType::Numeric,
        DataType::Numeric,
        DataType::Numeric,
        DataType::Numeric,
        DataType::Numeric,
        DataType::Numeric,
        DataType::Numeric,
        DataType::Numeric,
        DataType::Numeric,
    ]
    .iter()
    .enumerate()
    {
        let field = row_description.field(idx).expect("field should exist");
        assert_eq!(field.data_type(), *expected_type);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn show_client_memory_reports_memory_stats() {
    let command = ShowClientMemory;
    let messages = command
        .execute()
        .await
        .expect("show client memory execution failed");

    assert!(messages.len() >= 1, "expected at least row description");

    let row_description = RowDescription::from_bytes(messages[0].payload())
        .expect("row description message should parse");
    let actual_names: Vec<&str> = row_description
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect();
    let expected_names = vec![
        "client_id",
        "database",
        "user",
        "addr",
        "port",
        "buffer_reallocs",
        "buffer_reclaims",
        "buffer_bytes_used",
        "buffer_bytes_alloc",
        "prepared_statements_bytes",
        "net_buffer_bytes",
        "total_bytes",
    ];
    assert_eq!(actual_names, expected_names);

    for (idx, expected_type) in [
        DataType::Bigint,
        DataType::Text,
        DataType::Text,
        DataType::Text,
        DataType::Numeric,
        DataType::Numeric,
        DataType::Numeric,
        DataType::Numeric,
        DataType::Numeric,
        DataType::Numeric,
        DataType::Numeric,
        DataType::Numeric,
    ]
    .iter()
    .enumerate()
    {
        let field = row_description.field(idx).expect("field should exist");
        assert_eq!(field.data_type(), *expected_type);
    }
}
