use pgdog_config::{PassthroughAuth, Role};

use crate::{
    backend::databases::{
        add, databases, from_config, reload_from_existing, replace_databases, reset_stats,
    },
    config::{ConfigAndUsers, Database, User, set},
};

use super::{Pool, Request};

fn config() -> ConfigAndUsers {
    let mut config = ConfigAndUsers::default();
    config.config.general.passthrough_auth = PassthroughAuth::EnabledPlain;
    config.config.general.min_pool_size = 0;
    config.config.databases.push(Database {
        name: "passthrough_stats".into(),
        host: "127.0.0.1".into(),
        port: 5432,
        role: Role::Primary,
        database_name: Some("pgdog".into()),
        ..Default::default()
    });
    config
}

fn user() -> User {
    User::new("pgdog", "pgdog", "passthrough_stats")
}

fn current_pool() -> Pool {
    databases()
        .cluster(("pgdog", "passthrough_stats"))
        .expect("authenticated user")
        .shards()[0]
        .pools()
        .into_iter()
        .next()
        .expect("pool")
}

fn reload(config: &ConfigAndUsers, preserve: bool) {
    set(config.clone()).expect("configuration");
    replace_databases(from_config(config), preserve).expect("reload");
}

#[tokio::test]
async fn test_passthrough_stats_survive_reload_without_static_users() {
    let config = config();
    reload(&config, false);
    add(user()).expect("passthrough user");
    let original = current_pool();
    {
        let mut inner = original.lock();
        inner.stats.counts.query_count = 40;
        inner.stats.counts.xact_count = 20;
        inner.errors = 4;
        inner.out_of_sync = 3;
        inner.re_synced = 2;
        inner.force_close = 1;
    }
    let mut in_flight = original.get(&Request::default()).await.expect("checkout");
    in_flight.execute_checked("SELECT 42").await.expect("query");

    // Repeated reloads before reauthentication must preserve the same history.
    for _ in 0..2 {
        reload(&config, true);
        assert!(
            databases().all().is_empty(),
            "reload must not retain authorization"
        );
        assert!(!original.state().online);
    }
    add(user()).expect("authenticate again");
    let restored = current_pool();
    assert_ne!(original.id(), restored.id());
    let state = restored.state();
    assert_eq!(
        state.stats.counts.query_count, 40,
        "passthrough query counters must survive reload"
    );
    assert_eq!(state.stats.counts.xact_count, 20);
    assert_eq!(
        (
            state.errors,
            state.out_of_sync,
            state.re_synced,
            state.force_close
        ),
        (4, 3, 2, 1)
    );

    drop(in_flight);
    let after_checkin = restored.state().stats.counts.query_count;
    assert!(
        after_checkin > 40,
        "late query completion must reach the new pool"
    );
    reload_from_existing().expect("reload active user");
    assert_eq!(
        current_pool().state().stats.counts.query_count,
        after_checkin,
        "history must not be counted twice"
    );
}

#[tokio::test]
async fn test_passthrough_retained_stats_respect_reset_and_reconnect() {
    for reset in [true, false] {
        let config = config();
        reload(&config, false);
        add(user()).expect("passthrough user");
        current_pool().lock().stats.counts.query_count = 40;
        reload(&config, true);
        if reset {
            reset_stats();
        } else {
            // RECONNECT uses replace_databases without preserving the old pools.
            reload(&config, false);
        }
        add(user()).expect("authenticate again");
        assert_eq!(current_pool().state().stats.counts.query_count, 0);
    }
}

#[tokio::test]
async fn test_disabling_passthrough_discards_retained_stats() {
    let mut config = config();
    reload(&config, false);
    add(user()).expect("passthrough user");
    current_pool().lock().stats.counts.query_count = 40;
    reload(&config, true);
    config.config.general.passthrough_auth = PassthroughAuth::Disabled;
    reload(&config, true);
    config.config.general.passthrough_auth = PassthroughAuth::EnabledPlain;
    reload(&config, true);
    add(user()).expect("authenticate again");
    assert_eq!(current_pool().state().stats.counts.query_count, 0);
}
