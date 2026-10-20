use std::sync::Arc;

use pgdog_config::{
    Config, ConfigAndUsers, Database, General, LoadSchema, PassthroughAuth, Role, User, Users,
};

use crate::auth::AuthResult;
use crate::backend::databases;
use crate::config::{config, set};

use super::{check_or_add, restore_after_reload};

fn setup(passthrough_auth: PassthroughAuth, users: Vec<User>) {
    // Use the same PostgreSQL fixture as the backend server and pool tests:
    // pgdog:pgdog at 127.0.0.1:5432/pgdog, with password authentication enabled.
    // PGDOG_TEST_PG_PORT can select an isolated PostgreSQL instance when the
    // developer's default instance uses trust authentication.
    let port = std::env::var("PGDOG_TEST_PG_PORT")
        .map(|port| port.parse::<u16>().expect("valid PostgreSQL test port"))
        .unwrap_or(5432);
    let config = ConfigAndUsers {
        config: Config {
            general: General {
                passthrough_auth,
                checkout_timeout: 1_000,
                connect_timeout: 1_000,
                min_pool_size: 0,
                load_schema: LoadSchema::Off,
                ..Default::default()
            },
            databases: ["pgdog", "pgdog_alias"]
                .into_iter()
                .map(|name| Database {
                    name: name.to_owned(),
                    database_name: Some("pgdog".to_owned()),
                    host: "127.0.0.1".to_owned(),
                    port,
                    role: Role::Primary,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
        users: Users {
            users,
            ..Default::default()
        },
        ..Default::default()
    };
    let config = set(config).expect("set test config");
    databases::replace_databases(databases::from_config(&config), false)
        .expect("replace test databases");
}

fn user(password: Option<&str>) -> User {
    User {
        name: "pgdog".to_owned(),
        database: "pgdog".to_owned(),
        password: password.map(str::to_owned),
        ..Default::default()
    }
}

#[tokio::test]
async fn cached_password_skips_database_validation_and_reload() {
    for mode in [
        PassthroughAuth::Enabled,
        PassthroughAuth::EnabledPlain,
        PassthroughAuth::EnabledAllowChange,
        PassthroughAuth::EnabledPlainAllowChange,
    ] {
        // This cached password differs from PostgreSQL's current password.
        // Success therefore requires skipping the database validation.
        setup(mode.clone(), vec![user(Some("cached_password"))]);
        let before = config();
        let pools = databases::databases();

        let result = check_or_add(user(Some("cached_password")))
            .await
            .expect("authenticate");

        assert_eq!(result, AuthResult::Ok, "mode: {mode:?}");
        assert!(Arc::ptr_eq(&before, &config()));
        assert!(Arc::ptr_eq(&pools, &databases::databases()));
    }
}

#[tokio::test]
async fn disallowed_password_change_is_rejected_without_database_validation() {
    for mode in [PassthroughAuth::Enabled, PassthroughAuth::EnabledPlain] {
        setup(mode, vec![user(Some("pgdog"))]);
        let before = config();

        let result = check_or_add(user(Some("wrong")))
            .await
            .expect("authenticate");

        assert_eq!(result, AuthResult::NoPassthroughPasswordChange);
        assert!(Arc::ptr_eq(&before, &config()));
    }
}

#[tokio::test]
async fn new_user_requires_database_validation() {
    setup(PassthroughAuth::EnabledPlain, vec![]);
    let before = config();

    let result = check_or_add(user(Some("wrong")))
        .await
        .expect("authenticate");

    assert_eq!(result, AuthResult::NoPassthroughDatabaseCheck);
    assert!(Arc::ptr_eq(&before, &config()));
    assert!(config().users.find(&user(None)).is_none());
}

#[tokio::test]
async fn initial_password_requires_database_validation() {
    setup(PassthroughAuth::EnabledPlain, vec![user(None)]);
    let before = config();

    let result = check_or_add(user(Some("wrong")))
        .await
        .expect("authenticate");

    assert_eq!(result, AuthResult::NoPassthroughDatabaseCheck);
    assert!(Arc::ptr_eq(&before, &config()));
    assert_eq!(config().users.find(&user(None)), Some(user(None)));
}

#[tokio::test]
async fn allowed_password_change_requires_database_validation() {
    for mode in [
        PassthroughAuth::EnabledAllowChange,
        PassthroughAuth::EnabledPlainAllowChange,
    ] {
        setup(mode.clone(), vec![user(Some("pgdog"))]);
        let before = config();

        let result = check_or_add(user(Some("wrong")))
            .await
            .expect("authenticate");

        assert_eq!(
            result,
            AuthResult::NoPassthroughDatabaseCheck,
            "mode: {mode:?}"
        );
        assert!(Arc::ptr_eq(&before, &config()));
        assert_eq!(config().users.find(&user(None)), Some(user(Some("pgdog"))));
    }
}

#[tokio::test]
async fn cached_password_is_scoped_to_user_and_database() {
    setup(
        PassthroughAuth::EnabledPlain,
        vec![user(Some("cached_password"))],
    );
    let before = config();

    for candidate in [
        User {
            name: "pgdog_passthrough_missing_user".to_owned(),
            ..user(Some("cached_password"))
        },
        User {
            database: "pgdog_alias".to_owned(),
            ..user(Some("cached_password"))
        },
    ] {
        let result = check_or_add(candidate).await.expect("authenticate");

        assert_eq!(result, AuthResult::NoPassthroughDatabaseCheck);
        assert!(Arc::ptr_eq(&before, &config()));
    }
}

#[tokio::test]
async fn new_user_with_valid_postgres_password_is_cached() {
    setup(PassthroughAuth::EnabledPlain, vec![]);
    let provided = user(Some("pgdog"));

    let result = check_or_add(provided.clone()).await.expect("authenticate");

    assert_eq!(result, AuthResult::Ok);
    assert_eq!(config().users.find(&provided), Some(provided));
    assert!(databases::databases().cluster(("pgdog", "pgdog")).is_ok());
}

#[tokio::test]
async fn initial_valid_password_preserves_existing_user_settings() {
    let existing = User {
        statement_timeout: Some(100),
        pool_size: Some(7),
        ..user(None)
    };
    setup(PassthroughAuth::EnabledPlain, vec![existing.clone()]);

    let result = check_or_add(user(Some("pgdog")))
        .await
        .expect("authenticate");

    assert_eq!(result, AuthResult::Ok);
    let expected = User {
        password: Some("pgdog".to_owned()),
        ..existing
    };
    assert_eq!(config().users.find(&expected), Some(expected));
}

#[tokio::test]
async fn allowed_valid_password_change_preserves_existing_user_settings() {
    for mode in [
        PassthroughAuth::EnabledAllowChange,
        PassthroughAuth::EnabledPlainAllowChange,
    ] {
        let existing = User {
            statement_timeout: Some(100),
            pool_size: Some(7),
            ..user(Some("old_password"))
        };
        setup(mode, vec![existing.clone()]);

        let result = check_or_add(user(Some("pgdog")))
            .await
            .expect("authenticate");

        assert_eq!(result, AuthResult::Ok);
        let expected = User {
            password: Some("pgdog".to_owned()),
            ..existing
        };
        assert_eq!(config().users.find(&expected), Some(expected));
    }
}

#[tokio::test]
async fn restore_missing_user_skips_database_validation() {
    setup(PassthroughAuth::EnabledPlain, vec![]);
    let restored = user(Some("secret"));

    let result = restore_after_reload(restored.clone()).expect("restore user");

    assert_eq!(result, AuthResult::Ok);
    assert_eq!(config().users.find(&restored), Some(restored));
}

#[tokio::test]
async fn restore_matching_password_leaves_config_and_pools_unchanged() {
    setup(PassthroughAuth::EnabledPlain, vec![user(Some("secret"))]);
    let before = config();
    let pools = databases::databases();

    let result = restore_after_reload(user(Some("secret"))).expect("restore user");

    assert_eq!(result, AuthResult::Ok);
    assert!(Arc::ptr_eq(&before, &config()));
    assert!(Arc::ptr_eq(&pools, &databases::databases()));
}

#[tokio::test]
async fn restore_password_preserves_existing_user_settings() {
    for (mode, old_password) in [
        (PassthroughAuth::EnabledPlain, None),
        (PassthroughAuth::EnabledPlainAllowChange, Some("old")),
    ] {
        let existing = User {
            statement_timeout: Some(100),
            pool_size: Some(7),
            server_password: Some("server_secret".to_owned()),
            ..user(old_password)
        };
        setup(mode, vec![existing.clone()]);

        let result = restore_after_reload(user(Some("new"))).expect("restore user");

        assert_eq!(result, AuthResult::Ok);
        let expected = User {
            password: Some("new".to_owned()),
            ..existing
        };
        assert_eq!(config().users.find(&expected), Some(expected));
    }
}

#[tokio::test]
async fn restore_rejects_disallowed_password_change() {
    setup(PassthroughAuth::EnabledPlain, vec![user(Some("secret"))]);
    let before = config();

    let result = restore_after_reload(user(Some("different"))).expect("restore user");

    assert_eq!(result, AuthResult::NoPassthroughPasswordChange);
    assert!(Arc::ptr_eq(&before, &config()));
}
