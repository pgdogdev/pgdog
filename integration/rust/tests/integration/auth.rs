use rust::setup::admin_sqlx;
use serial_test::serial;
use sqlx::{Connection, Executor, PgConnection, Row};

#[tokio::test]
#[serial]
async fn test_auth() {
    let admin = admin_sqlx().await;
    let bad_password = "postgres://pgdog:skjfhjk23h4234@127.0.0.1:6432/pgdog";

    admin.execute("SET auth_type TO 'trust'").await.unwrap();
    assert_setting_str("auth_type", "trust").await;

    let mut any_password = PgConnection::connect(bad_password).await.unwrap();
    any_password.execute("SELECT 1").await.unwrap();

    let mut empty_password = PgConnection::connect("postgres://pgdog@127.0.0.1:6432/pgdog")
        .await
        .unwrap();
    empty_password.execute("SELECT 1").await.unwrap();

    admin.execute("SET auth_type TO 'scram'").await.unwrap();
    assert_setting_str("auth_type", "scram").await;

    assert!(PgConnection::connect(bad_password).await.is_err());
}

async fn assert_setting_str(name: &str, expected: &str) {
    let admin = admin_sqlx().await;
    let rows = admin.fetch_all("SHOW CONFIG").await.unwrap();
    let mut found = false;
    for row in rows {
        let db_name: String = row.get(0);
        let value: String = row.get(1);

        if name == db_name {
            found = true;
            assert_eq!(value, expected);
        }
    }

    assert!(found);
}

#[tokio::test]
async fn test_passthrough_auth() {
    let admin = admin_sqlx().await;
    // Make sure settings are coming from config file.
    admin.execute("RELOAD").await.unwrap();

    assert_setting_str("passthrough_auth", "disabled").await;

    let no_user = PgConnection::connect("postgres://pgdog1:pgdog@127.0.0.1:6432/pgdog")
        .await
        .err()
        .unwrap();

    assert!(
        no_user
            .to_string()
            .contains("password for user \"pgdog1\" and database \"pgdog\" is wrong")
    );

    admin
        .execute("SET passthrough_auth TO 'enabled_plain'")
        .await
        .unwrap();

    assert_setting_str("passthrough_auth", "enabled_plain").await;

    // First connection after auth changed to passthrough.
    let mut original = PgConnection::connect("postgres://pgdog1:pgdog@127.0.0.1:6432/pgdog")
        .await
        .unwrap();
    original.execute("SELECT 1").await.unwrap();

    let mut tasks = vec![];

    for _ in 0..10 {
        tasks.push(tokio::spawn(async move {
            let mut user = PgConnection::connect("postgres://pgdog1:pgdog@127.0.0.1:6432/pgdog")
                .await
                .unwrap();

            user.execute("SELECT 1").await.unwrap();
            user.close().await.unwrap();
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    // Test reload survival.
    let mut user = PgConnection::connect("postgres://pgdog1:pgdog@127.0.0.1:6432/pgdog")
        .await
        .unwrap();
    user.execute("SELECT 1").await.unwrap();

    // Survive the reload.
    admin.execute("RELOAD").await.unwrap();
    admin
        .execute("SET passthrough_auth TO 'enabled_plain'")
        .await
        .unwrap();

    user.execute("SELECT 1").await.unwrap();
    original.execute("SELECT 1").await.unwrap();
}
