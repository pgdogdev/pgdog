use rust::setup::admin_sqlx;
use rust::utils::assert_setting_str;
use serial_test::serial;
use sqlx::{Connection, Executor, PgConnection};

#[tokio::test]
#[serial]
async fn test_auth_types() {
    let admin = admin_sqlx().await;
    let good = "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog";
    let bad = "postgres://pgdog:wrong@127.0.0.1:6432/pgdog";
    let none = "postgres://pgdog@127.0.0.1:6432/pgdog";

    for auth_type in ["md5", "scram", "plain", "trust"] {
        admin
            .execute(format!("SET auth_type TO '{auth_type}'").as_str())
            .await
            .unwrap();
        assert_setting_str("auth_type", auth_type).await;

        let mut conn = PgConnection::connect(good).await.unwrap();
        conn.execute("SELECT 1").await.unwrap();

        if auth_type == "trust" {
            let mut conn = PgConnection::connect(bad).await.unwrap();
            conn.execute("SELECT 1").await.unwrap();

            let mut conn = PgConnection::connect(none).await.unwrap();
            conn.execute("SELECT 1").await.unwrap();
        } else {
            let bad_err = PgConnection::connect(bad).await.err().unwrap();
            assert!(
                bad_err
                    .to_string()
                    .contains("password for user \"pgdog\" and database \"pgdog\" is wrong"),
                "{auth_type}: bad password error: {bad_err}"
            );
            let none_err = PgConnection::connect(none).await.err().unwrap();
            assert!(
                none_err
                    .to_string()
                    .contains("password for user \"pgdog\" and database \"pgdog\" is wrong"),
                "{auth_type}: no password error: {none_err}"
            );
        }
    }

    // Reset config.
    admin.execute("RELOAD").await.unwrap();
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

#[tokio::test]
async fn test_passthrough_password_change() {
    let admin = admin_sqlx().await;
    let mut direct =
        PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:5432/pgdog?sslmode=disable")
            .await
            .unwrap();

    // Ensure clean state.
    admin.execute("RELOAD").await.unwrap();
    admin
        .execute("SET passthrough_auth TO 'enabled_plain_allow_change'")
        .await
        .unwrap();
    assert_setting_str("passthrough_auth", "enabled_plain_allow_change").await;

    // Make sure pgdog1 has the original password.
    direct
        .execute("ALTER USER pgdog1 PASSWORD 'pgdog'")
        .await
        .unwrap();

    // Connect with original password and keep connection alive.
    let mut existing = PgConnection::connect("postgres://pgdog1:pgdog@127.0.0.1:6432/pgdog")
        .await
        .unwrap();
    existing.execute("SELECT 1").await.unwrap();

    // Change password in PostgreSQL directly.
    direct
        .execute("ALTER USER pgdog1 PASSWORD 'new_password'")
        .await
        .unwrap();

    // New connection with new password should work.
    let mut new_conn = PgConnection::connect("postgres://pgdog1:new_password@127.0.0.1:6432/pgdog")
        .await
        .unwrap();
    new_conn.execute("SELECT 1").await.unwrap();

    // Existing connection should still work.
    existing.execute("SELECT 1").await.unwrap();

    // Cleanup: restore original password.
    direct
        .execute("ALTER USER pgdog1 PASSWORD 'pgdog'")
        .await
        .unwrap();

    admin.execute("RELOAD").await.unwrap();
}
