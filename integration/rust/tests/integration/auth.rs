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
#[serial]
async fn test_multiple_passwords() {
    let admin = admin_sqlx().await;
    admin.execute("RELOAD").await.unwrap();

    // The `pgdog_multi_pass` user in `integration/users.toml` is configured
    // with `passwords = ["alpha_pass", "beta_pass"]`. Both should be accepted
    // by every password-based auth type.
    for auth_type in ["md5", "scram", "plain"] {
        admin
            .execute(format!("SET auth_type TO '{auth_type}'").as_str())
            .await
            .unwrap();
        assert_setting_str("auth_type", auth_type).await;

        for password in ["alpha_pass", "beta_pass"] {
            let url = format!("postgres://pgdog_multi_pass:{password}@127.0.0.1:6432/pgdog");
            let mut conn = PgConnection::connect(&url)
                .await
                .unwrap_or_else(|e| panic!("{auth_type}: {password} failed to connect: {e}"));
            conn.execute("SELECT 1").await.unwrap();
            conn.close().await.unwrap();
        }

        // Wrong password should still be rejected.
        let bad = "postgres://pgdog_multi_pass:not_a_password@127.0.0.1:6432/pgdog";
        let err = PgConnection::connect(bad).await.err().unwrap();
        assert!(
            err.to_string()
                .contains("password for user \"pgdog_multi_pass\" and database \"pgdog\" is wrong"),
            "{auth_type}: wrong password should fail: {err}"
        );
    }

    admin.execute("RELOAD").await.unwrap();
}

#[tokio::test]
#[serial]
async fn test_multiple_passwords_server_side() {
    let admin = admin_sqlx().await;
    admin.execute("RELOAD").await.unwrap();

    // The `pgdog_multi_pass_server` user has no `server_password`, so PgDog
    // uses the user's `passwords` to authenticate against Postgres. The list
    // is `["wrong_password", "pgdog"]` — only the second one actually works
    // upstream. PgDog must try both and fall through to the working one.
    //
    // The same list is also offered to the client, so connecting with either
    // password should succeed.
    for auth_type in ["md5", "scram", "plain"] {
        admin
            .execute(format!("SET auth_type TO '{auth_type}'").as_str())
            .await
            .unwrap();
        assert_setting_str("auth_type", auth_type).await;

        for password in ["wrong_password", "pgdog"] {
            let url = format!("postgres://pgdog_multi_pass_server:{password}@127.0.0.1:6432/pgdog");
            let mut conn = PgConnection::connect(&url)
                .await
                .unwrap_or_else(|e| panic!("{auth_type}: {password} failed to connect: {e}"));
            conn.execute("SELECT 1").await.unwrap();
            conn.close().await.unwrap();
        }

        let bad = "postgres://pgdog_multi_pass_server:not_a_password@127.0.0.1:6432/pgdog";
        let err = PgConnection::connect(bad).await.err().unwrap();
        assert!(
            err.to_string().contains(
                "password for user \"pgdog_multi_pass_server\" and database \"pgdog\" is wrong"
            ),
            "{auth_type}: wrong password should fail: {err}"
        );
    }

    admin.execute("RELOAD").await.unwrap();
}

#[tokio::test]
#[serial]
async fn test_multi_password_server_rotation() {
    let admin = admin_sqlx().await;
    admin.execute("RELOAD").await.unwrap();

    // Direct connection to Postgres so we can rotate `pgdog2`'s password
    // out from under PgDog.
    let mut direct =
        PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:5432/pgdog?sslmode=disable")
            .await
            .unwrap();

    // Make sure pgdog2 starts in the known-good state.
    direct
        .execute("ALTER USER pgdog2 PASSWORD 'pgdog'")
        .await
        .unwrap();

    // The `pgdog_pw_rotate` user is configured with
    //   passwords = ["rotated_pass", "pgdog"]
    //   server_user = "pgdog2"
    // PgDog tries each password in order against the upstream Postgres until
    // one succeeds. Initially "pgdog" is the live password, so PgDog falls
    // through past "rotated_pass" and connects with "pgdog".
    let url = "postgres://pgdog_pw_rotate:rotated_pass@127.0.0.1:6432/pgdog";
    let mut conn = PgConnection::connect(url).await.unwrap();
    conn.execute("SELECT 1").await.unwrap();
    conn.close().await.unwrap();

    // Rotate the upstream password to the OTHER configured password.
    direct
        .execute("ALTER USER pgdog2 PASSWORD 'rotated_pass'")
        .await
        .unwrap();

    // Force PgDog to drop its existing pooled connections so the next client
    // request opens a fresh upstream connection that has to re-authenticate.
    admin.execute("RELOAD").await.unwrap();
    admin.execute("RECONNECT").await.unwrap();

    // PgDog should now succeed by trying "rotated_pass" first and matching.
    let mut conn = PgConnection::connect(url).await.unwrap();
    conn.execute("SELECT 1").await.unwrap();
    conn.close().await.unwrap();

    // Cleanup: restore the original upstream password and pool state.
    direct
        .execute("ALTER USER pgdog2 PASSWORD 'pgdog'")
        .await
        .unwrap();
    admin.execute("RELOAD").await.unwrap();
    admin.execute("RECONNECT").await.unwrap();
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

#[tokio::test]
async fn test_scram_hashed_passthrough() {
    let mut conn =
        sqlx::PgConnection::connect("postgres://pgdog_hashed:pgdog@127.0.0.1:6432/pgdog")
            .await
            .unwrap();
    conn.execute("SELECT 1").await.unwrap();

    let conn =
        sqlx::PgConnection::connect("postgres://pgdog_hashed:badpw@127.0.0.1:6432/pgdog").await;
    assert!(conn.is_err());
    assert!(
        conn.err()
            .unwrap()
            .to_string()
            .contains("password for user")
    );
}
