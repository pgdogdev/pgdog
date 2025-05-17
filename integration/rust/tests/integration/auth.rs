use rust::setup::admin_sqlx;
use serial_test::serial;
use sqlx::{Connection, Executor, PgConnection, Row};

#[tokio::test]
#[serial]
async fn test_auth() {
    let admin = admin_sqlx().await;
    let bad_password = "postgres://pgdog:skjfhjk23h4234@127.0.0.1:6432/pgdog";

    admin.execute("SET auth_type TO 'trust'").await.unwrap();
    assert_auth("trust").await;

    let mut any_password = PgConnection::connect(bad_password).await.unwrap();
    any_password.execute("SELECT 1").await.unwrap();

    let mut empty_password = PgConnection::connect("postgres://pgdog@127.0.0.1:6432/pgdog")
        .await
        .unwrap();
    empty_password.execute("SELECT 1").await.unwrap();

    admin.execute("SET auth_type TO 'scram'").await.unwrap();
    assert_auth("scram").await;

    assert!(PgConnection::connect(bad_password).await.is_err());
}

async fn assert_auth(expected: &str) {
    let admin = admin_sqlx().await;
    let rows = admin.fetch_all("SHOW CONFIG").await.unwrap();
    let mut found = false;
    for row in rows {
        let name: String = row.get(0);
        let value: String = row.get(1);

        if name == "auth_type" {
            found = true;
            assert_eq!(value, expected);
        }
    }

    assert!(found);
}
