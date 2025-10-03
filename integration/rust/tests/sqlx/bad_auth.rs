use rust::setup::admin_sqlx;
use serial_test::serial;
use sqlx::{Connection, Executor, PgConnection};

#[tokio::test]
#[serial]
async fn test_bad_auth() {
    admin_sqlx().await.execute("RELOAD").await.unwrap();

    for user in ["pgdog", "pgdog_bad_user"] {
        for password in ["bad_password", "another_password", ""] {
            for db in ["random_db", "pgdog"] {
                let err = PgConnection::connect(&format!(
                    "postgres://{}:{}@127.0.0.1:6432/{}",
                    user, password, db
                ))
                .await
                .err()
                .unwrap();
                println!("{}", err);
                let err = err.to_string();
                assert!(
                    err.contains("is down")
                        || err.contains("is wrong, or the database does not exist")
                );
            }
        }
    }

    admin_sqlx().await.execute("RELOAD").await.unwrap();
}
