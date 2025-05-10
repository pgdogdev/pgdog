use serial_test::serial;
use sqlx::{Connection, PgConnection};

#[tokio::test]
#[serial]
async fn test_bad_auth() {
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
                assert!(err.to_string().contains(&format!(
                    "user \"{}\" and database \"{}\" is wrong, or the database does not exist",
                    user, db
                )));
            }
        }
    }
}
