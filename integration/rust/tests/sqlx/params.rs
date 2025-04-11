use sqlx::{PgConnection, prelude::*};

#[tokio::test]
async fn test_params() {
    let mut conn = PgConnection::connect(
        "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog?options=-c%20intervalstyle%3Diso_8601",
    )
    .await
    .unwrap();
    let row = conn.fetch_one("SHOW intervalstyle").await.unwrap();
    assert_eq!(row.get::<String, usize>(0), "iso_8601");
}
