use sqlx::{PgConnection, prelude::*};

#[tokio::test]
async fn test_params() {
    let mut conn1 = PgConnection::connect(
        "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog?options=-c%20intervalstyle%3Diso_8601%20-c%20jit%3Don%20-c%20statement_timeout%3D3s",
    )
    .await
    .unwrap();

    let mut conn2 = PgConnection::connect(
        "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog?options=-c%20intervalstyle%3Dsql_standard%20-c%20jit%3Doff%20-c%20statement_timeout%3D3001ms",
    )
    .await
    .unwrap();

    let handle1 = tokio::spawn(async move {
        for _ in 0..2500 {
            let row = conn1.fetch_one("SHOW intervalstyle").await.unwrap();
            assert_eq!(row.get::<String, usize>(0), "iso_8601");

            let row = conn1.fetch_one("SHOW jit").await.unwrap();
            assert_eq!(row.get::<String, usize>(0), "on");

            let row = conn1.fetch_one("SHOW statement_timeout").await.unwrap();
            assert_eq!(row.get::<String, usize>(0), "3s");
        }
    });

    let handle2 = tokio::spawn(async move {
        for _ in 0..2500 {
            let row = conn2.fetch_one("SHOW intervalstyle").await.unwrap();
            assert_eq!(row.get::<String, usize>(0), "sql_standard");

            let row = conn2.fetch_one("SHOW jit").await.unwrap();
            assert_eq!(row.get::<String, usize>(0), "off");

            let row = conn2.fetch_one("SHOW statement_timeout").await.unwrap();
            assert_eq!(row.get::<String, usize>(0), "3001ms");
        }
    });

    handle1.await.unwrap();
    handle2.await.unwrap();
}

#[tokio::test]
async fn test_set_param() {
    let mut conn1 = PgConnection::connect(
        "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog?options=-c%20intervalstyle%3Diso_8601",
    )
    .await
    .unwrap();

    let mut conn2 = PgConnection::connect(
        "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog?options=-c%20intervalstyle%3Dsql_standard",
    )
    .await
    .unwrap();

    // This should record it in the client params struct as well.
    conn1
        .execute("SET intervalstyle TO 'postgres'")
        .await
        .unwrap();

    for _ in 0..25 {
        // Conn 2 takes the connection conn1 just used.
        conn2.execute("BEGIN").await.unwrap();
        let row = conn2.fetch_one("SHOW intervalstyle").await.unwrap();
        assert_eq!(row.get::<String, usize>(0), "sql_standard");

        // Conn 1 is forced to get a new one, which should now be synchronized
        // with the right param value.
        conn1.fetch_one("SHOW intervalstyle").await.unwrap();
        let row = conn1.fetch_one("SHOW intervalstyle").await.unwrap();
        assert_eq!(row.get::<String, usize>(0), "postgres");

        conn2.execute("COMMIT").await.unwrap();
    }
}
