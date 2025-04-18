use sqlx::{Postgres, pool::Pool, postgres::PgPoolOptions};
use tokio_postgres::*;

pub async fn connections_tokio() -> Vec<Client> {
    let mut results = vec![];

    for db in ["pgdog", "pgdog_sharded"] {
        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 user=pgdog dbname={} password=pgdog port=6432 options=-c%20search_path%3D$user,public",
                db
            ),
            NoTls,
        )
        .await
        .unwrap();

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        results.push(client);
    }

    results
}

pub async fn connections_sqlx() -> Vec<Pool<Postgres>> {
    let mut pools = vec![];
    for db in ["pgdog", "pgdog_sharded"] {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&format!("postgres://pgdog:pgdog@127.0.0.1:6432/{}", db))
            .await
            .unwrap();
        pools.push(pool);
    }

    pools
}

pub async fn connection_failover() -> Pool<Postgres> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!("postgres://pgdog:pgdog@127.0.0.1:6432/failover"))
        .await
        .unwrap();

    pool
}

pub async fn admin_tokio() -> Client {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 user=admin dbname=admin password=pgdog port=6432",),
        NoTls,
    )
    .await
    .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client
}

pub async fn admin_sqlx() -> Pool<Postgres> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("postgres://admin:pgdog@127.0.0.1:6432/admin"))
        .await
        .unwrap();

    pool
}
