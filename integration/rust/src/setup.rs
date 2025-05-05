use sqlx::{Executor, Postgres, Row, pool::Pool, postgres::PgPoolOptions};
use tokio_postgres::*;

pub async fn connections_tokio() -> Vec<Client> {
    let mut results = vec![];

    for db in ["pgdog", "pgdog_sharded"] {
        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 user=pgdog dbname={} password=pgdog port=6432 options=-c%20search_path%3D$user,public%20-capplication_name%3Dtokio",
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
            .connect(&format!(
                "postgres://pgdog:pgdog@127.0.0.1:6432/{}?application_name=sqlx",
                db
            ))
            .await
            .unwrap();
        pools.push(pool);
    }

    pools
}

#[derive(Debug, PartialEq, Clone)]
pub struct Backend {
    pub pid: i32,
    pub backend_start: String,
}

pub async fn backends(application_name: &str, pool: &Pool<Postgres>) -> Vec<Backend> {
    pool.fetch_all(
        format!(
            "SELECT pid::INTEGER,
            backend_start::TEXT
        FROM pg_stat_activity
        WHERE application_name = '{}'
        ORDER BY backend_start",
            application_name
        )
        .as_str(),
    )
    .await
    .unwrap()
    .into_iter()
    .map(|r| Backend {
        pid: r.get(0),
        backend_start: r.get(1),
    })
    .collect()
}

pub async fn connection_failover() -> Pool<Postgres> {
    PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://pgdog:pgdog@127.0.0.1:6432/failover?application_name=sqlx")
        .await
        .unwrap()
}

pub async fn admin_tokio() -> Client {
    let (client, connection) = tokio_postgres::connect(
        "host=127.0.0.1 user=admin dbname=admin password=pgdog port=6432",
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
    PgPoolOptions::new()
        .max_connections(1)
        .connect("postgres://admin:pgdog@127.0.0.1:6432/admin")
        .await
        .unwrap()
}
