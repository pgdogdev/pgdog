use sqlx::{Postgres, pool::Pool, postgres::PgPoolOptions};


pub async fn connection() -> Pool<Postgres> {
    PgPoolOptions::new()
        .max_connections(1)
        .connect("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog?application_name=sqlx")
        .await
        .unwrap()
}

pub async fn connection_direct() -> Pool<Postgres> {
    PgPoolOptions::new()
        .max_connections(1)
        .connect("postgres://pgdog:pgdog@127.0.0.1:5432/pgdog?application_name=sqlx_direct")
        .await
        .unwrap()
}

/// `options` should be a space-separated list of `-c key=value` pairs using
/// the exact percent-encoded form that PostgreSQL DSNs require (space - `%20`,
/// `=` - `%3D`).
pub async fn connection_with_options(options: &str) -> Pool<Postgres> {
    PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!(
            "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog?application_name=sqlx&options={}",
            options,
        ))
        .await
        .unwrap()
}

pub async fn redis_available() -> bool {
    tokio::net::TcpStream::connect("127.0.0.1:6379")
        .await
        .is_ok()
}

#[macro_export]
macro_rules! require_redis {
    () => {
        if !redis_available().await {
            panic!("Redis required at 127.0.0.1:6379 — start it before running cache tests");
        }
    };
}
