use rust::setup::*;
use sqlx::{Executor, Row, postgres::PgPoolOptions, types::BigDecimal};
use tokio::spawn;

#[tokio::test]
async fn test_prepared_cache() {
    let mut conns = vec![];
    for _ in 0..5 {
        conns.push(connections_sqlx().await);
    }

    for conns in &conns {
        for conn in conns.iter() {
            sqlx::query("/* test_prepared_cache_rust */ SELECT $1")
                .bind(5)
                .fetch_one(conn)
                .await
                .unwrap();

            let err = sqlx::query("/* test_prepared_cache_rust_error */ SELECT sfsdfsdf")
                .fetch_one(conn)
                .await;

            assert!(err.is_err());
        }
    }

    let admin = admin_sqlx().await;
    let prepared = admin
        .fetch_all("SHOW PREPARED")
        .await
        .unwrap()
        .into_iter()
        .filter(|row| {
            row.get::<String, &str>("statement")
                .contains("/* test_prepared_cache_rust */")
        })
        .next()
        .unwrap();

    let clients = conns
        .iter()
        .map(|conns| conns.iter().map(|p| p.size()).sum::<u32>() as usize)
        .sum::<usize>();

    assert_eq!(
        prepared
            .get::<BigDecimal, &str>("used_by")
            .to_plain_string()
            .parse::<i64>()
            .unwrap(),
        clients as i64
    );

    for pools in &conns {
        for conn in pools {
            conn.close().await;
        }
    }

    let prepared = admin
        .fetch_all("SHOW PREPARED")
        .await
        .unwrap()
        .into_iter()
        .filter(|row| {
            row.get::<String, &str>("statement")
                .contains("/* test_prepared_cache_rust")
        })
        .collect::<Vec<_>>();

    assert_eq!(prepared.len(), 2);

    prepared.iter().for_each(|row| {
        assert_eq!(
            row.get::<BigDecimal, &str>("used_by").to_plain_string(),
            "0",
        )
    });
}

#[tokio::test]
async fn test_prepard_cache_eviction() {
    let admin = admin_sqlx().await;
    let conns = connections_sqlx().await;
    // Clear all server stats.
    admin.execute("RECONNECT").await.unwrap();

    // Remove all prepared statements.
    admin
        .execute("SET prepared_statements_limit TO 0")
        .await
        .unwrap();

    admin
        .execute("SET prepared_statements_limit TO 2")
        .await
        .unwrap();

    for _ in 0..10 {
        for id in 0..50 {
            let query = format!(
                "/* test_prepard_cache_eviction: {id} */ SELECT $1::bigint"
            );
            for conn in &conns {
                let row = sqlx::query(query.as_str())
                    .bind(id)
                    .fetch_one(conn)
                    .await
                    .unwrap();
                assert_eq!(row.get::<i64, usize>(0), id);
            }
        }
    }

    let prepared = admin.fetch_all("SHOW PREPARED").await.unwrap();
    assert_eq!(prepared.len(), 50);

    for conn in conns {
        conn.close().await;
    }

    // Evicted only when clients disconnect or manually close the statement.
    let prepared = admin.fetch_all("SHOW PREPARED").await.unwrap();
    assert_eq!(prepared.len(), 2);

    let servers = admin.fetch_all("SHOW SERVERS").await.unwrap();

    for server in servers {
        let stmts = server.get::<BigDecimal, &str>("prepared_statements");
        let val: i64 = stmts.to_plain_string().parse().unwrap(); // that's fine.
        assert!(val <= 2);
    }

    // Reset config
    admin.execute("RELOAD").await.unwrap();
}

#[tokio::test]
async fn test_memory_realloc() {
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog?application_name=test_memory_alloc&statement_cache_capacity=500")
        .await
        .unwrap();

    let admin = admin_sqlx().await;
    // Clear all server stats.
    admin.execute("RECONNECT").await.unwrap();

    // Remove all prepared statements.
    admin
        .execute("SET prepared_statements_limit TO 0")
        .await
        .unwrap();

    admin
        .execute("SET prepared_statements_limit TO 50")
        .await
        .unwrap();

    let mut tasks = vec![];

    for _ in 0..25 {
        let pool = pool.clone();
        tasks.push(spawn(async move {
                for i in 0..1000 {
                    let query = format!("SELECT $1::bigint, $2::integer, $3::text, 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', 'Splits the bytes into two at the given index.Afterwards self contains elements [at, len), and the returned Bytes contains elements [0, at).This is an O(1) operation that just increases the reference count and sets a few indices.', '{i}'");
                    let _result = sqlx::query(query.as_str()).bind(i as i64).bind(i).bind("test").execute(&pool).await.unwrap();
                }
            }));
    }

    for task in tasks {
        task.await.unwrap();
    }
}
