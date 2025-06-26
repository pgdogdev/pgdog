use rust::setup::*;
use sqlx::{Executor, Row, types::BigDecimal};

#[tokio::test]
async fn test_prepared_cache() {
    let mut conns = vec![];
    for _ in 0..5 {
        conns.push(connections_sqlx().await);
    }

    let clients = conns.iter().map(|conns| conns.len()).sum::<usize>();

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

    assert_eq!(
        prepared
            .get::<BigDecimal, &str>("used_by")
            .to_plain_string(),
        clients.to_string(),
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
    admin
        .execute("SET prepared_statements_limit TO 2")
        .await
        .unwrap();

    for _ in 0..10 {
        for id in 0..50 {
            let query = format!(
                "/* test_prepard_cache_eviction: {} */ SELECT $1::bigint",
                id
            );
            for conn in &conns {
                let row = sqlx::query(query.as_str())
                    .bind(id as i64)
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
