use rust::setup::connections_sqlx;
use sqlx::{Executor, Row};

#[tokio::test]
async fn test_distinct() {
    let conns = connections_sqlx().await;

    for conn in conns {
        conn.execute("CREATE TABLE IF NOT EXISTS test_distinct(id BIGINT, email VARCHAR)")
            .await
            .unwrap();

        for i in 0..10 {
            for email in ["test@test.com", "apples@test.com"] {
                let _ = sqlx::query("INSERT INTO test_distinct VALUES ($1, $2)")
                    .bind(i)
                    .bind(email)
                    .fetch_all(&conn)
                    .await
                    .unwrap();
            }
        }

        let rows = conn
            .fetch_all("SELECT DISTINCT * FROM test_distinct")
            .await
            .unwrap();

        assert_eq!(rows.len(), 20);

        let rows = conn
            .fetch_all("SELECT DISTINCT ON (id) * FROM test_distinct")
            .await
            .unwrap();
        let ids = rows
            .iter()
            .map(|r| r.get::<i64, usize>(0))
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

        let rows = conn
            .fetch_all("SELECT DISTINCT ON (email) * FROM test_distinct ORDER BY email")
            .await
            .unwrap();

        let emails = rows
            .iter()
            .map(|r| r.get::<String, usize>(1))
            .collect::<Vec<_>>();
        assert_eq!(
            emails,
            vec!["apples@test.com".to_string(), "test@test.com".to_string()]
        );
    }
}
