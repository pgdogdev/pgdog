use crate::setup::{admin_sqlx, connections_sqlx};
use sqlx::{Executor, Pool, Postgres};

#[tokio::test]
async fn test_insert_split_with_many_binds() {
    let conns = connections_sqlx().await;

    setup_schema(&conns, "test_insert_split_with_many_binds", "int8").await;
    let sharded = &conns[1];
    let values_str = (1..=20)
        .step_by(2)
        .map(|i| format!("(${}, ${})", i, i + 1))
        .collect::<Vec<_>>()
        .join(",");

    let sql = format!(
        "INSERT INTO test_insert_split_with_many_binds (customer_id, value) VALUES {}",
        values_str
    );
    let mut query = sqlx::query(&sql);
    for i in 1..=10 {
        query = query.bind(i).bind(i);
    }
    let res = query.execute(sharded).await.unwrap();
    assert_eq!(res.rows_affected(), 10);

    let data = sqlx::query_as::<_, (i64, i64)>("SELECT * FROM test_insert_split_with_many_binds")
        .fetch_all(sharded)
        .await
        .unwrap();
    assert_eq!(
        data,
        vec![
            (1, 1),
            (2, 2),
            (3, 3),
            (4, 4),
            (5, 5),
            (6, 6),
            (7, 7),
            (8, 8),
            (9, 9),
            (10, 10),
        ]
    );
}

async fn setup_schema(conns: &[Pool<Postgres>], table_name: &str, data_type: &str) {
    for conn in conns {
        conn.execute(&*format!("DROP TABLE IF EXISTS {table_name}"))
            .await
            .unwrap();
        conn.execute(&*format!(
            "CREATE TABLE {table_name} (customer_id BIGINT NOT NULL, value {data_type})",
        ))
        .await
        .unwrap();
        admin_sqlx().await.execute("RELOAD").await.unwrap();
    }
}
