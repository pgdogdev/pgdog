use rust::setup::connections_sqlx;

#[tokio::test]
async fn test_connect() {
    for conn in connections_sqlx().await {
        for i in 0..1 {
            let row: (i64,) = sqlx::query_as("SELECT $1")
                .bind(i)
                .fetch_one(&conn)
                .await
                .unwrap();

            assert_eq!(row.0, i);
        }
    }
}
