use rust::setup::connections;

#[tokio::test]
async fn select_one() {
    for conn in connections().await {
        for _ in 0..25 {
            let rows = conn.query("SELECT $1::bigint", &[&1_i64]).await.unwrap();

            assert_eq!(rows.len(), 1);
            let one: i64 = rows[0].get(0);
            assert_eq!(one, 1);
        }
    }
}
