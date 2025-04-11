use rust::setup::*;

#[tokio::test]
async fn test_prepared() {
    let conns = connections_tokio().await;
    for conn in conns {
        let stmt = conn.prepare("SELECT $1::bigint").await.unwrap();
        for i in 0..64_i64 {
            let result = conn.query(&stmt, &[&i]).await.unwrap();
            let result: i64 = result[0].get(0);
            assert_eq!(result, i);
        }
    }
}
