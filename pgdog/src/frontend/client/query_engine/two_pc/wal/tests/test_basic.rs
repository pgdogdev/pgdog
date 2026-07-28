use super::*;

#[tokio::test]
async fn test_wal_basic() {
    let client = TwoPcTestClient::new().await;

    for _ in 0..20 {
        client.execute().await;
    }

    let segment = client.get_segment(1).await;

    assert!(segment.size() > 0);
    assert_eq!(segment.records().len(), 20 * 3); // 2 states, 1 remove

    client.shutdown().await;
}
