use super::*;

#[tokio::test]
async fn test_wal_basic() {
    let client = TwoPcTestClient::new(1_000_000, None).await;

    for _ in 0..20 {
        client.execute().await;
    }

    let segment = client.get_segment(1).await;

    assert!(segment.size() > 0);
    assert_eq!(segment.records().len(), 20 * 3); // 2 states, 1 remove
    for transaction in segment.records().chunks_exact(3) {
        assert_eq!(transaction[0].code, 'i');
        assert!(transaction[0].data.len() > size_of::<u64>());
        assert_eq!(transaction[1].code, '2');
        assert_eq!(transaction[1].data.len(), size_of::<u64>());
        assert_eq!(transaction[2].code, 'r');
        assert_eq!(transaction[2].data.len(), size_of::<u64>());
    }

    client.shutdown().await;
}
