use super::prelude::*;

#[tokio::test]
async fn test_unknown_unlock_preserves_other_session_locks() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    for key in [2026092121i64, 2026092122] {
        client
            .send_simple(Query::new(format!(
                "/* pgdog_shard: 0 */ SELECT pg_advisory_lock({key})"
            )))
            .await;
        client.read_until('Z').await.expect("lock acquired");
    }
    for query in [
        "SELECT pg_advisory_unlock(NULL::bigint)",
        "SELECT pg_advisory_unlock((SELECT 2026092123::bigint))",
        "SELECT pg_advisory_unlock(value) FROM (VALUES (NULL::bigint)) AS t(value)",
    ] {
        client
            .send_simple(Query::new(format!("/* pgdog_shard: 0 */ {query}")))
            .await;
        client
            .read_until('Z')
            .await
            .expect("individual unlock completed");
        assert!(client.backend_locked(), "{query}");
        assert!(client.engine.advisory_locks().contains(2026092121));
        assert!(client.engine.advisory_locks().contains(2026092122));
    }
    client
        .send_simple(Query::new(
            "/* pgdog_shard: 0 */ SELECT pg_advisory_unlock(2026092121)",
        ))
        .await;
    client
        .read_until('Z')
        .await
        .expect("known unlock completed");
    assert!(client.backend_locked());
    assert!(!client.engine.advisory_locks().contains(2026092121));
    assert!(client.engine.advisory_locks().contains(2026092122));

    client
        .send_simple(Query::new(
            "/* pgdog_shard: 0 */ SELECT pg_advisory_unlock_all()",
        ))
        .await;
    client.read_until('Z').await.expect("unlock all completed");
    assert!(!client.backend_locked());
    assert_eq!(client.engine.advisory_locks().len(), 0);
}
