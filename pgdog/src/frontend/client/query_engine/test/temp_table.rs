use super::prelude::*;

#[tokio::test]
async fn test_creating_temp_tables_locks_client() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send_simple(Query::new("CREATE TEMP TABLE foo (id int)"))
        .await;
    client.read_until('Z').await.unwrap();

    assert!(client.backend_locked());

    client
        .send_simple(Query::new("CREATE TEMP TABLE bar (id int)"))
        .await;
    client.read_until('Z').await.unwrap();

    assert!(client.backend_locked());

    client.send_simple(Query::new("DROP TABLE foo")).await;
    client.read_until('Z').await.unwrap();

    assert!(client.backend_locked());

    client.send_simple(Query::new("DROP TABLE bar")).await;
    client.read_until('Z').await.unwrap();

    assert!(!client.backend_locked());
}

#[tokio::test]
async fn test_temp_tables_on_commit() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();
    client
        .send_simple(Query::new("CREATE TEMP TABLE foo (id int) ON COMMIT DROP"))
        .await;
    client.read_until('Z').await.unwrap();
    assert!(client.backend_locked());

    client.send_simple(Query::new("COMMIT")).await;
    client.read_until('Z').await.unwrap();
    assert!(!client.backend_locked());
}

#[tokio::test]
async fn test_temp_tables_drop_on_rollback() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();
    client
        .send_simple(Query::new("CREATE TEMP TABLE foo (id int)"))
        .await;
    client.read_until('Z').await.unwrap();
    assert!(client.backend_locked());

    client.send_simple(Query::new("COMMIT")).await;
    client.read_until('Z').await.unwrap();
    assert!(client.backend_locked());

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();
    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();
    assert!(client.backend_locked());

    client.send_simple(Query::new("DROP TABLE foo")).await;
    client.read_until('Z').await.unwrap();
    assert!(!client.backend_locked());

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();
    client
        .send_simple(Query::new("CREATE TEMP TABLE foo (id int)"))
        .await;
    client.read_until('Z').await.unwrap();
    assert!(client.backend_locked());

    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();
    assert!(!client.backend_locked());
}
