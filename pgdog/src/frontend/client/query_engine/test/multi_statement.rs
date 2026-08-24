use super::prelude::*;

#[tokio::test]
async fn test_multi_statement() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send_simple(Query::new(
            "SELECT * FROM sharded WHERE false; SELECT 1; SELECT 2;",
        ))
        .await;
    let msgs = client.read_until('Z').await.unwrap();
    let codes = msgs.iter().map(|m| m.code()).collect::<Vec<_>>();

    assert_eq!(codes, ['T', 'C', 'T', 'D', 'C', 'T', 'D', 'C', 'Z']);
}

#[tokio::test]
async fn test_multi_error_postgres() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send_simple(Query::new(
            "SELECT * FROM sharded WHERE false; SELECT * FROM doesnt_exist; DROP TABLE sharded;",
        ))
        .await;
    let err = client.read_until('Z').await.unwrap_err();
    assert_eq!(err.code, "42P01");

    client
        .send_simple(Query::new("SELECT * FROM sharded WHERE false"))
        .await;
    client
        .read_until('Z')
        .await
        .expect("sharded table got dropped");
}

#[tokio::test]
async fn test_multi_error_transaction() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send_simple(Query::new("DROP TABLE sharded; SELECT sdf;"))
        .await;
    let err = client.read_until('Z').await.unwrap_err();
    assert_eq!(err.code, "42P01");

    client
        .send_simple(Query::new("SELECT * FROM sharded WHERE false"))
        .await;
    client
        .read_until('Z')
        .await
        .expect("sharded table got dropped");
}
