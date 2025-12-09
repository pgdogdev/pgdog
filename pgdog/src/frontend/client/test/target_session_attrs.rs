use super::*;

async fn run_target_session_test(property: &str, query: &str) -> Message {
    let mut params = Parameters::default();
    params.insert("target_session_attrs", property);

    let (mut stream, mut client) = test_client_with_params(params, true).await;
    let mut engine = QueryEngine::from_client(&client).unwrap();

    let expected = if property == "primary" {
        Role::Primary
    } else if property == "standby" {
        Role::Replica
    } else {
        panic!("unexpected property: {}", property);
    };
    assert_eq!(client.sticky.role, Some(expected));

    stream
        .write_all(&Query::new(query).to_bytes().unwrap())
        .await
        .unwrap();
    stream.flush().await.unwrap();

    client.buffer(State::Idle).await.unwrap();
    client.client_messages(&mut engine).await.unwrap();

    let reply = engine.backend().read().await.unwrap();

    reply
}

#[tokio::test]
async fn test_target_session_attrs_standby() {
    let reply = run_target_session_test(
        "standby",
        "CREATE TABLE test_target_session_attrs_standby(id BIGINT)",
    )
    .await;
    assert_eq!(reply.code(), 'E');
    let error = ErrorResponse::from_bytes(reply.to_bytes().unwrap()).unwrap();
    assert_eq!(
        error.message,
        "cannot execute CREATE TABLE in a read-only transaction"
    );
}

#[tokio::test]
async fn test_target_session_attrs_primary() {
    for _ in 0..5 {
        let reply = run_target_session_test(
            "primary",
            "CREATE TABLE IF NOT EXISTS test_target_session_attrs_primary(id BIGINT)",
        )
        .await;
        assert_ne!(reply.code(), 'E');
    }
}
