use crate::frontend::router::parser::rewrite::statement::plan::RewriteResult;

use super::prelude::*;

use super::{test_client, test_sharded_client};

async fn run_test(messages: Vec<ProtocolMessage>) -> Vec<ClientRequest> {
    let mut client = test_sharded_client();

    client.client_request = ClientRequest::from(messages);

    let mut engine = QueryEngine::from_client(&client).unwrap();
    let mut context = QueryEngineContext::new(&mut client);

    engine.rewrite_extended(&mut context).unwrap();
    engine.parse_and_rewrite(&mut context).await.unwrap();

    assert!(
        matches!(context.rewrite_result, Some(RewriteResult::InsertSplit(_))),
        "expected rewrite insert split"
    );

    match context.rewrite_result.unwrap() {
        RewriteResult::InsertSplit(requests) => requests,
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_insert_split() {
    let requests = run_test(vec![
        ProtocolMessage::Parse(Parse::new_anonymous(
            "INSERT INTO test (id, email) VALUES ($1, $2), ($3, $4)",
        )),
        ProtocolMessage::Bind(Bind::new_params(
            "",
            &[
                Parameter::new("1".as_bytes()),
                Parameter::new("test@test.com".as_bytes()),
                Parameter::new("1234567890102334".as_bytes()),
                Parameter::new("test2@test.com".as_bytes()),
            ],
        )),
        ProtocolMessage::Execute(Execute::new()),
        ProtocolMessage::Sync(Sync),
    ])
    .await;

    assert_eq!(
        requests.len(),
        2,
        "expected rewrite split to contain two requests"
    );

    for (request, (id, email)) in requests.iter().zip(vec![
        ("1".as_bytes(), "test@test.com".as_bytes()),
        ("1234567890102334".as_bytes(), "test2@test.com".as_bytes()),
    ]) {
        assert!(
            matches!(request[0].clone(), ProtocolMessage::Parse(parse) if parse.query() == "INSERT INTO test (id, email) VALUES ($1, $2)" && parse.anonymous()),
            "expected single tuple insert with no name"
        );
        match request[1].clone() {
            ProtocolMessage::Bind(bind) => {
                assert_eq!(bind.params_raw().get(0).unwrap().data, id);
                assert_eq!(bind.params_raw().get(1).unwrap().data, email);
                assert!(bind.anonymous());
            }
            _ => panic!("expected bind"),
        }
    }
}
#[tokio::test]
async fn test_insert_split_prepared() {
    let requests = run_test(vec![
        ProtocolMessage::Parse(Parse::named(
            "__test_1",
            "INSERT INTO test (id, email) VALUES ($1, $2), ($3, $4)",
        )),
        ProtocolMessage::Bind(Bind::new_params(
            "__test_1",
            &[
                Parameter::new("1".as_bytes()),
                Parameter::new("test@test.com".as_bytes()),
                Parameter::new("1234567890102334".as_bytes()),
                Parameter::new("test2@test.com".as_bytes()),
            ],
        )),
    ])
    .await;

    assert_eq!(requests.len(), 2);

    for (request, (id, email)) in requests.iter().zip(vec![
        ("1".as_bytes(), "test@test.com".as_bytes()),
        ("1234567890102334".as_bytes(), "test2@test.com".as_bytes()),
    ]) {
        assert!(
            matches!(request[0].clone(), ProtocolMessage::Parse(parse) if parse.query() == "INSERT INTO test (id, email) VALUES ($1, $2)" && parse.name() == "__pgdog_2"),
            "expected single tuple insert"
        );
        match request[1].clone() {
            ProtocolMessage::Bind(bind) => {
                assert_eq!(bind.params_raw().get(0).unwrap().data, id);
                assert_eq!(bind.params_raw().get(1).unwrap().data, email);
                assert_eq!(bind.statement(), "__pgdog_2");
            }
            _ => panic!("expected bind"),
        }
    }
}

#[tokio::test]
async fn test_insert_split_simple() {
    let requests = run_test(vec![ProtocolMessage::Query(Query::new(
        "INSERT INTO test (id, email) VALUES (1, 'test@test.com'), (2, 'test2@test.com') RETURNING *",
    ))])
    .await;

    assert_eq!(requests.len(), 2);

    match requests[0][0].clone() {
        ProtocolMessage::Query(query) => assert_eq!(
            query.query(),
            "INSERT INTO test (id, email) VALUES (1, 'test@test.com') RETURNING *"
        ),
        _ => panic!("not a query"),
    }

    match requests[1][0].clone() {
        ProtocolMessage::Query(query) => assert_eq!(
            query.query(),
            "INSERT INTO test (id, email) VALUES (2, 'test2@test.com') RETURNING *"
        ),
        _ => panic!("not a query"),
    }
}

#[tokio::test]
async fn test_insert_split_not_sharded() {
    let mut client = test_client();
    client.client_request = ClientRequest::from(vec![
        ProtocolMessage::Parse(Parse::new_anonymous(
            "INSERT INTO test (id, email) VALUES ($1, $2), ($3, $4)",
        )),
        ProtocolMessage::Other(Flush.message().unwrap()),
    ]);
    let mut engine = QueryEngine::from_client(&client).unwrap();
    let mut context = QueryEngineContext::new(&mut client);
    engine.parse_and_rewrite(&mut context).await.unwrap();

    assert!(context.rewrite_result.is_none());
}
