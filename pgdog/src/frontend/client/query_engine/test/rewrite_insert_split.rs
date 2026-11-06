use std::ops::Deref;

use crate::frontend::router::parser::Shard;
use crate::frontend::router::parser::rewrite::statement::plan::RewriteResult;
use crate::net::messages::bind::Format;
use pgdog_postgres_types::{Array, Datum};

use super::prelude::*;

use super::{test_client, test_sharded_client};

async fn run_test(messages: Vec<ProtocolMessage>) -> Vec<ClientRequest> {
    let mut client = test_sharded_client();

    client.client_request = ClientRequest::from(messages);

    let mut engine = QueryEngine::from_client(&client).unwrap();
    let (mut context, client_request) = QueryEngineContext::new(&mut client);

    engine
        .rewrite_extended(&mut context, &mut client_request.messages)
        .unwrap();
    let rewrite_result = engine
        .parse_and_rewrite(&mut context, client_request)
        .await
        .unwrap();

    assert!(
        matches!(rewrite_result, Some(RewriteResult::InsertSplit(_))),
        "expected rewrite insert split"
    );

    match rewrite_result.unwrap() {
        RewriteResult::InsertSplit(requests) => requests.deref().clone(),
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
        std::assert_matches!(
            &request.messages[0], ProtocolMessage::Parse(parse) if parse.query() == "INSERT INTO test (id, email) VALUES ($1, $2)" && parse.anonymous(),
            "expected single tuple insert with no name"
        );
        match &request.messages[1] {
            ProtocolMessage::Bind(bind) => {
                assert_eq!(bind.params_raw().first().unwrap().data, id);
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
        std::assert_matches!(
            &request.messages[0], ProtocolMessage::Parse(parse) if parse.query() == "INSERT INTO test (id, email) VALUES ($1, $2)" && parse.name() == "__pgdog_2",
            "expected single tuple insert"
        );
        match &request.messages[1] {
            ProtocolMessage::Bind(bind) => {
                assert_eq!(bind.params_raw().first().unwrap().data, id);
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

    std::assert_matches!(
        &requests[0].messages[0],
        ProtocolMessage::Query(query) if query.query() == "INSERT INTO test (id, email) VALUES (1, 'test@test.com') RETURNING *",
    );

    std::assert_matches!(
        &requests[1].messages[0],
        ProtocolMessage::Query(query) if query.query() == "INSERT INTO test (id, email) VALUES (2, 'test2@test.com') RETURNING *",
    );
}

#[tokio::test]
async fn test_insert_split_not_sharded() {
    let mut client = test_client();
    client.client_request = ClientRequest::from(vec![
        ProtocolMessage::Parse(Parse::new_anonymous(
            "INSERT INTO test (id, email) VALUES ($1, $2), ($3, $4)",
        )),
        ProtocolMessage::Other(Flush.message()),
    ]);
    let engine = QueryEngine::from_client(&client).unwrap();
    let (mut context, client_request) = QueryEngineContext::new(&mut client);
    let rewrite_result = engine
        .parse_and_rewrite(&mut context, client_request)
        .await
        .unwrap();

    assert!(rewrite_result.is_none());
}

const UNNEST_INSERT: &str = "INSERT INTO sharded (id, value) SELECT * FROM unnest($1::pg_catalog._int8, $2::pg_catalog._text)";

fn unnest_insert_messages(
    sql: &str,
    ids: &[i64],
    values: &[Option<&str>],
    format: Format,
) -> Vec<ProtocolMessage> {
    let array = |elements: Vec<Datum>, element_oid| {
        Parameter::new(&Array::new(elements, element_oid).encode(format).unwrap())
    };
    let ids = array(ids.iter().map(|id| Datum::Bigint(*id)).collect(), 20);
    let values = array(
        values
            .iter()
            .map(|value| value.map_or(Datum::Null, |value| Datum::Text(value.into())))
            .collect(),
        25,
    );

    vec![
        ProtocolMessage::Parse(Parse::new_anonymous(sql)),
        ProtocolMessage::Bind(Bind::new_params_codes("", &[ids, values], &[format])),
        ProtocolMessage::Execute(Execute::new()),
        ProtocolMessage::Sync(Sync),
    ]
}

// Returns the (id, value) rows that the requests insert (one list per request)
fn inserted_rows_by_request(requests: &[ClientRequest]) -> Vec<Vec<(i64, Option<String>)>> {
    requests
        .iter()
        .map(|request| {
            std::assert_matches!(
                &request.messages[0],
                ProtocolMessage::Parse(parse) if parse.query() == UNNEST_INSERT,
                "expected the original unnest INSERT"
            );

            let ProtocolMessage::Bind(bind) = &request.messages[1] else {
                panic!("expected bind");
            };

            let decode = |index: usize, element_oid: i32| {
                let param = bind.parameter(index).unwrap().unwrap();
                Array::decode_typed(param.data(), param.format(), element_oid)
                    .unwrap()
                    .elements()
                    .clone()
            };

            decode(0, 20)
                .into_iter()
                .zip(decode(1, 25))
                .map(|(id, value)| match (id, value) {
                    (Datum::Bigint(id), Datum::Text(value)) => (id, Some(value)),
                    (Datum::Bigint(id), Datum::Null) => (id, None),
                    other => panic!("unexpected row {other:?}"),
                })
                .collect()
        })
        .collect()
}

// Check that unnest(..) INSERT with rows going to both shards gets transformed into one INSERT per shard,
// as well as that the INSERTs send all of the rows once (including the rows that unnest(..) pads with NULLs).
//
// We also check that, if we have a row with a NULL sharding key, that it goes to all shards.
#[tokio::test]
async fn test_insert_split_unnest() {
    let ids: Vec<i64> = (1..=20).collect();
    let values: Vec<String> = (1..=10).map(|id| format!("v{id}")).collect();
    let mut values: Vec<Option<&str>> = values.iter().map(|value| Some(value.as_str())).collect();
    values.push(None);

    let requests = run_test(unnest_insert_messages(
        UNNEST_INSERT,
        &ids,
        &values,
        Format::Text,
    ))
    .await;

    assert_eq!(requests.len(), 2, "expected one INSERT per shard");
    assert_ne!(requests[0].route().shard(), requests[1].route().shard());

    let mut rows: Vec<_> = inserted_rows_by_request(&requests)
        .into_iter()
        .flatten()
        .collect();

    rows.sort();

    let expected: Vec<_> = ids
        .iter()
        .map(|&id| (id, (id <= 10).then(|| format!("v{id}"))))
        .collect();
    assert_eq!(rows, expected);

    let requests = run_test(vec![
        ProtocolMessage::Parse(Parse::new_anonymous(UNNEST_INSERT)),
        ProtocolMessage::Bind(Bind::new_params(
            "",
            &[Parameter::new(b"{1,NULL}"), Parameter::new(b"{a,b}")],
        )),
        ProtocolMessage::Execute(Execute::new()),
        ProtocolMessage::Sync(Sync),
    ])
    .await;

    let all_shards: Vec<_> = requests
        .iter()
        .filter(|request| matches!(request.route().shard(), Shard::All))
        .collect();

    assert_eq!(all_shards.len(), 1);

    let ProtocolMessage::Bind(bind) = &all_shards[0].messages[1] else {
        panic!("expected bind");
    };

    let ids = bind.parameter(0).unwrap().unwrap();
    assert_eq!(
        Array::decode_typed(ids.data(), ids.format(), 20)
            .unwrap()
            .elements(),
        &vec![Datum::Null]
    );
}

// Check that we decode and encode binary arrays and that unnest(..) INSERT w/ all rows going to one shard collapses to one INSERT
#[tokio::test]
async fn test_insert_split_unnest_binary_one_shard() {
    let ids: Vec<i64> = (1..=20).collect();
    let values = vec![Some("a"); 20];
    let requests = run_test(unnest_insert_messages(
        UNNEST_INSERT,
        &ids,
        &values,
        Format::Binary,
    ))
    .await;

    assert_eq!(requests.len(), 2, "expected one INSERT per shard");

    // Two ids that the router sent to the same shard.
    let same_shard_ids: Vec<i64> = inserted_rows_by_request(&requests)[0]
        .iter()
        .take(2)
        .map(|(id, _)| *id)
        .collect();

    let requests = run_test(unnest_insert_messages(
        UNNEST_INSERT,
        &same_shard_ids,
        &[Some("a"), Some("b")],
        Format::Binary,
    ))
    .await;

    assert_eq!(requests.len(), 1, "expected one INSERT for one shard");
    assert_eq!(
        inserted_rows_by_request(&requests)[0],
        vec![
            (same_shard_ids[0], Some("a".to_string())),
            (same_shard_ids[1], Some("b".to_string())),
        ]
    );
}
