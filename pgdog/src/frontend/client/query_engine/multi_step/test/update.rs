use rand::{thread_rng, Rng};

use crate::{
    expect_message,
    frontend::{
        client::{
            query_engine::{multi_step::UpdateMulti, QueryEngineContext},
            test::TestClient,
        },
        ClientRequest,
    },
    net::{
        bind::Parameter, Bind, CommandComplete, DataRow, Describe, Execute, Flush, Parameters,
        Parse, Protocol, Query, ReadyForQuery, RowDescription, Sync, TransactionState,
    },
};

use super::super::super::Error;

async fn same_shard_check(request: ClientRequest) -> Result<(), Error> {
    let mut client = TestClient::new_rewrites(Parameters::default()).await;
    client.client().client_request.extend(request.messages);

    let mut context = QueryEngineContext::new(&mut client.client);
    client.engine.parse_and_rewrite(&mut context).await?;
    client.engine.route_query(&mut context).await?;

    assert!(
        context.client_request.route().shard().is_direct(),
        "UPDATE stmt should be using direct-to-shard routing"
    );

    client.engine.connect(&mut context, None).await?;

    assert!(
        client.engine.backend.is_direct(),
        "backend should be connected with Binding::Direct"
    );

    let rewrite = context
        .client_request
        .ast
        .as_ref()
        .expect("ast to exist")
        .rewrite_plan
        .clone()
        .sharding_key_update
        .clone()
        .expect("sharding key update to exist");

    let mut update = UpdateMulti::new(&mut client.engine, rewrite);
    assert!(
        update.is_same_shard(&context).unwrap(),
        "query should not trigger multi-shard update"
    );

    // Won't error out because the query goes to the same shard
    // as the old shard.
    update.execute(&mut context).await?;

    Ok(())
}

#[tokio::test]
async fn test_update_check_simple() {
    same_shard_check(
        vec![Query::new("UPDATE sharded SET id = 1 WHERE id = 1 AND value = 'test'").into()].into(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_update_check_extended() {
    same_shard_check(
        vec![
            Parse::new_anonymous("UPDATE sharded SET id = $1 WHERE id = $1 AND value = $2").into(),
            Bind::new_params(
                "",
                &[
                    Parameter::new("1234".as_bytes()),
                    Parameter::new("test".as_bytes()),
                ],
            )
            .into(),
            Execute::new().into(),
            Sync.into(),
        ]
        .into(),
    )
    .await
    .unwrap();

    same_shard_check(
        vec![
            Parse::new_anonymous(
                "UPDATE sharded SET id = $1, value = $2 WHERE id = $3 AND value = $4",
            )
            .into(),
            Bind::new_params(
                "",
                &[
                    Parameter::new("1234".as_bytes()),
                    Parameter::new("test".as_bytes()),
                    Parameter::new("1234".as_bytes()),
                    Parameter::new("test2".as_bytes()),
                ],
            )
            .into(),
            Execute::new().into(),
            Sync.into(),
        ]
        .into(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_row_same_shard() {
    crate::logger();
    let mut client = TestClient::new_rewrites(Parameters::default()).await;

    let id = thread_rng().gen::<i64>();

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({id}, 'test value')",
            id = id
        )))
        .await;
    client.read_until('Z').await.unwrap();

    // Start a transaction.
    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    assert!(
        client.client.in_transaction(),
        "client should be in transaction"
    );

    client.client.client_request = ClientRequest::from(vec![Query::new(format!(
        "UPDATE sharded SET id = {} WHERE value = 'test value' AND id = {}",
        id + 1,
        id
    ))
    .into()]);

    let mut context = QueryEngineContext::new(&mut client.client);

    client.engine.parse_and_rewrite(&mut context).await.unwrap();

    assert!(
        context
            .client_request
            .ast
            .as_ref()
            .expect("ast to exist")
            .rewrite_plan
            .sharding_key_update
            .is_some(),
        "sharding key update should exist on the request"
    );

    client.engine.route_query(&mut context).await.unwrap();
    client.engine.execute(&mut context).await.unwrap();

    let cmd = client.read().await;

    assert_eq!(
        CommandComplete::try_from(cmd).unwrap().command(),
        "UPDATE 1"
    );

    expect_message!(client.read().await, ReadyForQuery);

    client.send_simple(Query::new("ROLLBACK")).await;
    assert!(
        !client.client.in_transaction(),
        "client should not be in transaction"
    );

    client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_no_rows_updated() {
    let mut client = TestClient::new_rewrites(Parameters::default()).await;
    let id = thread_rng().gen::<i64>();

    // Transaction not required because
    // it'll check for existing row first (on the same shard).
    client
        .send_simple(Query::new(format!(
            "UPDATE sharded SET id = {} WHERE id = {}",
            id,
            id + 1
        )))
        .await;
    let cc = client.read().await;
    expect_message!(cc.clone(), CommandComplete);
    assert_eq!(CommandComplete::try_from(cc).unwrap().command(), "UPDATE 0");
    expect_message!(client.read().await, ReadyForQuery);
}

#[tokio::test]
async fn test_transaction_required() {
    let mut client = TestClient::new_rewrites(Parameters::default()).await;

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id) VALUES (1) ON CONFLICT(id) DO NOTHING",
        )))
        .await;
    client.read_until('Z').await.unwrap();

    let err = client
        .try_send_simple(Query::new(format!(
            "UPDATE sharded SET id = 11 WHERE id = 1",
        )))
        .await
        .expect_err("expected shard key update to fail without a transaction");
    assert_eq!(
        err.to_string(),
        "sharding key update must be executed inside a transaction"
    );
}

#[tokio::test]
async fn test_move_rows_simple() {
    let mut client = TestClient::new_rewrites(Parameters::default()).await;

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id) VALUES (1) ON CONFLICT(id) DO NOTHING",
        )))
        .await;
    client.read_until('Z').await.unwrap();

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .try_send_simple(Query::new(
            "UPDATE sharded SET id = 11 WHERE id = 1 RETURNING id",
        ))
        .await
        .unwrap();

    let reply = client.read_until('Z').await.unwrap();

    reply
        .into_iter()
        .zip(['T', 'D', 'C', 'Z'])
        .for_each(|(message, code)| {
            assert_eq!(message.code(), code);
            match code {
                'C' => assert_eq!(
                    CommandComplete::try_from(message).unwrap().command(),
                    "UPDATE 1"
                ),
                'Z' => assert!(
                    ReadyForQuery::try_from(message).unwrap().state().unwrap()
                        == TransactionState::InTrasaction
                ),
                'T' => assert_eq!(
                    RowDescription::try_from(message)
                        .unwrap()
                        .field(0)
                        .unwrap()
                        .name,
                    "id"
                ),
                'D' => assert_eq!(
                    DataRow::try_from(message).unwrap().column(0).unwrap(),
                    "11".as_bytes()
                ),
                _ => unreachable!(),
            }
        });
}

#[tokio::test]
async fn test_move_rows_extended() {
    let mut client = TestClient::new_rewrites(Parameters::default()).await;

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id) VALUES (1) ON CONFLICT(id) DO NOTHING",
        )))
        .await;
    client.read_until('Z').await.unwrap();

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .send(Parse::new_anonymous(
            "UPDATE sharded SET id = $2 WHERE id = $1 RETURNING id",
        ))
        .await;
    client
        .send(Bind::new_params(
            "",
            &[
                Parameter::new("1".as_bytes()),
                Parameter::new("11".as_bytes()),
            ],
        ))
        .await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    let reply = client.read_until('Z').await.unwrap();

    reply
        .into_iter()
        .zip(['1', '2', 'D', 'C', 'Z'])
        .for_each(|(message, code)| {
            assert_eq!(message.code(), code);
            match code {
                'C' => assert_eq!(
                    CommandComplete::try_from(message).unwrap().command(),
                    "UPDATE 1"
                ),
                'Z' => assert!(
                    ReadyForQuery::try_from(message).unwrap().state().unwrap()
                        == TransactionState::InTrasaction
                ),
                'T' => assert_eq!(
                    RowDescription::try_from(message)
                        .unwrap()
                        .field(0)
                        .unwrap()
                        .name,
                    "id"
                ),
                'D' => assert_eq!(
                    DataRow::try_from(message).unwrap().column(0).unwrap(),
                    "11".as_bytes()
                ),
                '1' | '2' => (),
                _ => unreachable!(),
            }
        });
}

#[tokio::test]
async fn test_move_rows_prepared() {
    crate::logger();
    let mut client = TestClient::new_rewrites(Parameters::default()).await;

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id) VALUES (1) ON CONFLICT(id) DO NOTHING",
        )))
        .await;
    client.read_until('Z').await.unwrap();

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .send(Parse::named(
            "__test_1",
            "UPDATE sharded SET id = $2 WHERE id = $1 RETURNING id",
        ))
        .await;
    client.send(Describe::new_statement("__test_1")).await;
    client.send(Flush).await;
    client.try_process().await.unwrap();

    let reply = client.read_until('T').await.unwrap();

    reply
        .into_iter()
        .zip(['1', 't', 'T'])
        .for_each(|(message, code)| {
            assert_eq!(message.code(), code);

            match code {
                'T' => assert_eq!(
                    RowDescription::try_from(message)
                        .unwrap()
                        .field(0)
                        .unwrap()
                        .name,
                    "id"
                ),

                't' | '1' => (),
                _ => unreachable!(),
            }
        });

    client
        .send(Bind::new_params(
            "__test_1",
            &[
                Parameter::new("1".as_bytes()),
                Parameter::new("11".as_bytes()),
            ],
        ))
        .await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    let reply = client.read_until('Z').await.unwrap();

    reply
        .into_iter()
        .zip(['2', 'D', 'C', 'Z'])
        .for_each(|(message, code)| {
            assert_eq!(message.code(), code);
            match code {
                'C' => assert_eq!(
                    CommandComplete::try_from(message).unwrap().command(),
                    "UPDATE 1"
                ),
                'Z' => assert!(
                    ReadyForQuery::try_from(message).unwrap().state().unwrap()
                        == TransactionState::InTrasaction
                ),
                'D' => assert_eq!(
                    DataRow::try_from(message).unwrap().column(0).unwrap(),
                    "11".as_bytes()
                ),
                '1' | '2' => (),
                _ => unreachable!(),
            }
        });
}
