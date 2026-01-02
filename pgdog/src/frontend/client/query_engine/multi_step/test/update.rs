use rand::{rng, Rng};

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
        bind::Parameter, Bind, CommandComplete, DataRow, Describe, ErrorResponse, Execute, Flush,
        Format, Parameters, Parse, Protocol, Query, ReadyForQuery, RowDescription, Sync,
        TransactionState,
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
async fn test_row_same_shard_no_transaction() {
    crate::logger();
    let mut client = TestClient::new_rewrites(Parameters::default()).await;

    let shard_0 = client.random_id_for_shard(0);
    let shard_0_1 = client.random_id_for_shard(0);

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'test value')",
            shard_0
        )))
        .await;
    client.read_until('Z').await.unwrap();

    client.client.client_request = ClientRequest::from(vec![Query::new(format!(
        "UPDATE sharded SET id = {} WHERE value = 'test value' AND id = {}",
        shard_0_1, shard_0
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
}

#[tokio::test]
async fn test_no_rows_updated() {
    let mut client = TestClient::new_rewrites(Parameters::default()).await;
    let id = rng().random::<i64>();

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

    let shard_0 = client.random_id_for_shard(0);
    let shard_1 = client.random_id_for_shard(1);

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id) VALUES ({}) ON CONFLICT(id) DO NOTHING",
            shard_0
        )))
        .await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(format!(
            "UPDATE sharded SET id = {} WHERE id = {}",
            shard_1, shard_0
        )))
        .await;
    let err = ErrorResponse::try_from(client.read().await).expect("expected error");
    assert_eq!(
        err.message,
        "sharding key update must be executed inside a transaction"
    );
    // Connection still good.
    client.send_simple(Query::new("SELECT 1")).await;
    client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_move_rows_simple() {
    let mut client = TestClient::new_rewrites(Parameters::default()).await;

    let shard_0_id = client.random_id_for_shard(0);
    let shard_1_id = client.random_id_for_shard(1);

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id) VALUES ({}) ON CONFLICT(id) DO NOTHING",
            shard_0_id
        )))
        .await;
    client.read_until('Z').await.unwrap();

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .try_send_simple(Query::new(format!(
            "UPDATE sharded SET id = {} WHERE id = {} RETURNING id",
            shard_1_id, shard_0_id
        )))
        .await
        .unwrap();

    let reply = client.read_until('Z').await.unwrap();

    let shard_1_id_str = shard_1_id.to_string();
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
                    shard_1_id_str.as_bytes()
                ),
                _ => unreachable!(),
            }
        });
}

#[tokio::test]
async fn test_move_rows_extended() {
    let mut client = TestClient::new_rewrites(Parameters::default()).await;

    let shard_0_id = client.random_id_for_shard(0);
    let shard_1_id = client.random_id_for_shard(1);

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id) VALUES ({}) ON CONFLICT(id) DO NOTHING",
            shard_0_id
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
                Parameter::new(shard_0_id.to_string().as_bytes()),
                Parameter::new(shard_1_id.to_string().as_bytes()),
            ],
        ))
        .await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    let reply = client.read_until('Z').await.unwrap();

    let shard_1_id_str = shard_1_id.to_string();
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
                'D' => assert_eq!(
                    DataRow::try_from(message).unwrap().column(0).unwrap(),
                    shard_1_id_str.as_bytes()
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

    let shard_0_id = client.random_id_for_shard(0);
    let shard_1_id = client.random_id_for_shard(1);

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id) VALUES ({}) ON CONFLICT(id) DO NOTHING",
            shard_0_id
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
                Parameter::new(shard_0_id.to_string().as_bytes()),
                Parameter::new(shard_1_id.to_string().as_bytes()),
            ],
        ))
        .await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    let reply = client.read_until('Z').await.unwrap();

    let shard_1_id_str = shard_1_id.to_string();
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
                    shard_1_id_str.as_bytes()
                ),
                '1' | '2' => (),
                _ => unreachable!(),
            }
        });
}

#[tokio::test]
async fn test_same_shard_binary() {
    let mut client = TestClient::new_rewrites(Parameters::default()).await;
    let id = client.random_id_for_shard(0);
    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id) VALUES ({})",
            id
        )))
        .await;
    client.read_until('Z').await.unwrap();
    let id_2 = client.random_id_for_shard(0);
    client
        .send(Parse::new_anonymous(
            "UPDATE sharded SET id = $1 WHERE id = $2 RETURNING *",
        ))
        .await;
    client
        .send(Bind::new_params_codes(
            "",
            &[
                Parameter::new(&id_2.to_be_bytes()),
                Parameter::new(&id.to_be_bytes()),
            ],
            &[Format::Binary],
        ))
        .await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();
    let messages = client.read_until('Z').await.unwrap();

    messages
        .into_iter()
        .zip(['1', '2', 'D', 'C', 'Z'])
        .for_each(|(message, code)| {
            assert_eq!(message.code(), code);
            if message.code() == 'C' {
                assert_eq!(
                    CommandComplete::try_from(message).unwrap().command(),
                    "UPDATE 1"
                );
            }
        });
}

#[tokio::test]
async fn test_update_with_expr() {
    // Test that UPDATE with expression columns (not simple values) works correctly.
    // This validates the bind parameter alignment fix where expression columns
    // don't consume bind parameter slots.
    //
    // Note: Expressions that reference the original row's columns (like COALESCE(value, 'default'))
    // won't work because they're inserted literally into the INSERT statement where those
    // columns don't exist. Only standalone expressions like 'prefix' || 'suffix' work.
    let mut client = TestClient::new_rewrites(Parameters::default()).await;

    // Use random IDs to avoid conflicts with other tests
    let shard_0_id = client.random_id_for_shard(0);
    let shard_1_id = client.random_id_for_shard(1);

    // Insert a row into shard 0
    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'original') ON CONFLICT(id) DO UPDATE SET value = 'original'",
            shard_0_id
        )))
        .await;
    client.read_until('Z').await.unwrap();

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    // UPDATE that moves row to different shard with an expression column.
    // Use a standalone expression that doesn't reference any columns.
    client
        .try_send_simple(Query::new(format!(
            "UPDATE sharded SET id = {}, value = 'prefix' || '_suffix' WHERE id = {} RETURNING id, value",
            shard_1_id, shard_0_id
        )))
        .await
        .unwrap();

    let reply = client.read_until('Z').await.unwrap();

    let shard_1_id_str = shard_1_id.to_string();
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
                'T' => {
                    let rd = RowDescription::try_from(message).unwrap();
                    assert_eq!(rd.field(0).unwrap().name, "id");
                    assert_eq!(rd.field(1).unwrap().name, "value");
                }
                'D' => {
                    let dr = DataRow::try_from(message).unwrap();
                    assert_eq!(dr.column(0).unwrap(), shard_1_id_str.as_bytes());
                    // The value should be 'prefix_suffix' from the expression
                    assert_eq!(dr.column(1).unwrap(), "prefix_suffix".as_bytes());
                }
                _ => unreachable!(),
            }
        });

    client.send_simple(Query::new("COMMIT")).await;
    client.read_until('Z').await.unwrap();

    // Verify the row was actually moved to the new shard with correct values
    client
        .send_simple(Query::new(format!(
            "SELECT id, value FROM sharded WHERE id = {}",
            shard_1_id
        )))
        .await;
    let reply = client.read_until('Z').await.unwrap();

    let data_row = reply
        .iter()
        .find(|m| m.code() == 'D')
        .expect("should have data row");
    let dr = DataRow::try_from(data_row.clone()).unwrap();
    assert_eq!(dr.column(0).unwrap(), shard_1_id_str.as_bytes());
    assert_eq!(dr.column(1).unwrap(), "prefix_suffix".as_bytes());
}
