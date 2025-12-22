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
        bind::Parameter, Bind, CommandComplete, Execute, Parameters, Parse, Query, ReadyForQuery,
        Sync,
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
