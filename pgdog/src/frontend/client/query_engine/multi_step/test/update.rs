use crate::{
    backend::pool::connection::binding::Binding,
    expect_message,
    frontend::{
        ClientRequest,
        client::{
            query_engine::{QueryEngineContext, multi_step::error::Error},
            test::TestClient,
        },
    },
    net::{
        Bind, CommandComplete, DataRow, Describe, ErrorResponse, Execute, Flush, Format,
        Parameters, Parse, Protocol, Query, ReadyForQuery, RowDescription, Sync, TransactionState,
        bind::Parameter,
    },
};

const FK_CHILD_TABLE: &str = "shard_key_update_fk_child";

const SHARDED_TABLE_DDL: &str = "CREATE TABLE IF NOT EXISTS sharded (
    id BIGINT PRIMARY KEY,
    value TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    enabled BOOLEAN DEFAULT false,
    user_id BIGINT,
    region_id INTEGER DEFAULT 10,
    country_id SMALLINT DEFAULT 5,
    options JSONB DEFAULT '{}'::jsonb
)";

async fn ensure_sharded_table(client: &mut TestClient) {
    client.send(Query::new(SHARDED_TABLE_DDL)).await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();
}

async fn setup_fk_child(client: &mut TestClient, on_delete: &str) {
    ensure_sharded_table(client).await;

    client
        .send(Query::new(format!("DROP TABLE IF EXISTS {FK_CHILD_TABLE}")))
        .await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    client
        .send(Query::new(format!(
            "CREATE TABLE {FK_CHILD_TABLE} (
                id BIGINT PRIMARY KEY,
                parent_id BIGINT NOT NULL REFERENCES sharded(id) ON DELETE {on_delete}
            )"
        )))
        .await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();
}

async fn cleanup_fk_child(client: &mut TestClient) {
    client
        .send(Query::new(format!("DROP TABLE IF EXISTS {FK_CHILD_TABLE}")))
        .await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();
}

async fn same_shard_check(request: ClientRequest) -> Result<(), Error> {
    let mut client = TestClient::new_rewrites(Parameters::default()).await;
    client.client().client_request.extend(request.messages);

    let mut context = QueryEngineContext::new(&mut client.client);
    let (query_planner, offset_plan) = client.engine.parse_and_rewrite(&mut context).await?;
    client
        .engine
        .route_query(&mut context, offset_plan.as_ref())
        .await?;

    assert!(
        context.client_request.route().shard().is_direct(),
        "UPDATE stmt should be using direct-to-shard routing"
    );

    client.engine.connect(&mut context, None).await?;

    std::assert_matches!(&*client.engine.backend, Binding::Direct(..));

    let ast = context.client_request.ast.clone().expect("ast was set");
    assert!(
        ast.rewrite_plan.sharding_key_update,
        "sharding key update to exist"
    );

    assert!(
        query_planner.is_none(),
        "query should not trigger multi-shard update"
    );

    // Won't error out because the query goes to the same shard
    // as the old shard.
    client.engine.execute(&mut context, query_planner).await?;

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

    client.client.client_request = ClientRequest::from(vec![
        Query::new(format!(
            "UPDATE sharded SET id = {} WHERE value = 'test value' AND id = {}",
            shard_0_1, shard_0
        ))
        .into(),
    ]);

    let mut context = QueryEngineContext::new(&mut client.client);

    let (query_planner, offset_plan) = client.engine.parse_and_rewrite(&mut context).await.unwrap();

    assert!(
        context
            .client_request
            .ast
            .as_ref()
            .expect("ast to exist")
            .rewrite_plan
            .sharding_key_update,
        "sharding key update should exist on the request"
    );

    client
        .engine
        .route_query(&mut context, offset_plan.as_ref())
        .await
        .unwrap();
    client
        .engine
        .execute(&mut context, query_planner)
        .await
        .unwrap();

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

    // Ensure that we generate 2 random IDs consistent with the same shard.
    let shard_0_id_1 = client.random_id_for_shard(0);
    let shard_0_id_2 = client.random_id_for_shard(0);

    // Transaction not required because
    // it'll check for existing row first (on the same shard).
    client
        .send_simple(Query::new(format!(
            "UPDATE sharded SET id = {} WHERE id = {}",
            shard_0_id_1, shard_0_id_2
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

#[tokio::test]
async fn test_foreign_key_on_delete_sharding_key_update() {
    let mut client = TestClient::new_rewrites(Parameters::default()).await;

    setup_fk_child(&mut client, "CASCADE").await;

    let shard_0_id = client.random_id_for_shard(0);
    let shard_0_other_id = client.random_id_for_shard(0);
    let shard_1_id = client.random_id_for_shard(1);

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id) VALUES ({}) ON CONFLICT(id) DO NOTHING",
            shard_0_id
        )))
        .await;
    client.read_until('Z').await.unwrap();

    // Same-shard sharding key updates are still allowed with destructive FKs.
    client
        .try_send_simple(Query::new(format!(
            "UPDATE sharded SET id = {} WHERE id = {}",
            shard_0_other_id, shard_0_id
        )))
        .await
        .unwrap();
    let cmd = client.read().await;
    assert_eq!(
        CommandComplete::try_from(cmd).unwrap().command(),
        "UPDATE 1"
    );
    expect_message!(client.read().await, ReadyForQuery);

    // Cross-shard move with ON DELETE CASCADE is blocked before delete/insert.
    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(format!(
            "UPDATE sharded SET id = {} WHERE id = {}",
            shard_1_id, shard_0_other_id
        )))
        .await;
    let err = ErrorResponse::try_from(client.read().await).expect("expected error");
    assert!(
        err.message.contains(
            "sharding key update would move a row referenced by an ON DELETE foreign key"
        ),
        "unexpected error message: {}",
        err.message
    );
    client.read_until('Z').await.unwrap();
    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();

    client.send_simple(Query::new("SELECT 1")).await;
    client.read_until('Z').await.unwrap();

    // ON DELETE RESTRICT does not trigger the PgDog block.
    setup_fk_child(&mut client, "RESTRICT").await;

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id) VALUES ({}) ON CONFLICT(id) DO NOTHING",
            shard_0_other_id
        )))
        .await;
    client.read_until('Z').await.unwrap();

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .try_send_simple(Query::new(format!(
            "UPDATE sharded SET id = {} WHERE id = {}",
            shard_1_id, shard_0_other_id
        )))
        .await
        .unwrap();

    let reply = client.read_until('Z').await.unwrap();
    reply
        .into_iter()
        .zip(['C', 'Z'])
        .for_each(|(message, code)| {
            assert_eq!(message.code(), code);
            match code {
                'C' => assert_eq!(
                    CommandComplete::try_from(message).unwrap().command(),
                    "UPDATE 1"
                ),
                'Z' => assert_eq!(
                    ReadyForQuery::try_from(message).unwrap().state().unwrap(),
                    TransactionState::InTrasaction
                ),
                _ => unreachable!(),
            }
        });

    client.send_simple(Query::new("COMMIT")).await;
    client.read_until('Z').await.unwrap();

    cleanup_fk_child(&mut client).await;
}

#[tokio::test]
async fn test_move_rows_insert_error_is_reported() {
    let mut client = TestClient::new_rewrites(Parameters::default()).await;
    ensure_sharded_table(&mut client).await;

    let shard_0_id = client.random_id_for_shard(0);
    let shard_1_id = client.random_id_for_shard(1);

    // We want the INSERT step to hit a duplicate key.
    for id in [shard_0_id, shard_1_id] {
        client
            .send_simple(Query::new(format!(
                "INSERT INTO sharded (id) VALUES ({}) ON CONFLICT(id) DO NOTHING",
                id
            )))
            .await;
        client.read_until('Z').await.unwrap();
    }

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(format!(
            "UPDATE sharded SET id = {} WHERE id = {}",
            shard_1_id, shard_0_id
        )))
        .await;
    let error = ErrorResponse::try_from(client.read().await)
        .expect("expected error from failed insert step");
    assert_eq!(error.code, "23505", "{error:?}");
    expect_message!(client.read().await, ReadyForQuery);

    // Connection still good (no transaction issues)
    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();
}

/// Note: this is a case where we have to use a `RowDescription` over the cache.
#[tokio::test]
async fn test_move_rows_after_table_recreated() {
    // Schema cache is stale (maybe due to a migration or something else)
    let mut client = TestClient::new_rewrites(Parameters::default())
        .await
        .without_schema_reload();

    // Recreate the table with fewer columns than the schema pgdog loaded.
    for ddl in [
        "DROP TABLE IF EXISTS sharded CASCADE",
        "CREATE TABLE sharded (id BIGINT PRIMARY KEY, value TEXT)",
    ] {
        client.send(Query::new(ddl)).await;
        client.try_process().await.unwrap();
        client.read_until('Z').await.unwrap();
    }

    let shard_0_id = client.random_id_for_shard(0);
    let shard_1_id = client.random_id_for_shard(1);

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'test')",
            shard_0_id
        )))
        .await;
    client.read_until('Z').await.unwrap();

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(format!(
            "UPDATE sharded SET id = {} WHERE id = {}",
            shard_1_id, shard_0_id
        )))
        .await;
    let update_reply = client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(format!(
            "SELECT id FROM sharded WHERE id = {}",
            shard_1_id
        )))
        .await;
    let select_reply = client.read_until('Z').await.unwrap();

    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();

    // Restore the table before asserting so other tests aren't affected.
    client
        .send(Query::new("DROP TABLE IF EXISTS sharded"))
        .await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();
    ensure_sharded_table(&mut client).await;

    let cc = update_reply
        .iter()
        .find(|message| message.code() == 'C')
        .cloned()
        .unwrap_or_else(|| panic!("expected UPDATE to succeed, got: {update_reply:?}"));
    assert_eq!(CommandComplete::try_from(cc).unwrap().command(), "UPDATE 1");
    assert_eq!(
        select_reply
            .iter()
            .filter(|message| message.code() == 'D')
            .count(),
        1,
        "row should exist on the new shard: {select_reply:?}"
    );
}

#[tokio::test]
async fn test_move_rows_sqlx_flow() {
    let client = TestClient::new_rewrites(Parameters::default()).await;
    sqlx_flow(client).await;
}

#[tokio::test]
async fn test_move_rows_sqlx_flow_two_pc() {
    let client = TestClient::new_rewrites(Parameters::default())
        .await
        .with_two_pc();
    sqlx_flow(client).await;
}

async fn sqlx_flow(mut client: TestClient) {
    ensure_sharded_table(&mut client).await;

    let shard_0_id = client.random_id_for_shard(0);
    let shard_1_id = client.random_id_for_shard(1);

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'test')",
            shard_0_id
        )))
        .await;
    client.read_until('Z').await.unwrap();

    client
        .send(Parse::named(
            "sqlx_s_1",
            "UPDATE sharded SET id = $2 WHERE id = $1",
        ))
        .await;
    client.send(Describe::new_statement("sqlx_s_1")).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    let bind = || {
        Bind::new_params_codes_results(
            "sqlx_s_1",
            &[
                Parameter::new(&shard_0_id.to_be_bytes()),
                Parameter::new(&shard_1_id.to_be_bytes()),
            ],
            &[Format::Binary, Format::Binary],
            &[1],
        )
    };

    // Connection stays usable.
    client.send(bind()).await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();
    let error = ErrorResponse::try_from(client.read().await).expect("expected error");
    assert_eq!(
        error.message,
        "sharding key update must be executed inside a transaction"
    );
    expect_message!(client.read().await, ReadyForQuery);

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    // Same cached statement. Bind/Execute/Sync only.
    client.send(bind()).await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();
    let reply = client.read_until('Z').await.unwrap();
    let cc = reply
        .iter()
        .find(|message| message.code() == 'C')
        .cloned()
        .unwrap_or_else(|| panic!("expected CommandComplete, got: {reply:?}"));
    assert_eq!(CommandComplete::try_from(cc).unwrap().command(), "UPDATE 1");

    for query in [
        "SELECT id FROM sharded WHERE id = {}",
        "COMMIT",
        "SELECT id FROM sharded WHERE id = {}",
    ] {
        client
            .send_simple(Query::new(query.replace("{}", &shard_1_id.to_string())))
            .await;
        let reply = client.read_until('Z').await.unwrap();
        if query != "COMMIT" {
            assert_eq!(
                reply.iter().filter(|message| message.code() == 'D').count(),
                1,
                "row should be on the new shard: {reply:?}"
            );
        }
    }

    // Cleanup
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({}, {})",
            shard_0_id, shard_1_id
        )))
        .await;
    client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_move_rows_zero_rows() {
    let mut client = TestClient::new_rewrites(Parameters::default()).await;
    ensure_sharded_table(&mut client).await;

    let shard_0_id = client.random_id_for_shard(0);
    let shard_1_id = client.random_id_for_shard(1);

    // No row with shard_0_id exists.
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id = {}",
            shard_0_id
        )))
        .await;
    client.read_until('Z').await.unwrap();

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(format!(
            "UPDATE sharded SET id = {} WHERE id = {}",
            shard_1_id, shard_0_id
        )))
        .await;
    let cc = client.read().await;
    expect_message!(cc.clone(), CommandComplete);
    assert_eq!(CommandComplete::try_from(cc).unwrap().command(), "UPDATE 0");
    expect_message!(client.read().await, ReadyForQuery);

    // Transaction still healthy.
    client.send_simple(Query::new("COMMIT")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(format!(
            "SELECT id FROM sharded WHERE id = {}",
            shard_1_id
        )))
        .await;
    let reply = client.read_until('Z').await.unwrap();
    assert_eq!(
        reply.iter().filter(|message| message.code() == 'D').count(),
        0,
        "no row should have been created: {reply:?}"
    );
}

#[tokio::test]
async fn test_shard_key_update_disabled_does_not_execute() {
    let mut client = TestClient::new_rewrites(Parameters::default())
        .await
        .with_shard_key_error();
    ensure_sharded_table(&mut client).await;

    let shard_0_id = client.random_id_for_shard(0);
    let shard_1_id = client.random_id_for_shard(1);

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id) VALUES ({}) ON CONFLICT(id) DO NOTHING",
            shard_0_id
        )))
        .await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(format!(
            "UPDATE sharded SET id = {} WHERE id = {}",
            shard_1_id, shard_0_id
        )))
        .await;
    let error = ErrorResponse::try_from(client.read().await).expect("expected error");
    assert_eq!(
        error.message, "sharding key updates are forbidden",
        "{error:?}"
    );
    expect_message!(client.read().await, ReadyForQuery);

    // Row untouched.
    client
        .send_simple(Query::new(format!(
            "SELECT id FROM sharded WHERE id = {}",
            shard_0_id
        )))
        .await;
    let reply = client.read_until('Z').await.unwrap();
    assert_eq!(
        reply.iter().filter(|message| message.code() == 'D').count(),
        1,
        "row must still exist under its original id: {reply:?}"
    );
}

#[tokio::test]
async fn test_move_rows_error_then_commit() {
    let mut client = TestClient::new_rewrites(Parameters::default()).await;
    ensure_sharded_table(&mut client).await;

    let shard_0_id = client.random_id_for_shard(0);
    let shard_1_id = client.random_id_for_shard(1);

    // INSERT step hits a duplicate key.
    for id in [shard_0_id, shard_1_id] {
        client
            .send_simple(Query::new(format!(
                "INSERT INTO sharded (id) VALUES ({}) ON CONFLICT(id) DO NOTHING",
                id
            )))
            .await;
        client.read_until('Z').await.unwrap();
    }

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(format!(
            "UPDATE sharded SET id = {} WHERE id = {}",
            shard_1_id, shard_0_id
        )))
        .await;
    let error = ErrorResponse::try_from(client.read().await).expect("expected error");
    assert_eq!(error.code, "23505", "{error:?}");
    expect_message!(client.read().await, ReadyForQuery);

    // The transaction failed mid-plan (DELETE ran, INSERT errored).
    // COMMIT must roll back.
    client.send_simple(Query::new("COMMIT")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(format!(
            "SELECT id FROM sharded WHERE id = {}",
            shard_0_id
        )))
        .await;
    let reply = client.read_until('Z').await.unwrap();
    assert_eq!(
        reply.iter().filter(|message| message.code() == 'D').count(),
        1,
        "the failed move must not commit its DELETE: {reply:?}"
    );
}
