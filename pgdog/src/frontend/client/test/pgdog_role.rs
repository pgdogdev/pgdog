use std::{ops::Deref, time::Duration};

use pgdog_config::{ReadWriteSplit, ReadWriteStrategy, SessionRole};
use tokio::time::timeout;

use crate::{
    backend::databases::reload_from_existing,
    config::{config, load_test_replicas, set},
    expect_message,
    net::{
        Bind, CopyData, CopyDone, DataRow, ErrorResponse, Execute, Message, Parameters, Parse,
        Protocol, Query, ReadyForQuery, Sync,
    },
};

use super::test_client::TestClient;

/// `load_test_replicas` has one primary and one replica with round-robin
/// load balancing. With `include_primary`, two consecutive replica requests
/// reach both, so a write that can land on the primary does.
const ATTEMPTS: usize = 2;

const READ_ONLY_QUERY: &str =
    "SELECT (current_setting('default_transaction_read_only') = 'on')::int";

fn configure(user_role: Option<SessionRole>, strategy: ReadWriteStrategy) {
    load_test_replicas();

    let mut config = config().deref().clone();
    for user in &mut config.users.users {
        user.role = user_role;
    }
    config.config.general.read_write_split = ReadWriteSplit::IncludePrimary;
    config.config.general.read_write_strategy = strategy;
    set(config).unwrap();
    reload_from_existing().unwrap();
}

async fn new_client(startup_role: Option<&str>) -> TestClient {
    let mut params = Parameters::default();
    if let Some(role) = startup_role {
        params.insert("pgdog.role", role);
    }

    TestClient::new(params).await
}

async fn setup(table: &str, user_role: Option<SessionRole>) -> TestClient {
    setup_with_strategy(table, user_role, ReadWriteStrategy::Conservative).await
}

async fn setup_with_strategy(
    table: &str,
    user_role: Option<SessionRole>,
    strategy: ReadWriteStrategy,
) -> TestClient {
    configure(user_role, strategy);

    let mut primary = new_client(Some("primary")).await.leak_pool();
    execute(
        &mut primary,
        &format!("CREATE TABLE IF NOT EXISTS {table}(id BIGINT)"),
    )
    .await
    .unwrap();
    execute(&mut primary, &format!("DELETE FROM {table}"))
        .await
        .unwrap();
    execute(&mut primary, &format!("INSERT INTO {table} VALUES (1)"))
        .await
        .unwrap();
    primary
}

async fn execute(client: &mut TestClient, sql: &str) -> Result<Vec<Message>, ErrorResponse> {
    client.send_simple(Query::new(sql)).await;
    finish(client).await
}

async fn execute_extended(
    client: &mut TestClient,
    sql: &str,
) -> Result<Vec<Message>, ErrorResponse> {
    client.send(Parse::new_anonymous(sql)).await;
    client.send(Bind::new_statement("")).await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();
    finish(client).await
}

async fn finish(client: &mut TestClient) -> Result<Vec<Message>, ErrorResponse> {
    let result = client.read_until('Z').await;
    if result.is_err() {
        expect_message!(client.read().await, ReadyForQuery);
    }
    result
}

fn first_value_is_one(messages: Vec<Message>) -> bool {
    let row = messages
        .into_iter()
        .find(|message| message.code() == 'D')
        .expect("data row");
    let row = expect_message!(row, DataRow);
    row.get_int(0, true).unwrap() == 1
}

async fn read_only(client: &mut TestClient, comment: &str) -> bool {
    first_value_is_one(
        execute(client, &format!("{comment} {READ_ONLY_QUERY}"))
            .await
            .unwrap(),
    )
}

fn count_query(table: &str, comment: &str) -> String {
    format!("{comment} SELECT (count(*) = 1 AND min(id) = 1)::int FROM {table}")
}

async fn assert_unchanged(primary: &mut TestClient, table: &str) {
    let messages = execute(primary, &count_query(table, "")).await.unwrap();
    assert!(first_value_is_one(messages), "data changed in {table}");
}

async fn assert_read(client: &mut TestClient, table: &str, comment: &str) {
    let messages = execute(client, &count_query(table, comment))
        .await
        .expect("read failed");
    assert!(first_value_is_one(messages), "read returned wrong data");
}

fn writes(table: &str, comment: &str) -> [String; 3] {
    [
        format!("{comment} INSERT INTO {table} VALUES (2)"),
        format!("{comment} UPDATE {table} SET id = 2"),
        format!("{comment} DELETE FROM {table}"),
    ]
}

async fn assert_replica(client: &mut TestClient, primary: &mut TestClient, table: &str) {
    assert_replica_with_comment(client, primary, table, "").await;
}

async fn assert_replica_with_comment(
    client: &mut TestClient,
    primary: &mut TestClient,
    table: &str,
    comment: &str,
) {
    for _ in 0..ATTEMPTS {
        assert_read(client, table, comment).await;
    }

    for sql in writes(table, comment) {
        for _ in 0..ATTEMPTS {
            assert!(
                execute(client, &sql).await.is_err(),
                "write went to the primary: {sql}"
            );
        }
    }

    assert_unchanged(primary, table).await;
}

async fn assert_primary(client: &mut TestClient, table: &str) {
    assert_primary_with_comment(client, table, "").await;
}

async fn assert_primary_with_comment(client: &mut TestClient, table: &str, comment: &str) {
    for _ in 0..ATTEMPTS {
        assert!(!read_only(client, comment).await, "read went to a replica");
        execute(client, &format!("{comment} UPDATE {table} SET id = 1"))
            .await
            .expect("write went to a replica");
    }
}

async fn assert_replica_in_transaction(
    client: &mut TestClient,
    primary: &mut TestClient,
    table: &str,
    prelude: &[&str],
    comment: &str,
) {
    for sql in writes(table, comment) {
        for _ in 0..ATTEMPTS {
            execute(client, "BEGIN").await.unwrap();
            for statement in prelude {
                execute(client, statement).await.unwrap();
            }
            assert_read(client, table, comment).await;
            assert!(
                execute(client, &sql).await.is_err(),
                "write went to the primary: {sql}"
            );
            execute(client, "ROLLBACK").await.unwrap();
        }
    }

    assert_unchanged(primary, table).await;
}

async fn assert_primary_in_transaction(
    client: &mut TestClient,
    table: &str,
    prelude: &[&str],
    comment: &str,
) {
    for _ in 0..ATTEMPTS {
        execute(client, "BEGIN").await.unwrap();
        for statement in prelude {
            execute(client, statement).await.unwrap();
        }
        assert!(!read_only(client, comment).await, "read went to a replica");
        execute(client, &format!("{comment} UPDATE {table} SET id = 1"))
            .await
            .expect("write went to a replica");
        execute(client, "ROLLBACK").await.unwrap();
    }
}

#[tokio::test]
async fn test_startup_replica() {
    let table = "test_startup_replica";
    let mut primary = setup(table, None).await;
    let mut client = new_client(Some("replica")).await;

    assert_replica(&mut client, &mut primary, table).await;
}

#[tokio::test]
async fn test_startup_primary() {
    let table = "test_startup_primary";
    setup(table, None).await;
    let mut client = new_client(Some("primary")).await;

    assert_primary(&mut client, table).await;
}

#[tokio::test]
async fn test_set_replica() {
    let table = "test_set_replica";
    let mut primary = setup(table, None).await;
    let mut client = new_client(None).await;
    execute(&mut client, "SET pgdog.role TO 'replica'")
        .await
        .unwrap();

    assert_replica(&mut client, &mut primary, table).await;
}

#[tokio::test]
async fn test_set_primary() {
    let table = "test_set_primary";
    setup(table, None).await;
    let mut client = new_client(None).await;
    execute(&mut client, "SET pgdog.role TO 'primary'")
        .await
        .unwrap();

    assert_primary(&mut client, table).await;
}

#[tokio::test]
async fn test_startup_replica_set_primary() {
    let table = "test_startup_replica_set_primary";
    setup(table, None).await;
    let mut client = new_client(Some("replica")).await;
    execute(&mut client, "SET pgdog.role TO 'primary'")
        .await
        .unwrap();

    assert_primary(&mut client, table).await;
}

#[tokio::test]
async fn test_startup_primary_set_replica() {
    let table = "test_startup_primary_set_replica";
    let mut primary = setup(table, None).await;
    let mut client = new_client(Some("primary")).await;
    execute(&mut client, "SET pgdog.role TO 'replica'")
        .await
        .unwrap();

    assert_replica(&mut client, &mut primary, table).await;
}

#[tokio::test]
async fn test_startup_replica_transaction() {
    let table = "test_startup_replica_transaction";
    let mut primary = setup(table, None).await;
    let mut client = new_client(Some("replica")).await;

    assert_replica_in_transaction(&mut client, &mut primary, table, &[], "").await;
}

#[tokio::test]
async fn test_startup_replica_set_local_primary() {
    let table = "test_startup_replica_set_local_primary";
    setup(table, None).await;
    let mut client = new_client(Some("replica")).await;

    assert_primary_in_transaction(
        &mut client,
        table,
        &["SET LOCAL pgdog.role TO 'primary'"],
        "",
    )
    .await;
}

#[tokio::test]
async fn test_startup_primary_set_local_replica() {
    let table = "test_startup_primary_set_local_replica";
    let mut primary = setup(table, None).await;
    let mut client = new_client(Some("primary")).await;

    assert_replica_in_transaction(
        &mut client,
        &mut primary,
        table,
        &["SET LOCAL pgdog.role TO 'replica'"],
        "",
    )
    .await;
}

#[tokio::test]
async fn test_set_local_replica() {
    let table = "test_set_local_replica";
    let mut primary = setup(table, None).await;
    let mut client = new_client(None).await;

    assert_replica_in_transaction(
        &mut client,
        &mut primary,
        table,
        &["SET LOCAL pgdog.role TO 'replica'"],
        "",
    )
    .await;
}

#[tokio::test]
async fn test_startup_replica_comment_primary() {
    let table = "test_startup_replica_comment_primary";
    setup(table, None).await;
    let mut client = new_client(Some("replica")).await;

    assert_primary_with_comment(&mut client, table, "/* pgdog_role: primary */").await;
}

#[tokio::test]
async fn test_startup_primary_comment_replica() {
    let table = "test_startup_primary_comment_replica";
    let mut primary = setup(table, None).await;
    let mut client = new_client(Some("primary")).await;

    assert_replica_with_comment(
        &mut client,
        &mut primary,
        table,
        "/* pgdog_role: replica */",
    )
    .await;
}

#[tokio::test]
async fn test_comment_replica() {
    let table = "test_comment_replica";
    let mut primary = setup(table, None).await;
    let mut client = new_client(None).await;

    assert_replica_with_comment(
        &mut client,
        &mut primary,
        table,
        "/* pgdog_role: replica */",
    )
    .await;
}

#[tokio::test]
async fn test_set_replica_comment_primary() {
    let table = "test_set_replica_comment_primary";
    setup(table, None).await;
    let mut client = new_client(None).await;
    execute(&mut client, "SET pgdog.role TO 'replica'")
        .await
        .unwrap();

    assert_primary_with_comment(&mut client, table, "/* pgdog_role: primary */").await;
}

#[tokio::test]
async fn test_set_primary_comment_replica() {
    let table = "test_set_primary_comment_replica";
    let mut primary = setup(table, None).await;
    let mut client = new_client(None).await;
    execute(&mut client, "SET pgdog.role TO 'primary'")
        .await
        .unwrap();

    assert_replica_with_comment(
        &mut client,
        &mut primary,
        table,
        "/* pgdog_role: replica */",
    )
    .await;
}

#[tokio::test]
async fn test_startup_primary_set_replica_reset() {
    let table = "test_startup_primary_set_replica_reset";
    setup(table, None).await;
    let mut client = new_client(Some("primary")).await;
    execute(&mut client, "SET pgdog.role TO 'replica'")
        .await
        .unwrap();
    execute(&mut client, "RESET pgdog.role").await.unwrap();

    assert_primary(&mut client, table).await;
}

#[tokio::test]
async fn test_startup_replica_set_primary_reset() {
    let table = "test_startup_replica_set_primary_reset";
    let mut primary = setup(table, None).await;
    let mut client = new_client(Some("replica")).await;
    execute(&mut client, "SET pgdog.role TO 'primary'")
        .await
        .unwrap();
    execute(&mut client, "RESET pgdog.role").await.unwrap();

    assert_replica(&mut client, &mut primary, table).await;
}

#[tokio::test]
async fn test_startup_replica_set_primary_reset_all() {
    let table = "test_startup_replica_set_primary_reset_all";
    let mut primary = setup(table, None).await;
    let mut client = new_client(Some("replica")).await;
    execute(&mut client, "SET pgdog.role TO 'primary'")
        .await
        .unwrap();
    execute(&mut client, "RESET ALL").await.unwrap();

    assert_replica(&mut client, &mut primary, table).await;
}

#[tokio::test]
async fn test_user_replica() {
    let table = "test_user_replica";
    let mut primary = setup(table, Some(SessionRole::Replica)).await;
    let mut client = new_client(None).await;

    assert_replica(&mut client, &mut primary, table).await;
}

#[tokio::test]
async fn test_user_primary() {
    let table = "test_user_primary";
    setup(table, Some(SessionRole::Primary)).await;
    let mut client = new_client(None).await;

    assert_primary(&mut client, table).await;
}

#[tokio::test]
async fn test_user_replica_startup_primary() {
    let table = "test_user_replica_startup_primary";
    setup(table, Some(SessionRole::Replica)).await;
    let mut client = new_client(Some("primary")).await;

    assert_primary(&mut client, table).await;
}

#[tokio::test]
async fn test_user_primary_startup_replica() {
    let table = "test_user_primary_startup_replica";
    let mut primary = setup(table, Some(SessionRole::Primary)).await;
    let mut client = new_client(Some("replica")).await;

    assert_replica(&mut client, &mut primary, table).await;
}

#[tokio::test]
async fn test_user_replica_set_primary() {
    let table = "test_user_replica_set_primary";
    setup(table, Some(SessionRole::Replica)).await;
    let mut client = new_client(None).await;
    execute(&mut client, "SET pgdog.role TO 'primary'")
        .await
        .unwrap();

    assert_primary(&mut client, table).await;
}

#[tokio::test]
async fn test_user_primary_set_replica() {
    let table = "test_user_primary_set_replica";
    let mut primary = setup(table, Some(SessionRole::Primary)).await;
    let mut client = new_client(None).await;
    execute(&mut client, "SET pgdog.role TO 'replica'")
        .await
        .unwrap();

    assert_replica(&mut client, &mut primary, table).await;
}

#[tokio::test]
async fn test_user_replica_set_local_primary() {
    let table = "test_user_replica_set_local_primary";
    setup(table, Some(SessionRole::Replica)).await;
    let mut client = new_client(None).await;

    assert_primary_in_transaction(
        &mut client,
        table,
        &["SET LOCAL pgdog.role TO 'primary'"],
        "",
    )
    .await;
}

#[tokio::test]
async fn test_user_primary_set_local_replica() {
    let table = "test_user_primary_set_local_replica";
    let mut primary = setup(table, Some(SessionRole::Primary)).await;
    let mut client = new_client(None).await;

    assert_replica_in_transaction(
        &mut client,
        &mut primary,
        table,
        &["SET LOCAL pgdog.role TO 'replica'"],
        "",
    )
    .await;
}

#[tokio::test]
async fn test_user_replica_comment_primary() {
    let table = "test_user_replica_comment_primary";
    setup(table, Some(SessionRole::Replica)).await;
    let mut client = new_client(None).await;

    assert_primary_with_comment(&mut client, table, "/* pgdog_role: primary */").await;
}

#[tokio::test]
async fn test_user_primary_comment_replica() {
    let table = "test_user_primary_comment_replica";
    let mut primary = setup(table, Some(SessionRole::Primary)).await;
    let mut client = new_client(None).await;

    assert_replica_with_comment(
        &mut client,
        &mut primary,
        table,
        "/* pgdog_role: replica */",
    )
    .await;
}

#[tokio::test]
async fn test_user_replica_set_primary_reset() {
    let table = "test_user_replica_set_primary_reset";
    let mut primary = setup(table, Some(SessionRole::Replica)).await;
    let mut client = new_client(None).await;
    execute(&mut client, "SET pgdog.role TO 'primary'")
        .await
        .unwrap();
    execute(&mut client, "RESET pgdog.role").await.unwrap();

    assert_replica(&mut client, &mut primary, table).await;
}

#[tokio::test]
async fn test_user_replica_startup_invalid() {
    let table = "test_user_replica_startup_invalid";
    let mut primary = setup(table, Some(SessionRole::Replica)).await;
    let mut client = new_client(Some("auto")).await;

    assert_replica(&mut client, &mut primary, table).await;
}

#[tokio::test]
async fn test_user_primary_startup_invalid() {
    let table = "test_user_primary_startup_invalid";
    setup(table, Some(SessionRole::Primary)).await;
    let mut client = new_client(Some("auto")).await;

    assert_primary(&mut client, table).await;
}

#[tokio::test]
async fn test_transaction_comment_replica() {
    let table = "test_transaction_comment_replica";
    setup(table, None).await;
    let mut client = new_client(None).await;

    assert_primary_in_transaction(&mut client, table, &[], "/* pgdog_role: replica */").await;
}

#[tokio::test]
async fn test_aggressive_transaction_comment_replica() {
    let table = "test_aggressive_transaction_comment_replica";
    let mut primary = setup_with_strategy(table, None, ReadWriteStrategy::Aggressive).await;
    let mut client = new_client(None).await;

    assert_replica_in_transaction(
        &mut client,
        &mut primary,
        table,
        &[],
        "/* pgdog_role: replica */",
    )
    .await;
}

#[tokio::test]
async fn test_startup_primary_connected_set_local_replica() {
    let table = "test_startup_primary_connected_set_local_replica";
    setup(table, None).await;
    let mut client = new_client(Some("primary")).await;

    for _ in 0..ATTEMPTS {
        execute(&mut client, "BEGIN").await.unwrap();
        assert!(!read_only(&mut client, "").await, "read went to a replica");
        execute(&mut client, "SET LOCAL pgdog.role TO 'replica'")
            .await
            .unwrap();
        assert!(
            !read_only(&mut client, "").await,
            "transaction left the primary"
        );
        execute(&mut client, "ROLLBACK").await.unwrap();
    }
}

#[tokio::test]
async fn test_startup_replica_extended() {
    let table = "test_startup_replica_extended";
    let mut primary = setup(table, None).await;
    let mut client = new_client(Some("replica")).await;

    for sql in writes(table, "") {
        for _ in 0..ATTEMPTS {
            assert!(
                execute_extended(&mut client, &sql).await.is_err(),
                "write went to the primary: {sql}"
            );
        }
    }

    assert_unchanged(&mut primary, table).await;
}

#[tokio::test]
async fn test_startup_primary_extended() {
    let table = "test_startup_primary_extended";
    setup(table, None).await;
    let mut client = new_client(Some("primary")).await;

    for _ in 0..ATTEMPTS {
        let messages = execute_extended(&mut client, READ_ONLY_QUERY)
            .await
            .unwrap();
        assert!(!first_value_is_one(messages), "read went to a replica");
        execute_extended(&mut client, &format!("UPDATE {table} SET id = 1"))
            .await
            .expect("write went to a replica");
    }
}

#[tokio::test]
async fn test_startup_replica_copy() {
    let table = "test_startup_replica_copy";
    let mut primary = setup(table, None).await;
    let mut client = new_client(Some("replica")).await;

    for _ in 0..ATTEMPTS {
        let result = timeout(
            Duration::from_millis(200),
            execute(&mut client, &format!("COPY {table} FROM STDIN")),
        )
        .await
        .expect("COPY went to the primary");
        assert!(result.is_err(), "COPY went to the primary");
    }

    assert_unchanged(&mut primary, table).await;
}

#[tokio::test]
async fn test_startup_primary_copy() {
    let table = "test_startup_primary_copy";
    setup(table, None).await;
    let mut client = new_client(Some("primary")).await;

    client
        .send_simple(Query::new(format!("COPY {table} FROM STDIN")))
        .await;
    client
        .read_until('G')
        .await
        .expect("COPY went to a replica");
    client.send(CopyData::new(b"1\n")).await;
    client.send(CopyDone).await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();
}
