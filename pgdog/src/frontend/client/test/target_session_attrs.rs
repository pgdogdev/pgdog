use std::ops::Deref;

use pgdog_config::{ReadWriteSplit, SessionRole};

use crate::{
    backend::databases::reload_from_existing,
    config::{config, load_test_replicas, set},
    expect_message,
    net::{DataRow, ErrorResponse, Message, Parameters, Protocol, Query, ReadyForQuery},
};

use super::test_client::TestClient;

async fn new_replicas_exclude_primary(startup_role: Option<&str>) -> TestClient {
    new_replicas_with_user_role(startup_role, None).await
}

async fn new_replicas_with_user_role(
    startup_role: Option<&str>,
    user_role: Option<SessionRole>,
) -> TestClient {
    load_test_replicas();

    let mut config = config().deref().clone();
    for user in &mut config.users.users {
        user.role = user_role;
    }
    config.config.general.read_write_split = ReadWriteSplit::ExcludePrimary;
    set(config).unwrap();
    reload_from_existing().unwrap();

    let mut params = Parameters::default();
    if let Some(role) = startup_role {
        params.insert("pgdog.role", role);
    }

    TestClient::new(params).await
}

async fn execute(client: &mut TestClient, sql: &str) -> Result<Vec<Message>, ErrorResponse> {
    client.send_simple(Query::new(sql)).await;
    let result = client.read_until('Z').await;
    if result.is_err() {
        expect_message!(client.read().await, ReadyForQuery);
    }
    result
}

async fn create_table(client: &mut TestClient, table: &str) -> Result<(), ErrorResponse> {
    execute(
        client,
        &format!("CREATE TABLE IF NOT EXISTS {table}(id BIGINT)"),
    )
    .await
    .map(|_| ())
}

async fn read_only(client: &mut TestClient) -> bool {
    read_only_with_comment(client, "").await
}

async fn read_only_with_comment(client: &mut TestClient, comment: &str) -> bool {
    let messages = execute(
        client,
        &format!("{comment} SELECT (current_setting('default_transaction_read_only') = 'on')::int"),
    )
    .await
    .unwrap();
    let row = messages
        .into_iter()
        .find(|message| message.code() == 'D')
        .expect("data row");
    let row = expect_message!(row, DataRow);
    row.get_int(0, true).unwrap() == 1
}

async fn assert_primary(client: &mut TestClient, table: &str) {
    for _ in 0..5 {
        assert!(!read_only(client).await, "read went to a replica");
    }
    create_table(client, table)
        .await
        .expect("write went to a replica");
}

async fn assert_replica(client: &mut TestClient, table: &str) {
    for _ in 0..5 {
        assert!(read_only(client).await, "read went to the primary");
    }
    let err = create_table(client, table)
        .await
        .expect_err("write went to the primary");
    assert_eq!(
        err.message,
        "cannot execute CREATE TABLE in a read-only transaction"
    );
}

#[tokio::test]
async fn test_startup_replica() {
    let mut client = new_replicas_exclude_primary(Some("replica")).await;
    assert_replica(&mut client, "test_startup_replica").await;
}

#[tokio::test]
async fn test_startup_primary() {
    let mut client = new_replicas_exclude_primary(Some("primary")).await;
    assert_primary(&mut client, "test_startup_primary").await;
}

#[tokio::test]
async fn test_set_replica() {
    let mut client = new_replicas_exclude_primary(None).await;
    execute(&mut client, "SET pgdog.role TO 'replica'")
        .await
        .unwrap();

    assert_replica(&mut client, "test_set_replica").await;
}

#[tokio::test]
async fn test_set_primary() {
    let mut client = new_replicas_exclude_primary(None).await;
    execute(&mut client, "SET pgdog.role TO 'primary'")
        .await
        .unwrap();

    assert_primary(&mut client, "test_set_primary").await;
}

#[tokio::test]
async fn test_startup_replica_set_primary() {
    let mut client = new_replicas_exclude_primary(Some("replica")).await;
    execute(&mut client, "SET pgdog.role TO 'primary'")
        .await
        .unwrap();

    assert_primary(&mut client, "test_startup_replica_set_primary").await;
}

#[tokio::test]
async fn test_startup_primary_set_replica() {
    let mut client = new_replicas_exclude_primary(Some("primary")).await;
    execute(&mut client, "SET pgdog.role TO 'replica'")
        .await
        .unwrap();

    assert_replica(&mut client, "test_startup_primary_set_replica").await;
}

#[tokio::test]
async fn test_startup_replica_transaction() {
    let mut client = new_replicas_exclude_primary(Some("replica")).await;
    execute(&mut client, "BEGIN").await.unwrap();

    assert_replica(&mut client, "test_startup_replica_transaction").await;
    execute(&mut client, "ROLLBACK").await.unwrap();
}

#[tokio::test]
async fn test_startup_replica_set_local_primary() {
    let mut client = new_replicas_exclude_primary(Some("replica")).await;
    execute(&mut client, "BEGIN").await.unwrap();
    execute(&mut client, "SET LOCAL pgdog.role TO 'primary'")
        .await
        .unwrap();

    assert_primary(&mut client, "test_startup_replica_set_local_primary").await;
    execute(&mut client, "ROLLBACK").await.unwrap();
}

#[tokio::test]
async fn test_startup_primary_set_local_replica() {
    let mut client = new_replicas_exclude_primary(Some("primary")).await;
    execute(&mut client, "BEGIN").await.unwrap();
    execute(&mut client, "SET LOCAL pgdog.role TO 'replica'")
        .await
        .unwrap();

    assert_replica(&mut client, "test_startup_primary_set_local_replica").await;
    execute(&mut client, "ROLLBACK").await.unwrap();
}

#[tokio::test]
async fn test_startup_replica_comment_primary() {
    let mut client = new_replicas_exclude_primary(Some("replica")).await;

    for _ in 0..5 {
        assert!(!read_only_with_comment(&mut client, "/* pgdog_role: primary */").await);
    }
}

#[tokio::test]
async fn test_startup_primary_comment_replica() {
    let mut client = new_replicas_exclude_primary(Some("primary")).await;

    for _ in 0..5 {
        assert!(read_only_with_comment(&mut client, "/* pgdog_role: replica */").await);
    }
}

#[tokio::test]
async fn test_set_replica_comment_primary() {
    let mut client = new_replicas_exclude_primary(None).await;
    execute(&mut client, "SET pgdog.role TO 'replica'")
        .await
        .unwrap();

    for _ in 0..5 {
        assert!(!read_only_with_comment(&mut client, "/* pgdog_role: primary */").await);
    }
}

#[tokio::test]
async fn test_set_primary_comment_replica() {
    let mut client = new_replicas_exclude_primary(None).await;
    execute(&mut client, "SET pgdog.role TO 'primary'")
        .await
        .unwrap();

    for _ in 0..5 {
        assert!(read_only_with_comment(&mut client, "/* pgdog_role: replica */").await);
    }
}

#[tokio::test]
async fn test_set_primary_reset() {
    let mut client = new_replicas_exclude_primary(None).await;
    execute(&mut client, "SET pgdog.role TO 'primary'")
        .await
        .unwrap();
    execute(&mut client, "RESET pgdog.role").await.unwrap();

    for _ in 0..5 {
        assert!(read_only(&mut client).await, "read went to the primary");
    }
}

#[tokio::test]
async fn test_startup_primary_set_replica_reset() {
    let mut client = new_replicas_exclude_primary(Some("primary")).await;
    execute(&mut client, "SET pgdog.role TO 'replica'")
        .await
        .unwrap();
    execute(&mut client, "RESET pgdog.role").await.unwrap();

    assert_primary(&mut client, "test_startup_primary_set_replica_reset").await;
}

#[tokio::test]
async fn test_startup_replica_set_primary_reset() {
    let mut client = new_replicas_exclude_primary(Some("replica")).await;
    execute(&mut client, "SET pgdog.role TO 'primary'")
        .await
        .unwrap();
    execute(&mut client, "RESET pgdog.role").await.unwrap();

    assert_replica(&mut client, "test_startup_replica_set_primary_reset").await;
}

#[tokio::test]
async fn test_set_primary_reset_all() {
    let mut client = new_replicas_exclude_primary(None).await;
    execute(&mut client, "SET pgdog.role TO 'primary'")
        .await
        .unwrap();
    execute(&mut client, "RESET ALL").await.unwrap();

    for _ in 0..5 {
        assert!(read_only(&mut client).await, "read went to the primary");
    }
}

#[tokio::test]
async fn test_user_replica() {
    let mut client = new_replicas_with_user_role(None, Some(SessionRole::Replica)).await;
    assert_replica(&mut client, "test_user_replica").await;
}

#[tokio::test]
async fn test_user_primary() {
    let mut client = new_replicas_with_user_role(None, Some(SessionRole::Primary)).await;
    assert_primary(&mut client, "test_user_primary").await;
}

#[tokio::test]
async fn test_user_replica_startup_primary() {
    let mut client = new_replicas_with_user_role(Some("primary"), Some(SessionRole::Replica)).await;
    assert_primary(&mut client, "test_user_replica_startup_primary").await;
}

#[tokio::test]
async fn test_user_primary_startup_replica() {
    let mut client = new_replicas_with_user_role(Some("replica"), Some(SessionRole::Primary)).await;
    assert_replica(&mut client, "test_user_primary_startup_replica").await;
}

#[tokio::test]
async fn test_user_replica_set_primary() {
    let mut client = new_replicas_with_user_role(None, Some(SessionRole::Replica)).await;
    execute(&mut client, "SET pgdog.role TO 'primary'")
        .await
        .unwrap();

    assert_primary(&mut client, "test_user_replica_set_primary").await;
}

#[tokio::test]
async fn test_user_primary_set_replica() {
    let mut client = new_replicas_with_user_role(None, Some(SessionRole::Primary)).await;
    execute(&mut client, "SET pgdog.role TO 'replica'")
        .await
        .unwrap();

    assert_replica(&mut client, "test_user_primary_set_replica").await;
}

#[tokio::test]
async fn test_user_replica_set_local_primary() {
    let mut client = new_replicas_with_user_role(None, Some(SessionRole::Replica)).await;
    execute(&mut client, "BEGIN").await.unwrap();
    execute(&mut client, "SET LOCAL pgdog.role TO 'primary'")
        .await
        .unwrap();

    assert_primary(&mut client, "test_user_replica_set_local_primary").await;
    execute(&mut client, "ROLLBACK").await.unwrap();
}

#[tokio::test]
async fn test_user_primary_set_local_replica() {
    let mut client = new_replicas_with_user_role(None, Some(SessionRole::Primary)).await;
    execute(&mut client, "BEGIN").await.unwrap();
    execute(&mut client, "SET LOCAL pgdog.role TO 'replica'")
        .await
        .unwrap();

    assert_replica(&mut client, "test_user_primary_set_local_replica").await;
    execute(&mut client, "ROLLBACK").await.unwrap();
}

#[tokio::test]
async fn test_user_replica_comment_primary() {
    let mut client = new_replicas_with_user_role(None, Some(SessionRole::Replica)).await;

    for _ in 0..5 {
        assert!(!read_only_with_comment(&mut client, "/* pgdog_role: primary */").await);
    }
}

#[tokio::test]
async fn test_user_primary_comment_replica() {
    let mut client = new_replicas_with_user_role(None, Some(SessionRole::Primary)).await;

    for _ in 0..5 {
        assert!(read_only_with_comment(&mut client, "/* pgdog_role: replica */").await);
    }
}

#[tokio::test]
async fn test_user_replica_set_primary_reset() {
    let mut client = new_replicas_with_user_role(None, Some(SessionRole::Replica)).await;
    execute(&mut client, "SET pgdog.role TO 'primary'")
        .await
        .unwrap();
    execute(&mut client, "RESET pgdog.role").await.unwrap();

    assert_replica(&mut client, "test_user_replica_set_primary_reset").await;
}
