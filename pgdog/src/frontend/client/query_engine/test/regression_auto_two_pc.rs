use super::prelude::*;
use crate::{
    backend::{Cluster, pool::Request},
    expect_message,
    net::{CommandComplete, DataRow, ErrorResponse, ReadyForQuery},
};

async fn two_pc_client(auto: bool) -> (TestClient, Cluster) {
    crate::config::load_test_sharded();
    super::change_config(|general| {
        general.two_phase_commit = true;
        general.two_phase_commit_auto = Some(auto);
    });
    let client = TestClient::new(Parameters::default()).await;
    let cluster = client.engine.backend.cluster().unwrap().clone();
    (client, cluster)
}

async fn execute_on_shard(cluster: &Cluster, shard: usize, query: String) -> String {
    let mut server = cluster.primary(shard, &Request::default()).await.unwrap();
    let messages = server.execute(query).await.unwrap();
    let complete = messages
        .iter()
        .find(|message| message.code() == 'C')
        .unwrap();
    expect_message!(complete.clone(), CommandComplete)
        .command()
        .to_owned()
}

async fn delete_on_shard(cluster: &Cluster, shard: usize, ids: &[i64]) -> String {
    let ids = ids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    execute_on_shard(
        cluster,
        shard,
        format!("DELETE FROM sharded WHERE id IN ({ids})"),
    )
    .await
}

async fn assert_no_auto_two_pc_leak(client: &mut TestClient, cluster: &Cluster) {
    let id = client.random_id_for_shard(0);

    for query in [
        "BEGIN".to_owned(),
        format!("INSERT INTO sharded (id) VALUES ({id})"),
        "ROLLBACK".to_owned(),
    ] {
        client.send_simple(Query::new(query)).await;
        client.read_until('Z').await.unwrap();
    }

    assert_eq!(
        delete_on_shard(cluster, 0, &[id]).await,
        "DELETE 0",
        "explicit transaction must not be committed by a stale automatic 2pc state"
    );
}

#[tokio::test]
async fn same_shard_batch_insert_with_automatic_two_pc() {
    let (mut client, cluster) = two_pc_client(true).await;
    let id0 = client.random_id_for_shard(0);
    let id1 = client.random_id_for_shard(0);

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id) VALUES ({id0}), ({id1})"
        )))
        .await;
    assert_eq!(
        expect_message!(client.read().await, CommandComplete).command(),
        "INSERT 0 2"
    );
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'I',
        "automatic commit must finish before returning ReadyForQuery"
    );

    assert_eq!(
        delete_on_shard(&cluster, 0, &[id0, id1]).await,
        "DELETE 2",
        "both inserted rows must be committed on shard 0"
    );
}

#[tokio::test]
async fn cross_shard_batch_insert_with_automatic_two_pc() {
    let (mut client, cluster) = two_pc_client(true).await;
    let id0 = client.random_id_for_shard(0);
    let id1 = client.random_id_for_shard(1);

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id) VALUES ({id0}), ({id1})"
        )))
        .await;
    assert_eq!(
        expect_message!(client.read().await, CommandComplete).command(),
        "INSERT 0 2"
    );
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'I'
    );

    assert_eq!(delete_on_shard(&cluster, 0, &[id0]).await, "DELETE 1");
    assert_eq!(delete_on_shard(&cluster, 1, &[id1]).await, "DELETE 1");

    assert_no_auto_two_pc_leak(&mut client, &cluster).await;
}

#[tokio::test]
async fn cross_shard_batch_upsert_returning_with_automatic_two_pc() {
    let (mut client, cluster) = two_pc_client(true).await;
    let id0 = client.random_id_for_shard(0);
    let id1 = client.random_id_for_shard(1);

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({id0}, 'a'), ({id1}, 'b') \
             ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value RETURNING id"
        )))
        .await;
    let messages = client.read_until('Z').await.unwrap();

    let mut returned = messages
        .iter()
        .filter(|message| message.code() == 'D')
        .map(|message| {
            expect_message!(message.clone(), DataRow)
                .get_int(0, true)
                .unwrap()
        })
        .collect::<Vec<_>>();
    returned.sort();
    let mut expected = vec![id0, id1];
    expected.sort();
    assert_eq!(returned, expected);

    let complete = messages
        .iter()
        .find(|message| message.code() == 'C')
        .unwrap();
    assert_eq!(
        expect_message!(complete.clone(), CommandComplete).command(),
        "INSERT 0 2"
    );
    assert_eq!(
        expect_message!(messages.last().unwrap().clone(), ReadyForQuery).status,
        'I'
    );

    assert_eq!(delete_on_shard(&cluster, 0, &[id0]).await, "DELETE 1");
    assert_eq!(delete_on_shard(&cluster, 1, &[id1]).await, "DELETE 1");

    assert_no_auto_two_pc_leak(&mut client, &cluster).await;
}

#[tokio::test]
async fn cross_shard_batch_insert_error_with_automatic_two_pc() {
    let (mut client, cluster) = two_pc_client(true).await;
    let id0 = client.random_id_for_shard(0);
    let id1 = client.random_id_for_shard(1);

    execute_on_shard(
        &cluster,
        1,
        format!("INSERT INTO sharded (id) VALUES ({id1})"),
    )
    .await;

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id) VALUES ({id0}), ({id1})"
        )))
        .await;
    let error = client.read_until('Z').await.unwrap_err();
    assert_eq!(error.code, "23505");
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'I'
    );

    assert_eq!(
        delete_on_shard(&cluster, 0, &[id0]).await,
        "DELETE 0",
        "failed cross-shard insert must not commit on any shard"
    );
    assert_eq!(delete_on_shard(&cluster, 1, &[id1]).await, "DELETE 1");

    assert_no_auto_two_pc_leak(&mut client, &cluster).await;
}

#[tokio::test]
async fn cross_shard_batch_insert_in_explicit_transaction_with_automatic_two_pc() {
    let (mut client, cluster) = two_pc_client(true).await;
    let id0 = client.random_id_for_shard(0);
    let id1 = client.random_id_for_shard(1);

    client.send_simple(Query::new("BEGIN")).await;
    let begin = client.read_until('Z').await.unwrap();
    assert_eq!(
        expect_message!(begin.last().unwrap().clone(), ReadyForQuery).status,
        'T'
    );

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id) VALUES ({id0}), ({id1})"
        )))
        .await;
    assert_eq!(
        expect_message!(client.read().await, CommandComplete).command(),
        "INSERT 0 2"
    );
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'T'
    );

    client.send_simple(Query::new("COMMIT")).await;
    let commit = client.read_until('Z').await.unwrap();
    assert_eq!(
        expect_message!(commit.last().unwrap().clone(), ReadyForQuery).status,
        'I'
    );

    assert_eq!(delete_on_shard(&cluster, 0, &[id0]).await, "DELETE 1");
    assert_eq!(delete_on_shard(&cluster, 1, &[id1]).await, "DELETE 1");

    assert_no_auto_two_pc_leak(&mut client, &cluster).await;
}

#[tokio::test]
async fn cross_shard_batch_insert_without_automatic_two_pc() {
    let (mut client, cluster) = two_pc_client(false).await;
    let id0 = client.random_id_for_shard(0);
    let id1 = client.random_id_for_shard(1);

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id) VALUES ({id0}), ({id1})"
        )))
        .await;
    assert_eq!(
        expect_message!(client.read().await, CommandComplete).command(),
        "INSERT 0 2"
    );
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'I'
    );

    assert_eq!(delete_on_shard(&cluster, 0, &[id0]).await, "DELETE 1");
    assert_eq!(delete_on_shard(&cluster, 1, &[id1]).await, "DELETE 1");

    assert_no_auto_two_pc_leak(&mut client, &cluster).await;
}

#[tokio::test]
async fn cross_shard_batch_insert_extended_with_automatic_two_pc() {
    let (mut client, cluster) = two_pc_client(true).await;
    let id0 = client.random_id_for_shard(0);
    let id1 = client.random_id_for_shard(1);
    let (param0, param1) = (id0.to_string(), id1.to_string());

    client
        .send(Parse::new_anonymous(
            "INSERT INTO sharded (id) VALUES ($1), ($2)",
        ))
        .await;
    client
        .send(Bind::new_params(
            "",
            &[
                Parameter::new(param0.as_bytes()),
                Parameter::new(param1.as_bytes()),
            ],
        ))
        .await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    let messages = client.read_until('Z').await.unwrap();
    let complete = messages
        .iter()
        .find(|message| message.code() == 'C')
        .unwrap();
    assert_eq!(
        expect_message!(complete.clone(), CommandComplete).command(),
        "INSERT 0 2"
    );
    assert_eq!(
        expect_message!(messages.last().unwrap().clone(), ReadyForQuery).status,
        'I'
    );

    assert_eq!(delete_on_shard(&cluster, 0, &[id0]).await, "DELETE 1");
    assert_eq!(delete_on_shard(&cluster, 1, &[id1]).await, "DELETE 1");

    assert_no_auto_two_pc_leak(&mut client, &cluster).await;
}

#[tokio::test]
async fn cross_shard_write_error_with_automatic_two_pc() {
    let (mut client, cluster) = two_pc_client(true).await;

    client
        .send_simple(Query::new(
            "DELETE FROM sharded WHERE value = (1 / 0)::text",
        ))
        .await;
    let error = client.read_until('Z').await.unwrap_err();
    assert_eq!(error.code, "22012");
    let rfq = loop {
        let message = client.read().await;
        match message.code() {
            'E' => assert_eq!(expect_message!(message, ErrorResponse).code, "22012"),
            _ => break expect_message!(message, ReadyForQuery),
        }
    };
    assert_eq!(rfq.status, 'I');

    assert_no_auto_two_pc_leak(&mut client, &cluster).await;
}

#[tokio::test]
async fn cross_shard_batch_insert_explicit_rollback_with_automatic_two_pc() {
    let (mut client, cluster) = two_pc_client(true).await;
    let id0 = client.random_id_for_shard(0);
    let id1 = client.random_id_for_shard(1);

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id) VALUES ({id0}), ({id1})"
        )))
        .await;
    assert_eq!(
        expect_message!(client.read().await, CommandComplete).command(),
        "INSERT 0 2"
    );
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'T'
    );

    client.send_simple(Query::new("ROLLBACK")).await;
    let rollback = client.read_until('Z').await.unwrap();
    assert_eq!(
        expect_message!(rollback.last().unwrap().clone(), ReadyForQuery).status,
        'I'
    );

    assert_eq!(delete_on_shard(&cluster, 0, &[id0]).await, "DELETE 0");
    assert_eq!(delete_on_shard(&cluster, 1, &[id1]).await, "DELETE 0");

    assert_no_auto_two_pc_leak(&mut client, &cluster).await;
}

#[tokio::test]
async fn cross_shard_batch_insert_error_in_explicit_transaction_with_automatic_two_pc() {
    let (mut client, cluster) = two_pc_client(true).await;
    let id0 = client.random_id_for_shard(0);
    let id1 = client.random_id_for_shard(1);

    execute_on_shard(
        &cluster,
        1,
        format!("INSERT INTO sharded (id) VALUES ({id1})"),
    )
    .await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id) VALUES ({id0}), ({id1})"
        )))
        .await;
    let error = client.read_until('Z').await.unwrap_err();
    assert_eq!(error.code, "23505");
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'E'
    );

    client.send_simple(Query::new("ROLLBACK")).await;
    let rollback = client.read_until('Z').await.unwrap();
    assert_eq!(
        expect_message!(rollback.last().unwrap().clone(), ReadyForQuery).status,
        'I'
    );

    assert_eq!(delete_on_shard(&cluster, 0, &[id0]).await, "DELETE 0");
    assert_eq!(delete_on_shard(&cluster, 1, &[id1]).await, "DELETE 1");

    assert_no_auto_two_pc_leak(&mut client, &cluster).await;
}
