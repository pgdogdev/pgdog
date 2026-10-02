use super::prelude::*;
use crate::{
    backend::pool::Request,
    expect_message,
    net::{CommandComplete, ReadyForQuery},
};

#[tokio::test]
async fn same_shard_batch_insert_with_automatic_two_pc() {
    crate::config::load_test_sharded();
    super::change_config(|general| {
        general.two_phase_commit = true;
        general.two_phase_commit_auto = Some(true);
    });
    let mut client = TestClient::new(Parameters::default()).await;
    let cluster = client.engine.backend.cluster().unwrap().clone();
    let id0 = client.random_id_for_shard(0);
    let id1 = client.random_id_for_shard(0);

    // Both tuples must use one shard, while still exercising INSERT splitting.
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

    // Delete directly from shard 0 to verify both rows were committed there.
    let mut server = cluster.primary(0, &Request::default()).await.unwrap();
    let cleanup = server
        .execute(format!("DELETE FROM sharded WHERE id IN ({id0}, {id1})"))
        .await
        .unwrap();

    let deleted = cleanup
        .iter()
        .find(|message| message.code() == 'C')
        .unwrap();
    assert_eq!(
        expect_message!(deleted.clone(), CommandComplete).command(),
        "DELETE 2",
        "both inserted rows must be committed on shard 0"
    );
}
