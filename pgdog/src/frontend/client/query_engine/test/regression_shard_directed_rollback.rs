use super::prelude::*;
use crate::{expect_message, net::ReadyForQuery};

#[tokio::test]
#[ignore] // You must be crazy to do this and you deserve what happens to you.
async fn shard_directive_does_not_limit_rollback_participants() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    for sql in [
        "BEGIN",
        "/* pgdog_shard: 0 */ SELECT 1",
        "/* pgdog_shard: 1 */ SELECT 1",
    ] {
        client.send_simple(Query::new(sql)).await;
        client.read_until('Z').await.unwrap();
    }

    client
        .send_simple(Query::new("/* pgdog_shard: 0 */ ROLLBACK"))
        .await;
    let rollback = client.read_until('Z').await.unwrap();
    assert_eq!(
        expect_message!(rollback.last().unwrap().clone(), ReadyForQuery).status,
        'I'
    );

    // The other participant must also be idle after the acknowledged rollback.
    client
        .send_simple(Query::new("/* pgdog_shard: 1 */ SELECT 1"))
        .await;
    let messages = client.read_until('Z').await.unwrap();
    let status = expect_message!(messages.last().unwrap().clone(), ReadyForQuery).status;

    // Close the transaction left on shard 1 by the regression before asserting.
    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();
    assert_eq!(
        status, 'I',
        "ROLLBACK must end every participant's transaction"
    );
}
