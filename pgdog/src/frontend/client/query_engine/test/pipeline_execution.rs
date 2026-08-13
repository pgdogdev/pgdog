use crate::expect_message;
use crate::net::{
    BindComplete, CommandComplete, DataRow, DataType, Message, ParameterDescription, Parameters,
    ParseComplete, Protocol, ReadyForQuery, RowDescription,
};

use super::prelude::*;

/// Assert the next reply message
macro_rules! assert_message {
    ($messages:expr, $ty:ty) => {{
        let message = $messages
            .next()
            .unwrap_or_else(|| panic!("expected {}, got no more replies", stringify!($ty)));

        expect_message!(message, $ty)
    }};
}

/// Send the request with message and wait for the whole stream of response messages
async fn send_and_wait(
    client: &mut TestClient,
    request: impl IntoIterator<Item = ProtocolMessage>,
) -> Vec<Message> {
    for message in request {
        client.send(message).await;
    }
    client.try_process().await.unwrap();

    client.read_until('Z').await.unwrap()
}

/// Same as [`send_and_wait`] but won't fail on the first error
/// and will collect all the responses
async fn send_fail_and_wait(
    client: &mut TestClient,
    request: impl IntoIterator<Item = ProtocolMessage>,
) -> Vec<Message> {
    for message in request {
        client.send(message).await;
    }
    let _ = client.try_process().await;

    let mut replies = vec![];
    loop {
        let message = client.read().await;
        let last = message.code() == 'Z';
        replies.push(message);

        if last {
            return replies;
        }
    }
}

/// The id in the next row.
fn next_id(messages: &mut impl Iterator<Item = Message>, text: bool) -> i64 {
    let row = assert_message!(messages, DataRow);
    assert_eq!(row.len(), 1);

    row.get_int(0, text).unwrap()
}

/// Parameter types a ParameterDescription must carry, in order.
fn assert_params(description: ParameterDescription, types: &[DataType]) {
    let params = description
        .params()
        .iter()
        .map(|oid| DataType::from_oid(*oid))
        .collect::<Vec<_>>();

    assert_eq!(params, types);
}

/// Column names a RowDescription must carry, in order.
fn assert_columns(description: RowDescription, names: &[&str]) {
    let columns = description
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(columns, names);
}

/// Two ids, one per shard, so a merged sort has to interleave them. The one on
/// shard 1 is the smaller number and the longer string, so numeric and text
/// order disagree.
fn ids_across_shards(client: &mut TestClient) -> (i64, i64) {
    loop {
        let shard_0 = client.random_id_for_shard(0);
        let shard_1 = client.random_id_for_shard(1);

        if shard_1 < shard_0 && shard_1.to_string() > shard_0.to_string() {
            return (shard_0, shard_1);
        }
    }
}

/// One row per shard, under random ids, so a test that dies before it cleans
/// up cannot collide with another test.
async fn seed(client: &mut TestClient, (shard_0, shard_1): (i64, i64)) {
    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id) VALUES ({shard_0}), ({shard_1})"
        )))
        .await;
    client.read_until('Z').await.unwrap();
}

async fn cleanup(client: &mut TestClient, (shard_0, shard_1): (i64, i64)) {
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({shard_0}, {shard_1})"
        )))
        .await;
    client.read_until('Z').await.unwrap();
}

fn test_sql_sort(ids: (i64, i64)) -> String {
    format!(
        "SELECT id FROM sharded WHERE id IN ({}, {}) ORDER BY id",
        ids.0, ids.1
    )
}

fn test_sql_sort_desc(ids: (i64, i64)) -> String {
    format!("{} DESC", test_sql_sort(ids))
}

/// Two Parse and Describe pairs, then Sync. Nothing executes, so the router
/// sends the request to a single shard.
#[tokio::test]
async fn test_pipelined_parse_describe_sync_only() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    let mut messages = send_and_wait(
        &mut client,
        vec![
            Parse::named("s1", "SELECT id FROM sharded WHERE 1 = 0").into(),
            Describe::new_statement("s1").into(),
            Parse::named(
                "s2",
                "SELECT id, $1::text AS second FROM sharded WHERE 1 = 0",
            )
            .into(),
            Describe::new_statement("s2").into(),
            Sync.into(),
        ],
    )
    .await
    .into_iter();

    assert_message!(messages, ParseComplete);
    assert_params(assert_message!(messages, ParameterDescription), &[]);
    assert_columns(assert_message!(messages, RowDescription), &["id"]);

    assert_message!(messages, ParseComplete);
    assert_params(
        assert_message!(messages, ParameterDescription),
        &[DataType::Text],
    );
    assert_columns(assert_message!(messages, RowDescription), &["id", "second"]);

    assert_eq!(assert_message!(messages, ReadyForQuery).status, 'I');
    assert!(messages.next().is_none());
}

/// The same shape, plus Bind and Execute. The request is executable, so it
/// stays on the cross-shard route of the first statement.
#[tokio::test]
async fn test_pipelined_parse_describe() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    let mut messages = send_and_wait(
        &mut client,
        vec![
            Parse::named("c1", "SELECT id FROM sharded WHERE 1 = 0").into(),
            Describe::new_statement("c1").into(),
            Parse::named(
                "c2",
                "SELECT id, $1::text AS second FROM sharded WHERE 1 = 0",
            )
            .into(),
            Describe::new_statement("c2").into(),
            Bind::new_statement("c1").into(),
            Execute::new().into(),
            Sync.into(),
        ],
    )
    .await
    .into_iter();

    assert_message!(messages, ParseComplete);
    assert_params(assert_message!(messages, ParameterDescription), &[]);
    assert_columns(assert_message!(messages, RowDescription), &["id"]);

    assert_message!(messages, ParseComplete);
    assert_params(
        assert_message!(messages, ParameterDescription),
        &[DataType::Text],
    );
    assert_columns(assert_message!(messages, RowDescription), &["id", "second"]);

    assert_message!(messages, BindComplete);
    assert_eq!(
        assert_message!(messages, CommandComplete).command(),
        "SELECT 0"
    );
    assert_eq!(assert_message!(messages, ReadyForQuery).status, 'I');
    assert!(messages.next().is_none());
}

/// Three pairs in a row, each describing different parameters and columns.
#[tokio::test]
async fn test_pipelined_parse_describe_three_pairs() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    let mut messages = send_and_wait(
        &mut client,
        vec![
            Parse::named("c1", "SELECT id FROM sharded WHERE 1 = 0").into(),
            Describe::new_statement("c1").into(),
            Parse::named(
                "c2",
                "SELECT id, $1::text AS second FROM sharded WHERE 1 = 0",
            )
            .into(),
            Describe::new_statement("c2").into(),
            Parse::named(
                "c3",
                "SELECT id, $1::text AS second, $2::bigint AS third FROM sharded WHERE 1 = 0",
            )
            .into(),
            Describe::new_statement("c3").into(),
            Bind::new_statement("c1").into(),
            Execute::new().into(),
            Sync.into(),
        ],
    )
    .await
    .into_iter();

    assert_message!(messages, ParseComplete);
    assert_params(assert_message!(messages, ParameterDescription), &[]);
    assert_columns(assert_message!(messages, RowDescription), &["id"]);

    assert_message!(messages, ParseComplete);
    assert_params(
        assert_message!(messages, ParameterDescription),
        &[DataType::Text],
    );
    assert_columns(assert_message!(messages, RowDescription), &["id", "second"]);

    assert_message!(messages, ParseComplete);
    assert_params(
        assert_message!(messages, ParameterDescription),
        &[DataType::Text, DataType::Bigint],
    );
    assert_columns(
        assert_message!(messages, RowDescription),
        &["id", "second", "third"],
    );

    assert_message!(messages, BindComplete);
    assert_eq!(
        assert_message!(messages, CommandComplete).command(),
        "SELECT 0"
    );
    assert_eq!(assert_message!(messages, ReadyForQuery).status, 'I');
    assert!(messages.next().is_none());
}

/// A sorted cross-shard query buffers its rows, so pgdog decodes them. The
/// other Describe in the exchange names no sort column, and must not take the
/// decoder over.
#[tokio::test]
async fn test_pipelined_parse_describe_sorted() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    let ids = ids_across_shards(&mut client);
    seed(&mut client, ids).await;

    let replies = send_and_wait(
        &mut client,
        [
            ProtocolMessage::from(Parse::named("o1", test_sql_sort(ids))),
            Describe::new_statement("o1").into(),
            Parse::named("o2", "SELECT $1::text AS second FROM sharded").into(),
            Describe::new_statement("o2").into(),
            Bind::new_statement("o1").into(),
            Execute::new().into(),
            Sync.into(),
        ],
    )
    .await;

    cleanup(&mut client, ids).await;

    let mut messages = replies.into_iter();

    assert_message!(messages, ParseComplete);
    assert_params(assert_message!(messages, ParameterDescription), &[]);
    assert_columns(assert_message!(messages, RowDescription), &["id"]);

    assert_message!(messages, ParseComplete);
    assert_params(
        assert_message!(messages, ParameterDescription),
        &[DataType::Text],
    );
    assert_columns(assert_message!(messages, RowDescription), &["second"]);

    assert_message!(messages, BindComplete);
    assert_eq!(next_id(&mut messages, true), ids.1);
    assert_eq!(next_id(&mut messages, true), ids.0);
    assert_eq!(
        assert_message!(messages, CommandComplete).command(),
        "SELECT 2"
    );
    assert_eq!(assert_message!(messages, ReadyForQuery).status, 'I');
    assert!(messages.next().is_none());
}

/// The other Describe names the sort column too, with another type. The rows
/// belong to the statement that executes, so they sort as bigint.
#[tokio::test]
async fn test_pipelined_parse_describe_shadowed_column_type() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    let ids = ids_across_shards(&mut client);
    seed(&mut client, ids).await;

    let replies = send_and_wait(
        &mut client,
        [
            ProtocolMessage::from(Parse::named("t1", test_sql_sort(ids))),
            Describe::new_statement("t1").into(),
            Parse::named("t2", "SELECT 'shadow'::text AS id FROM sharded").into(),
            Describe::new_statement("t2").into(),
            Bind::new_statement("t1").into(),
            Execute::new().into(),
            Sync.into(),
        ],
    )
    .await;

    cleanup(&mut client, ids).await;

    let mut messages = replies.into_iter();

    assert_message!(messages, ParseComplete);
    assert_params(assert_message!(messages, ParameterDescription), &[]);
    assert_columns(assert_message!(messages, RowDescription), &["id"]);

    assert_message!(messages, ParseComplete);
    assert_params(assert_message!(messages, ParameterDescription), &[]);
    assert_columns(assert_message!(messages, RowDescription), &["id"]);

    assert_message!(messages, BindComplete);
    assert_eq!(next_id(&mut messages, true), ids.1);
    assert_eq!(next_id(&mut messages, true), ids.0);
    assert_eq!(
        assert_message!(messages, CommandComplete).command(),
        "SELECT 2"
    );
    assert_eq!(assert_message!(messages, ReadyForQuery).status, 'I');
    assert!(messages.next().is_none());
}

/// Two prepared statements execute in one exchange, as pgx does after its
/// prepare phase. Each sorted result set must reach the client whole, on its
/// own side of the CommandComplete that ends it.
#[tokio::test]
async fn test_pipelined_two_executes_keep_every_row() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    let ids = ids_across_shards(&mut client);
    seed(&mut client, ids).await;

    send_and_wait(
        &mut client,
        [
            ProtocolMessage::from(Parse::named("b1", test_sql_sort(ids))),
            Describe::new_statement("b1").into(),
            Parse::named("b2", test_sql_sort_desc(ids)).into(),
            Describe::new_statement("b2").into(),
            Sync.into(),
        ],
    )
    .await;

    // Both portals run before the client reads anything.
    let replies = send_and_wait(
        &mut client,
        [
            ProtocolMessage::from(Bind::new_statement("b1")),
            Execute::new().into(),
            Bind::new_statement("b2").into(),
            Execute::new().into(),
            Sync.into(),
        ],
    )
    .await;

    cleanup(&mut client, ids).await;

    let mut messages = replies.into_iter();

    assert_message!(messages, BindComplete);
    assert_eq!(next_id(&mut messages, true), ids.1);
    assert_eq!(next_id(&mut messages, true), ids.0);
    assert_eq!(
        assert_message!(messages, CommandComplete).command(),
        "SELECT 2"
    );

    assert_message!(messages, BindComplete);
    assert_eq!(next_id(&mut messages, true), ids.0);
    assert_eq!(next_id(&mut messages, true), ids.1);
    assert_eq!(
        assert_message!(messages, CommandComplete).command(),
        "SELECT 2"
    );

    assert_eq!(assert_message!(messages, ReadyForQuery).status, 'I');
    assert!(messages.next().is_none());
}

/// The client asks for binary results, as tokio-postgres does. The Bind is the
/// only thing telling pgdog these buffered rows are binary.
#[tokio::test]
async fn test_pipelined_binary_results_sort_as_binary() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    let ids = ids_across_shards(&mut client);
    seed(&mut client, ids).await;

    // Naming the statement caches its RowDescription, so the execute phase
    // sends no Parse and no Describe.
    send_and_wait(
        &mut client,
        [
            ProtocolMessage::from(Parse::named("y1", test_sql_sort(ids))),
            Describe::new_statement("y1").into(),
            Sync.into(),
        ],
    )
    .await;

    let replies = send_and_wait(
        &mut client,
        [
            ProtocolMessage::from(Bind::new_params_codes_results("y1", &[], &[], &[1])),
            Execute::new().into(),
            Sync.into(),
        ],
    )
    .await;

    cleanup(&mut client, ids).await;

    let mut messages = replies.into_iter();

    assert_message!(messages, BindComplete);
    assert_eq!(next_id(&mut messages, false), ids.1);
    assert_eq!(next_id(&mut messages, false), ids.0);
    assert_eq!(
        assert_message!(messages, CommandComplete).command(),
        "SELECT 2"
    );
    assert_eq!(assert_message!(messages, ReadyForQuery).status, 'I');
    assert!(messages.next().is_none());
}

/// An anonymous Bind has no cached RowDescription, so the Describe of the same
/// statement is the only thing that can type its rows. This is the shape pgdog
/// sends when it rewrites a sharding key update.
#[tokio::test]
async fn test_pipelined_anonymous_bind_sorts() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    let ids = ids_across_shards(&mut client);
    seed(&mut client, ids).await;

    let replies = send_and_wait(
        &mut client,
        [
            ProtocolMessage::from(Parse::new_anonymous(&test_sql_sort(ids))),
            Describe::new_statement("").into(),
            Bind::new_statement("").into(),
            Execute::new().into(),
            Sync.into(),
        ],
    )
    .await;

    cleanup(&mut client, ids).await;

    let mut messages = replies.into_iter();

    assert_message!(messages, ParseComplete);
    assert_params(assert_message!(messages, ParameterDescription), &[]);
    assert_columns(assert_message!(messages, RowDescription), &["id"]);

    assert_message!(messages, BindComplete);
    assert_eq!(next_id(&mut messages, true), ids.1);
    assert_eq!(next_id(&mut messages, true), ids.0);
    assert_eq!(
        assert_message!(messages, CommandComplete).command(),
        "SELECT 2"
    );
    assert_eq!(assert_message!(messages, ReadyForQuery).status, 'I');
    assert!(messages.next().is_none());
}

/// An Execute that fails at run time abandons the rest of the exchange, and
/// the session still sorts the next one.
#[tokio::test]
async fn test_pipelined_error_does_not_leak_into_the_next_exchange() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    let ids = ids_across_shards(&mut client);
    seed(&mut client, ids).await;

    let failing = format!(
        "SELECT id / 0 FROM sharded WHERE id IN ({}, {})",
        ids.0, ids.1
    );

    let replies = send_fail_and_wait(
        &mut client,
        [
            ProtocolMessage::from(Parse::named("f1", failing)),
            Parse::named("f2", test_sql_sort(ids)).into(),
            Describe::new_statement("f2").into(),
            Bind::new_statement("f1").into(),
            Execute::new().into(),
            Bind::new_statement("f2").into(),
            Execute::new().into(),
            Sync.into(),
        ],
    )
    .await;

    // Every shard reports the error, so the count is not pinned here.
    assert!(replies.iter().any(|message| message.code() == 'E'));
    assert_eq!(replies.last().unwrap().code(), 'Z');
    assert!(!replies.iter().any(|message| message.code() == 'D'));

    let replies = send_and_wait(
        &mut client,
        [
            ProtocolMessage::from(Bind::new_statement("f2")),
            Execute::new().into(),
            Sync.into(),
        ],
    )
    .await;

    cleanup(&mut client, ids).await;

    let mut messages = replies.into_iter();

    assert_message!(messages, BindComplete);
    assert_eq!(next_id(&mut messages, true), ids.1);
    assert_eq!(next_id(&mut messages, true), ids.0);
    assert_eq!(
        assert_message!(messages, CommandComplete).command(),
        "SELECT 2"
    );
    assert_eq!(assert_message!(messages, ReadyForQuery).status, 'I');
    assert!(messages.next().is_none());
}
