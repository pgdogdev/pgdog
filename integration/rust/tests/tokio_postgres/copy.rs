use bytes::{BufMut, BytesMut};
use futures_util::SinkExt;
use tokio_postgres::NoTls;

/// Demonstrate that COPY FROM STDIN via extended protocol (tokio-postgres)
/// works correctly through pgdog.
///
/// tokio-postgres sends COPY using Bind+Execute+Sync (extended protocol).
/// PostgreSQL ignores the Sync during COPY IN mode, producing only one
/// ReadyForQuery instead of two.  Without the remove_one_rfq() fix, the
/// stale ReadyForQuery expectation desyncs the state machine and the
/// connection becomes unusable for subsequent queries.
#[tokio::test]
async fn test_copy_in_extended_protocol() {
    let (conn, connection) = tokio_postgres::connect(
        "host=127.0.0.1 user=pgdog dbname=pgdog password=pgdog port=6432",
        NoTls,
    )
    .await
    .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Setup: clean slate
    conn.batch_execute(
        "DROP TABLE IF EXISTS _copy_test;
         CREATE TABLE _copy_test (id BIGINT, value TEXT);",
    )
    .await
    .unwrap();

    // COPY FROM STDIN — tokio-postgres sends this via extended protocol
    // (Parse, Bind, Execute, Sync), triggering the double-Sync pattern.
    let sink = conn
        .copy_in("COPY _copy_test (id, value) FROM STDIN")
        .await
        .unwrap();

    // Write some tab-delimited rows
    let mut buf = BytesMut::new();
    for i in 0..10_i64 {
        buf.put_slice(format!("{}\trow_{}\n", i, i).as_bytes());
    }
    futures_util::pin_mut!(sink);
    sink.send(buf.freeze()).await.unwrap();
    let rows_copied = sink.finish().await.unwrap();
    assert_eq!(rows_copied, 10);

    // This query AFTER the copy is the real test.
    // Without the fix, the state machine has a stale ReadyForQuery
    // and this query will either hang, error, or return wrong results.
    let rows = conn
        .query("SELECT count(*) FROM _copy_test", &[])
        .await
        .unwrap();
    let count: i64 = rows[0].get(0);
    assert_eq!(count, 10);

    // Cleanup
    conn.execute("DROP TABLE _copy_test", &[]).await.unwrap();
}
