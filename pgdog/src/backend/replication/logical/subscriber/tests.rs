use bytes::Bytes;
use pgdog_config::QueryParserEngine;
use pgdog_postgres_types::Oid;
use rand::Rng;

use crate::{
    backend::{
        pool::cluster::Cluster,
        replication::logical::publisher::{
            Lsn, PublicationTable, PublicationTableColumn, ReplicaIdentity, Table,
        },
        server::test::test_server,
        Server,
    },
    config::config,
    net::{
        replication::{
            logical::{
                begin::Begin,
                commit::Commit,
                delete::Delete as XLogDelete,
                insert::Insert as XLogInsert,
                relation::{Column as RelColumn, Relation},
                tuple_data::{Column as TupleColumn, Identifier, TupleData},
                update::Update as XLogUpdate,
            },
            XLogData,
        },
        CopyData, ToBytes,
    },
};

use super::stream::StreamSubscriber;

fn random_id() -> String {
    rand::rng()
        .random_range(1_000_000_000..i64::MAX)
        .to_string()
}

fn xlog_copy_data(payload: Bytes) -> CopyData {
    let xlog = XLogData {
        starting_point: 0,
        current_end: 0,
        system_clock: 0,
        bytes: payload,
    };
    CopyData::bytes(xlog.to_bytes().unwrap())
}

fn make_sharded_table() -> Table {
    Table {
        publication: "test".to_string(),
        table: PublicationTable {
            schema: "public".to_string(),
            name: "sharded".to_string(),
            attributes: "".to_string(),
            parent_schema: "".to_string(),
            parent_name: "".to_string(),
        },
        identity: ReplicaIdentity {
            oid: Oid(1),
            identity: "".to_string(),
            kind: "".to_string(),
        },
        columns: vec![
            PublicationTableColumn {
                oid: 1,
                name: "id".to_string(),
                type_oid: Oid(20), // bigint
                identity: true,
            },
            PublicationTableColumn {
                oid: 1,
                name: "value".to_string(),
                type_oid: Oid(25), // text
                identity: false,
            },
        ],
        lsn: Lsn::default(),
        query_parser_engine: QueryParserEngine::default(),
    }
}

fn make_sharded_test_b_table() -> Table {
    Table {
        publication: "test".to_string(),
        table: PublicationTable {
            schema: "public".to_string(),
            name: "sharded_test_b".to_string(),
            attributes: "".to_string(),
            parent_schema: "".to_string(),
            parent_name: "".to_string(),
        },
        identity: ReplicaIdentity {
            oid: Oid(2),
            identity: "".to_string(),
            kind: "".to_string(),
        },
        columns: vec![
            PublicationTableColumn {
                oid: 2,
                name: "id".to_string(),
                type_oid: Oid(20),
                identity: true,
            },
            PublicationTableColumn {
                oid: 2,
                name: "value".to_string(),
                type_oid: Oid(25),
                identity: false,
            },
        ],
        lsn: Lsn::default(),
        query_parser_engine: QueryParserEngine::default(),
    }
}

fn sharded_relation(oid: Oid) -> Relation {
    Relation {
        oid,
        namespace: "public".to_string(),
        name: "sharded".to_string(),
        replica_identity: 100,
        columns: vec![
            RelColumn {
                flag: 1,
                name: "id".to_string(),
                oid: Oid(20),
                type_modifier: -1,
            },
            RelColumn {
                flag: 0,
                name: "value".to_string(),
                oid: Oid(25),
                type_modifier: -1,
            },
        ],
    }
}

fn sharded_test_b_relation(oid: Oid) -> Relation {
    Relation {
        oid,
        namespace: "public".to_string(),
        name: "sharded_test_b".to_string(),
        replica_identity: 100,
        columns: vec![
            RelColumn {
                flag: 1,
                name: "id".to_string(),
                oid: Oid(20),
                type_modifier: -1,
            },
            RelColumn {
                flag: 0,
                name: "value".to_string(),
                oid: Oid(25),
                type_modifier: -1,
            },
        ],
    }
}

fn text_column(data: &str) -> TupleColumn {
    TupleColumn {
        identifier: Identifier::Format(crate::net::bind::Format::Text),
        len: data.len() as i32,
        data: Bytes::copy_from_slice(data.as_bytes()),
    }
}

fn begin_copy_data(lsn: i64) -> CopyData {
    xlog_copy_data(
        Begin {
            final_transaction_lsn: lsn,
            commit_timestamp: 0,
            xid: 1,
        }
        .to_bytes()
        .unwrap(),
    )
}

fn commit_copy_data(end_lsn: i64) -> CopyData {
    xlog_copy_data(
        Commit {
            flags: 0,
            commit_lsn: 0,
            end_lsn,
            commit_timestamp: 0,
        }
        .to_bytes()
        .unwrap(),
    )
}

fn relation_copy_data(oid: Oid) -> CopyData {
    xlog_copy_data(sharded_relation(oid).to_bytes().unwrap())
}

fn sharded_test_b_relation_copy_data(oid: Oid) -> CopyData {
    xlog_copy_data(sharded_test_b_relation(oid).to_bytes().unwrap())
}

fn insert_copy_data(oid: Oid, id: &str, value: &str) -> CopyData {
    xlog_copy_data(
        XLogInsert {
            xid: None,
            oid,
            tuple_data: TupleData {
                columns: vec![text_column(id), text_column(value)],
            },
        }
        .to_bytes()
        .unwrap(),
    )
}

fn delete_copy_data(oid: Oid, id: &str) -> CopyData {
    xlog_copy_data(
        XLogDelete {
            oid,
            key: Some(TupleData {
                columns: vec![text_column(id)],
            }),
            old: None,
        }
        .to_bytes()
        .unwrap(),
    )
}

fn toasted_column() -> TupleColumn {
    TupleColumn {
        identifier: Identifier::Toasted,
        len: 0,
        data: Bytes::new(),
    }
}

fn x_update(u: XLogUpdate) -> CopyData {
    xlog_copy_data(u.to_bytes().unwrap())
}

fn make_subscriber() -> StreamSubscriber {
    let cluster = Cluster::new_test(&config());
    let tables = vec![make_sharded_table(), make_sharded_test_b_table()];
    StreamSubscriber::new(&cluster, &tables, QueryParserEngine::default())
}

fn make_subscriber_with_tables(tables: Vec<Table>) -> StreamSubscriber {
    let cluster = Cluster::new_test(&config());
    StreamSubscriber::new(&cluster, &tables, QueryParserEngine::default())
}

fn make_subscriber_single_shard() -> StreamSubscriber {
    let cluster = Cluster::new_test_single_shard(&config());
    let tables = vec![make_sharded_table(), make_sharded_test_b_table()];
    StreamSubscriber::new(&cluster, &tables, QueryParserEngine::default())
}

/// Count rows matching the given id using a separate connection.
async fn count_row(server: &mut Server, table: &str, id: &str) -> i64 {
    let query = format!("SELECT COUNT(*) FROM {} WHERE id = {}", table, id);
    let rows: Vec<crate::net::DataRow> = server.fetch_all(query).await.unwrap();
    rows.first()
        .and_then(|row: &crate::net::DataRow| row.column(0))
        .map(|col| {
            std::str::from_utf8(&col[..])
                .unwrap()
                .parse::<i64>()
                .unwrap()
        })
        .unwrap_or(0)
}

async fn ensure_table(server: &mut Server, table: &str) {
    match table {
        "public.sharded" => {
            server
                .execute(
                    "CREATE TABLE IF NOT EXISTS public.sharded (\
                     id BIGINT PRIMARY KEY, value TEXT)",
                )
                .await
                .unwrap();
        }
        "public.sharded_test_b" => {
            server
                .execute(
                    "CREATE TABLE IF NOT EXISTS public.sharded_test_b (\
                     id BIGINT PRIMARY KEY, value TEXT)",
                )
                .await
                .unwrap();
        }
        "public.posts" => {
            server
                .execute(
                    "CREATE TABLE IF NOT EXISTS public.posts (\
                     id BIGINT PRIMARY KEY, title TEXT, body TEXT)",
                )
                .await
                .unwrap();
        }
        _ => (),
    }
}

/// Delete rows by id, cleaning up test data.
async fn cleanup(server: &mut Server, table: &str, ids: &[&str]) {
    ensure_table(server, table).await;

    for id in ids {
        server
            .execute(format!("DELETE FROM {} WHERE id = {}", table, id))
            .await
            .unwrap();
    }
}

// ── State machine tests ─────────────────────────────────────────────

/// Begin message sets in_transaction and records the LSN.
#[tokio::test]
async fn begin_sets_transaction_state() {
    let mut sub = make_subscriber();
    assert!(!sub.in_transaction());
    assert_eq!(sub.lsn(), 0);

    sub.connect().await.unwrap();
    sub.handle(begin_copy_data(100)).await.unwrap();

    assert!(sub.in_transaction());
    assert_eq!(sub.lsn(), 100);
    assert!(sub.lsn_changed());
}

/// Commit clears in_transaction, advances LSN, and returns a StatusUpdate.
#[tokio::test]
async fn commit_returns_status_and_clears_transaction() {
    let mut sub = make_subscriber();
    sub.connect().await.unwrap();

    sub.handle(begin_copy_data(100)).await.unwrap();
    assert!(sub.in_transaction());

    let result = sub.handle(commit_copy_data(200)).await.unwrap();
    assert!(!sub.in_transaction());
    assert_eq!(sub.lsn(), 200);

    let status = result.expect("commit should return a StatusUpdate");
    assert_eq!(status.last_applied, 200);
    assert_eq!(status.last_flushed, 200);
    assert_eq!(status.last_written, 200);
}

/// handle() returns None for non-commit messages.
#[tokio::test]
async fn begin_returns_no_status_update() {
    let mut sub = make_subscriber();
    sub.connect().await.unwrap();

    let result = sub.handle(begin_copy_data(100)).await.unwrap();
    assert!(result.is_none());
}

/// bytes_sharded accumulates across messages.
#[tokio::test]
async fn bytes_sharded_accumulates() {
    let mut sub = make_subscriber();
    sub.connect().await.unwrap();

    assert_eq!(sub.bytes_sharded(), 0);

    sub.handle(begin_copy_data(100)).await.unwrap();
    assert!(sub.bytes_sharded() > 0);

    let after_begin = sub.bytes_sharded();
    sub.handle(commit_copy_data(200)).await.unwrap();
    assert!(sub.bytes_sharded() > after_begin);
}

/// set_current_lsn returns true only when the LSN changes.
#[test]
fn lsn_changed_tracking() {
    let mut sub = make_subscriber();

    assert!(sub.set_current_lsn(100));
    assert!(sub.lsn_changed());

    assert!(!sub.set_current_lsn(100));
    assert!(!sub.lsn_changed());

    assert!(sub.set_current_lsn(200));
    assert!(sub.lsn_changed());
}

// ── Relation handling tests ─────────────────────────────────────────

/// Relation inside a transaction uses Flush — stays in transaction.
#[tokio::test]
async fn relation_inside_transaction() {
    let mut sub = make_subscriber();
    sub.connect().await.unwrap();

    sub.handle(begin_copy_data(100)).await.unwrap();
    assert!(sub.in_transaction());

    sub.handle(relation_copy_data(Oid(16384))).await.unwrap();
    assert!(sub.in_transaction());
}

/// Relation outside a transaction uses Sync.
#[tokio::test]
async fn relation_outside_transaction() {
    let mut sub = make_subscriber();
    sub.connect().await.unwrap();

    assert!(!sub.in_transaction());
    sub.handle(relation_copy_data(Oid(16384))).await.unwrap();
    assert!(!sub.in_transaction());
}

/// A second Relation for a *different* table arrives mid-transaction after rows
/// have already been inserted into the first table. The relation handler must
/// use Flush (not Sync) so the in-progress transaction is not broken, and
/// subsequent inserts to both tables must succeed within the same commit.
#[tokio::test]
async fn relation_after_insert_inside_transaction() {
    let mut sub = make_subscriber_single_shard();
    let mut verify = test_server().await;

    // Ensure the second table exists (CI only creates sharded/sharded_omni).
    verify
        .execute(
            "CREATE TABLE IF NOT EXISTS public.sharded_test_b (\
             id BIGINT PRIMARY KEY, value TEXT)",
        )
        .await
        .unwrap();

    sub.connect().await.unwrap();

    let oid_a = Oid(16384);
    let oid_b = Oid(16385);
    let id_a = random_id();
    let id_b = random_id();

    cleanup(&mut verify, "public.sharded", &[&id_a]).await;
    cleanup(&mut verify, "public.sharded_test_b", &[&id_b]).await;

    // Begin
    sub.handle(begin_copy_data(100)).await.unwrap();

    // First table: prepare + insert.
    sub.handle(relation_copy_data(oid_a)).await.unwrap();
    sub.handle(insert_copy_data(oid_a, &id_a, "table_a"))
        .await
        .unwrap();

    // Second table's Relation arrives mid-transaction — this prepares new
    // statements using Flush (not Sync), keeping the transaction open.
    sub.handle(sharded_test_b_relation_copy_data(oid_b))
        .await
        .unwrap();
    sub.handle(insert_copy_data(oid_b, &id_b, "table_b"))
        .await
        .unwrap();

    assert!(sub.in_transaction());

    // Commit both tables atomically.
    let status = sub.handle(commit_copy_data(200)).await.unwrap();
    assert!(!sub.in_transaction());
    assert!(status.is_some());
    assert_eq!(sub.lsn(), 200);

    // Both rows persisted.
    assert_eq!(count_row(&mut verify, "public.sharded", &id_a).await, 1);
    assert_eq!(
        count_row(&mut verify, "public.sharded_test_b", &id_b).await,
        1
    );

    cleanup(&mut verify, "public.sharded", &[&id_a]).await;
    cleanup(&mut verify, "public.sharded_test_b", &[&id_b]).await;
}

/// Two source tables (e.g. partition leaves) that map to the same destination
/// must each register their oid so DML for *both* oids is applied. Regression
/// test for the partition-dedup row-drop bug: previously the second leaf's
/// Relation message returned early without registering its oid in `statements`,
/// causing all subsequent inserts on that oid to be silently dropped.
#[tokio::test]
async fn partition_leaves_share_destination() {
    let mut leaf_a = make_sharded_table();
    leaf_a.table.name = "sharded_p1".to_string();
    leaf_a.table.parent_schema = "public".to_string();
    leaf_a.table.parent_name = "sharded".to_string();

    let mut leaf_b = make_sharded_table();
    leaf_b.table.name = "sharded_p2".to_string();
    leaf_b.table.parent_schema = "public".to_string();
    leaf_b.table.parent_name = "sharded".to_string();

    let cluster = Cluster::new_test_single_shard(&config());
    let mut sub = StreamSubscriber::new(&cluster, &[leaf_a, leaf_b], QueryParserEngine::default());
    let mut verify = test_server().await;
    sub.connect().await.unwrap();

    let oid_a = Oid(16384);
    let oid_b = Oid(16385);
    let id_a = random_id();
    let id_b = random_id();

    cleanup(&mut verify, "public.sharded", &[&id_a, &id_b]).await;

    // Each leaf has its own oid in the WAL stream but resolves to the same
    // destination table via parent_schema/parent_name.
    let mut relation_a = sharded_relation(oid_a);
    relation_a.name = "sharded_p1".to_string();
    let mut relation_b = sharded_relation(oid_b);
    relation_b.name = "sharded_p2".to_string();

    sub.handle(begin_copy_data(100)).await.unwrap();
    sub.handle(xlog_copy_data(relation_a.to_bytes().unwrap()))
        .await
        .unwrap();
    sub.handle(xlog_copy_data(relation_b.to_bytes().unwrap()))
        .await
        .unwrap();

    sub.handle(insert_copy_data(oid_a, &id_a, "leaf_a"))
        .await
        .unwrap();
    sub.handle(insert_copy_data(oid_b, &id_b, "leaf_b"))
        .await
        .unwrap();

    sub.handle(commit_copy_data(200)).await.unwrap();

    // Both inserts must land in the shared destination table. Before the fix,
    // leaf_b's row would be silently dropped.
    assert_eq!(count_row(&mut verify, "public.sharded", &id_a).await, 1);
    assert_eq!(count_row(&mut verify, "public.sharded", &id_b).await, 1);

    cleanup(&mut verify, "public.sharded", &[&id_a, &id_b]).await;
}

// ── Data flow tests ─────────────────────────────────────────────────

/// Full transaction: Begin → Relation → Insert → Commit, verified in Postgres.
#[tokio::test]
async fn full_insert_transaction() {
    let mut sub = make_subscriber();
    let mut verify = test_server().await;
    sub.connect().await.unwrap();

    let oid = Oid(16384);
    let id = random_id();

    cleanup(&mut verify, "public.sharded", &[&id]).await;

    sub.handle(begin_copy_data(100)).await.unwrap();
    sub.handle(relation_copy_data(oid)).await.unwrap();
    sub.handle(insert_copy_data(oid, &id, "hello"))
        .await
        .unwrap();

    let status = sub.handle(commit_copy_data(200)).await.unwrap();
    assert!(!sub.in_transaction());
    assert!(status.is_some());
    assert_eq!(sub.lsn(), 200);
    assert!(sub.bytes_sharded() > 0);

    assert_eq!(count_row(&mut verify, "public.sharded", &id).await, 1);

    cleanup(&mut verify, "public.sharded", &[&id]).await;
}

/// Insert then delete within two transactions, verify Postgres state after each.
#[tokio::test]
async fn full_delete_transaction() {
    let mut sub = make_subscriber();
    let mut verify = test_server().await;
    sub.connect().await.unwrap();

    let oid = Oid(16384);
    let id = random_id();

    cleanup(&mut verify, "public.sharded", &[&id]).await;

    // Insert
    sub.handle(begin_copy_data(100)).await.unwrap();
    sub.handle(relation_copy_data(oid)).await.unwrap();
    sub.handle(insert_copy_data(oid, &id, "to_delete"))
        .await
        .unwrap();
    sub.handle(commit_copy_data(200)).await.unwrap();

    assert_eq!(count_row(&mut verify, "public.sharded", &id).await, 1);

    // Delete
    sub.handle(begin_copy_data(300)).await.unwrap();
    sub.handle(delete_copy_data(oid, &id)).await.unwrap();
    let status = sub.handle(commit_copy_data(400)).await.unwrap();
    assert!(status.is_some());
    assert!(!sub.in_transaction());

    assert_eq!(count_row(&mut verify, "public.sharded", &id).await, 0);
}

/// Multiple transactions reuse prepared statements, both rows persisted.
#[tokio::test]
async fn multiple_transactions() {
    let mut sub = make_subscriber();
    let mut verify = test_server().await;
    sub.connect().await.unwrap();

    let oid = Oid(16384);
    let id1 = random_id();
    let id2 = random_id();

    cleanup(&mut verify, "public.sharded", &[&id1, &id2]).await;

    // First transaction
    sub.handle(begin_copy_data(100)).await.unwrap();
    sub.handle(relation_copy_data(oid)).await.unwrap();
    sub.handle(insert_copy_data(oid, &id1, "first"))
        .await
        .unwrap();
    let status = sub.handle(commit_copy_data(200)).await.unwrap();
    assert!(status.is_some());
    assert_eq!(sub.lsn(), 200);

    // Second transaction
    sub.handle(begin_copy_data(300)).await.unwrap();
    sub.handle(relation_copy_data(oid)).await.unwrap();
    sub.handle(insert_copy_data(oid, &id2, "second"))
        .await
        .unwrap();
    let status = sub.handle(commit_copy_data(400)).await.unwrap();
    assert!(status.is_some());
    assert_eq!(sub.lsn(), 400);

    assert_eq!(count_row(&mut verify, "public.sharded", &id1).await, 1);
    assert_eq!(count_row(&mut verify, "public.sharded", &id2).await, 1);

    cleanup(&mut verify, "public.sharded", &[&id1, &id2]).await;
}

/// LSN gating: inserts with already-applied LSN are skipped.
#[tokio::test]
async fn lsn_gating_skips_old_inserts() {
    let mut sub = make_subscriber();
    let mut verify = test_server().await;
    sub.connect().await.unwrap();

    let oid = Oid(16384);
    let id = random_id();
    let id2 = random_id();

    cleanup(&mut verify, "public.sharded", &[&id, &id2]).await;

    // First transaction sets table LSN to 100.
    sub.handle(begin_copy_data(100)).await.unwrap();
    sub.handle(relation_copy_data(oid)).await.unwrap();
    sub.handle(insert_copy_data(oid, &id, "first"))
        .await
        .unwrap();
    sub.handle(commit_copy_data(200)).await.unwrap();

    // Second transaction at LSN 50 (behind table LSN 100) — insert skipped.
    sub.handle(begin_copy_data(50)).await.unwrap();
    assert!(sub.lsn_applied(&oid));
    sub.handle(insert_copy_data(oid, &id2, "replayed"))
        .await
        .unwrap();
    sub.handle(commit_copy_data(60)).await.unwrap();

    assert_eq!(count_row(&mut verify, "public.sharded", &id).await, 1);
    assert_eq!(count_row(&mut verify, "public.sharded", &id2).await, 0);

    cleanup(&mut verify, "public.sharded", &[&id]).await;
}

/// Equal LSNs are skipped so streaming does not replay rows already copied by COPY.
#[tokio::test]
async fn lsn_gating_skips_copy_boundary_inserts() {
    let mut table = make_sharded_table();
    table.lsn = Lsn::from_i64(100);

    let mut sub = make_subscriber_with_tables(vec![table, make_sharded_test_b_table()]);
    let mut verify = test_server().await;
    sub.connect().await.unwrap();

    let oid = Oid(16384);
    let id = random_id();

    cleanup(&mut verify, "public.sharded", &[&id]).await;

    sub.handle(begin_copy_data(100)).await.unwrap();
    sub.handle(relation_copy_data(oid)).await.unwrap();
    sub.handle(insert_copy_data(oid, &id, "copied_already"))
        .await
        .unwrap();
    sub.handle(commit_copy_data(200)).await.unwrap();

    assert_eq!(count_row(&mut verify, "public.sharded", &id).await, 0);
}

/// Multiple rows in the same transaction must still be applied after inclusive LSN gating.
#[tokio::test]
async fn multiple_inserts_same_transaction_are_applied() {
    let mut sub = make_subscriber();
    let mut verify = test_server().await;
    sub.connect().await.unwrap();

    let oid = Oid(16384);
    let id1 = random_id();
    let id2 = random_id();

    cleanup(&mut verify, "public.sharded", &[&id1, &id2]).await;

    sub.handle(begin_copy_data(100)).await.unwrap();
    sub.handle(relation_copy_data(oid)).await.unwrap();
    sub.handle(insert_copy_data(oid, &id1, "first"))
        .await
        .unwrap();
    sub.handle(insert_copy_data(oid, &id2, "second"))
        .await
        .unwrap();
    sub.handle(commit_copy_data(200)).await.unwrap();

    assert_eq!(count_row(&mut verify, "public.sharded", &id1).await, 1);
    assert_eq!(count_row(&mut verify, "public.sharded", &id2).await, 1);

    cleanup(&mut verify, "public.sharded", &[&id1, &id2]).await;
}

// ── CopyData round-trip tests ───────────────────────────────────────

#[test]
fn copy_data_round_trip_begin() {
    let cd = begin_copy_data(42);
    let xlog = cd.xlog_data().expect("should parse as XLogData");
    let payload = xlog.payload().expect("should have payload");
    assert!(matches!(
        payload,
        crate::net::replication::xlog_data::XLogPayload::Begin(_)
    ));
}

#[test]
fn copy_data_round_trip_commit() {
    let cd = commit_copy_data(99);
    let xlog = cd.xlog_data().unwrap();
    let payload = xlog.payload().unwrap();
    assert!(matches!(
        payload,
        crate::net::replication::xlog_data::XLogPayload::Commit(_)
    ));
}

#[test]
fn copy_data_round_trip_relation() {
    let cd = relation_copy_data(Oid(16384));
    let xlog = cd.xlog_data().unwrap();
    let payload = xlog.payload().unwrap();
    assert!(matches!(
        payload,
        crate::net::replication::xlog_data::XLogPayload::Relation(_)
    ));
}

#[test]
fn copy_data_round_trip_insert() {
    let cd = insert_copy_data(Oid(16384), "1", "hello");
    let xlog = cd.xlog_data().unwrap();
    let payload = xlog.payload().unwrap();
    assert!(matches!(
        payload,
        crate::net::replication::xlog_data::XLogPayload::Insert(_)
    ));
}

#[test]
fn copy_data_round_trip_delete() {
    let cd = delete_copy_data(Oid(16384), "1");
    let xlog = cd.xlog_data().unwrap();
    let payload = xlog.payload().unwrap();
    assert!(matches!(
        payload,
        crate::net::replication::xlog_data::XLogPayload::Delete(_)
    ));
}

fn make_posts_table() -> Table {
    Table {
        publication: "test".to_string(),
        table: PublicationTable {
            schema: "public".to_string(),
            name: "posts".to_string(),
            attributes: "".to_string(),
            parent_schema: "".to_string(),
            parent_name: "".to_string(),
        },
        identity: ReplicaIdentity {
            oid: Oid(3),
            identity: "".to_string(),
            kind: "".to_string(),
        },
        columns: vec![
            PublicationTableColumn {
                oid: 3,
                name: "id".to_string(),
                type_oid: Oid(20),
                identity: true,
            },
            PublicationTableColumn {
                oid: 3,
                name: "title".to_string(),
                type_oid: Oid(25),
                identity: false,
            },
            PublicationTableColumn {
                oid: 3,
                name: "body".to_string(),
                type_oid: Oid(25),
                identity: false,
            },
        ],
        lsn: Lsn::default(),
        query_parser_engine: QueryParserEngine::default(),
    }
}

fn posts_relation(oid: Oid) -> Relation {
    Relation {
        oid,
        namespace: "public".to_string(),
        name: "posts".to_string(),
        replica_identity: 100,
        columns: vec![
            RelColumn {
                flag: 1,
                name: "id".to_string(),
                oid: Oid(20),
                type_modifier: -1,
            },
            RelColumn {
                flag: 0,
                name: "title".to_string(),
                oid: Oid(25),
                type_modifier: -1,
            },
            RelColumn {
                flag: 0,
                name: "body".to_string(),
                oid: Oid(25),
                type_modifier: -1,
            },
        ],
    }
}

fn posts_relation_copy_data(oid: Oid) -> CopyData {
    xlog_copy_data(posts_relation(oid).to_bytes().unwrap())
}

fn posts_insert_copy_data(oid: Oid, id: &str, title: &str, body: &str) -> CopyData {
    xlog_copy_data(
        XLogInsert {
            xid: None,
            oid,
            tuple_data: TupleData {
                columns: vec![text_column(id), text_column(title), text_column(body)],
            },
        }
        .to_bytes()
        .unwrap(),
    )
}

/// UPDATE for posts: title is set to `new_title`; body is marked Toasted (`'u'`).
/// This produces a tuple where exactly one non-identity column is absent, forcing
/// the subscriber through the slow-path `update_partial` code.
fn posts_update_title_copy_data(oid: Oid, id: &str, new_title: &str) -> CopyData {
    xlog_copy_data(
        XLogUpdate {
            oid,
            key: None,
            old: None,
            new: TupleData {
                columns: vec![text_column(id), text_column(new_title), toasted_column()],
            },
        }
        .to_bytes()
        .unwrap(),
    )
}

async fn fetch_posts_row(server: &mut Server, id: &str) -> Option<(String, String)> {
    let query = format!("SELECT title, body FROM public.posts WHERE id = {}", id);
    let rows: Vec<crate::net::DataRow> = server.fetch_all(query).await.unwrap();
    rows.first().and_then(|row| {
        let title = row
            .column(0)
            .map(|c| std::str::from_utf8(&c[..]).unwrap().to_string())?;
        let body = row
            .column(1)
            .map(|c| std::str::from_utf8(&c[..]).unwrap().to_string())?;
        Some((title, body))
    })
}

// ── Unchanged-TOAST handling tests ───────────────────────────────

/// UPDATE with an unchanged-TOAST column alongside a real updated column
/// exercises the slow path in the subscriber (`update_partial` + `partial_new`).
///
/// Fixture: `posts(id PK, title text, body text)`.  `body` is Toasted (`'u'`);
/// `title` carries a new value.  The subscriber must emit
/// `UPDATE posts SET title=$1 WHERE id=$2`, updating `title` and leaving `body` intact.
#[tokio::test]
async fn toast_update_preserves_unchanged_column() {
    let oid = Oid(16384);
    let mut sub = make_subscriber_with_tables(vec![make_posts_table()]);
    let mut verify = test_server().await;
    sub.connect().await.unwrap();

    let id = random_id();
    cleanup(&mut verify, "public.posts", &[&id]).await;

    // Seed the destination row.
    sub.handle(begin_copy_data(100)).await.unwrap();
    sub.handle(posts_relation_copy_data(oid)).await.unwrap();
    sub.handle(posts_insert_copy_data(
        oid,
        &id,
        "original-title",
        "original-large-body",
    ))
    .await
    .unwrap();
    sub.handle(commit_copy_data(200)).await.unwrap();

    let row = fetch_posts_row(&mut verify, &id)
        .await
        .expect("seed INSERT did not land");
    assert_eq!(row.0, "original-title");
    assert_eq!(row.1, "original-large-body");

    // UPDATE: title gets a new value; body is Toasted — slow path must execute.
    sub.handle(begin_copy_data(300)).await.unwrap();
    sub.handle(posts_update_title_copy_data(oid, &id, "updated-title"))
        .await
        .unwrap();
    sub.handle(commit_copy_data(400)).await.unwrap();

    let row = fetch_posts_row(&mut verify, &id)
        .await
        .expect("row disappeared after UPDATE");
    assert_eq!(row.0, "updated-title", "title was not updated");
    assert_eq!(
        row.1, "original-large-body",
        "unchanged-TOAST body was overwritten"
    );

    cleanup(&mut verify, "public.posts", &[&id]).await;
}

/// Two UPDATEs with the same TOAST shape (body Toasted, title updated) must
/// reuse the cached prepared statement generated by `ensure_update_shape` on the
/// first pass.  Both updates must apply correctly end-to-end.
#[tokio::test]
async fn toast_update_shape_reuse() {
    let oid = Oid(16384);
    let mut sub = make_subscriber_with_tables(vec![make_posts_table()]);
    let mut verify = test_server().await;
    sub.connect().await.unwrap();

    let id = random_id();
    cleanup(&mut verify, "public.posts", &[&id]).await;

    sub.handle(begin_copy_data(100)).await.unwrap();
    sub.handle(posts_relation_copy_data(oid)).await.unwrap();
    sub.handle(posts_insert_copy_data(oid, &id, "seed-title", "seed-body"))
        .await
        .unwrap();
    sub.handle(commit_copy_data(200)).await.unwrap();

    // Two UPDATEs with identical TOAST shape: title changes each time, body stays
    // Toasted.  The second must hit the shape cache without re-preparing.
    sub.handle(begin_copy_data(300)).await.unwrap();
    sub.handle(posts_update_title_copy_data(oid, &id, "first-update"))
        .await
        .unwrap();
    sub.handle(posts_update_title_copy_data(oid, &id, "second-update"))
        .await
        .unwrap();
    sub.handle(commit_copy_data(400)).await.unwrap();

    let row = fetch_posts_row(&mut verify, &id)
        .await
        .expect("row disappeared after shape-reuse UPDATEs");
    assert_eq!(row.0, "second-update", "second UPDATE title did not apply");
    assert_eq!(
        row.1, "seed-body",
        "body was overwritten during shape-reuse"
    );

    cleanup(&mut verify, "public.posts", &[&id]).await;
}

/// UPDATE where ALL non-identity columns are Toasted — the no-op branch must fire.
/// The destination row must be exactly as seeded; the LSN watermark must still advance.
#[tokio::test]
async fn toast_update_all_toasted_is_noop() {
    let oid = Oid(16384);
    let mut sub = make_subscriber_with_tables(vec![make_posts_table()]);
    let mut verify = test_server().await;
    sub.connect().await.unwrap();

    let id = random_id();
    cleanup(&mut verify, "public.posts", &[&id]).await;

    // Seed a row with known values.
    sub.handle(begin_copy_data(100)).await.unwrap();
    sub.handle(posts_relation_copy_data(oid)).await.unwrap();
    sub.handle(posts_insert_copy_data(
        oid,
        &id,
        "original-title",
        "original-body",
    ))
    .await
    .unwrap();
    sub.handle(commit_copy_data(200)).await.unwrap();

    let row = fetch_posts_row(&mut verify, &id)
        .await
        .expect("seed INSERT did not land");
    assert_eq!(row.0, "original-title");
    assert_eq!(row.1, "original-body");

    // UPDATE where both non-identity columns are Toasted — no-op path.
    sub.handle(begin_copy_data(300)).await.unwrap();
    sub.handle(xlog_copy_data(
        XLogUpdate {
            oid,
            key: None,
            old: None,
            new: TupleData {
                columns: vec![text_column(&id), toasted_column(), toasted_column()],
            },
        }
        .to_bytes()
        .unwrap(),
    ))
    .await
    .unwrap();
    sub.handle(commit_copy_data(400)).await.unwrap();

    // Watermark advanced even though we took the no-op path.
    assert_eq!(sub.lsn(), 400);

    // Row unchanged — the no-op path must not have touched either column.
    let row = fetch_posts_row(&mut verify, &id)
        .await
        .expect("row disappeared after all-TOAST no-op UPDATE");
    assert_eq!(
        row.0, "original-title",
        "title changed during all-TOAST no-op"
    );
    assert_eq!(
        row.1, "original-body",
        "body changed during all-TOAST no-op"
    );

    cleanup(&mut verify, "public.posts", &[&id]).await;
}

/// PK-change UPDATE with 'u' in the new tuple fails with ToastedRowMigration.
#[tokio::test]
async fn toast_pk_change_with_u_rejects() {
    let mut sub = make_subscriber();
    sub.connect().await.unwrap();

    let oid = Oid(16384);

    sub.handle(begin_copy_data(100)).await.unwrap();
    sub.handle(relation_copy_data(oid)).await.unwrap();
    let err = sub
        .handle(x_update(XLogUpdate {
            oid,
            key: Some(TupleData {
                columns: vec![text_column("old")],
            }),
            old: None,
            new: TupleData {
                columns: vec![text_column("new"), toasted_column()],
            },
        }))
        .await
        .expect_err("expected ToastedRowMigration");
    assert!(
        matches!(
            err,
            crate::backend::replication::logical::Error::ToastedRowMigration { .. }
        ),
        "got: {:?}",
        err
    );
}

/// No key: pgoutput emits 'u' for an out-of-line identity column that didn't change.
#[tokio::test]
async fn update_rejects_toasted_identity_no_key() {
    let oid = Oid(16384);
    let mut sub = make_subscriber_with_tables(vec![make_posts_table()]);
    sub.connect().await.unwrap();

    sub.handle(begin_copy_data(100)).await.unwrap();
    sub.handle(posts_relation_copy_data(oid)).await.unwrap();
    let err = sub
        .handle(x_update(XLogUpdate {
            oid,
            key: None,
            old: None,
            new: TupleData {
                columns: vec![
                    toasted_column(),
                    text_column("new-title"),
                    text_column("new-body"),
                ],
            },
        }))
        .await
        .expect_err("toasted identity must be rejected");
    assert!(
        matches!(
            err,
            crate::backend::replication::logical::Error::ToastedIdentityColumn { .. }
        ),
        "got: {err:?}"
    );
}

/// Key present (USING INDEX replica identity): identity column is still 'u' in the new tuple.
#[tokio::test]
async fn update_rejects_toasted_identity_with_key() {
    let oid = Oid(16384);
    let mut sub = make_subscriber_with_tables(vec![make_posts_table()]);
    sub.connect().await.unwrap();

    sub.handle(begin_copy_data(100)).await.unwrap();
    sub.handle(posts_relation_copy_data(oid)).await.unwrap();
    let err = sub
        .handle(x_update(XLogUpdate {
            oid,
            key: Some(TupleData {
                columns: vec![text_column("42")],
            }),
            old: None,
            new: TupleData {
                columns: vec![
                    toasted_column(),
                    text_column("new-title"),
                    text_column("new-body"),
                ],
            },
        }))
        .await
        .expect_err("toasted identity with key must be rejected");
    assert!(
        matches!(
            err,
            crate::backend::replication::logical::Error::ToastedIdentityColumn { .. }
        ),
        "got: {err:?}"
    );
}
