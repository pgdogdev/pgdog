//! Synthesize a 2PC WAL containing a Begin + Committing pair so the
//! crash-safety integration spec can simulate the Phase2 recovery
//! scenario without having to crash pgdog at exactly the right
//! microsecond.
//!
//! Usage: `wal_helper <wal_dir> <gid> <user> <database>`

use std::path::PathBuf;
use std::str::FromStr;

use bytes::BytesMut;

use pgdog::frontend::client::query_engine::two_pc::TwoPcTransaction;
use pgdog::frontend::client::query_engine::two_pc::wal::{
    BeginPayload, Record, Segment, TxnPayload,
};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 {
        eprintln!("usage: {} <wal_dir> <gid> <user> <database>", args[0]);
        std::process::exit(2);
    }
    let dir = PathBuf::from(&args[1]);
    let txn = TwoPcTransaction::from_str(&args[2])
        .unwrap_or_else(|_| panic!("invalid gid {:?}", args[2]));
    let user = args[3].clone();
    let database = args[4].clone();

    std::fs::create_dir_all(&dir).expect("create wal dir");

    let mut segment = Segment::create(&dir, 0).await.expect("create segment");

    let mut buf = BytesMut::new();
    Record::Begin(BeginPayload {
        txn,
        user,
        database,
    })
    .encode(&mut buf)
    .expect("encode begin");
    Record::Committing(TxnPayload { txn })
        .encode(&mut buf)
        .expect("encode committing");

    segment.commit(&buf, 2).await.expect("commit batch");
}
