use std::collections::HashSet;
use std::time::Duration;

use tempfile::TempDir;

use crate::backend::pool::Request;
use crate::backend::{Cluster, databases::databases};
use crate::config;
use crate::frontend::client::query_engine::TwoPcPhase;
use crate::frontend::client::query_engine::two_pc::statement::phase_control;
use crate::frontend::client::query_engine::two_pc::{Manager, TwoPcTransaction, wal::Segment};
use crate::net::DataRow;

pub(super) struct TwoPcTestClient {
    manager: Manager,
    cluster: Cluster,
    wal_segment_size: usize,
    checkpoint_interval: Option<Duration>,
    #[allow(dead_code)]
    tmp: TempDir,
}

impl TwoPcTestClient {
    // This is the execution flow in our query engine.
    //
    // It can be interrupted at any point and, if the WAL works correctly,
    // be recovered without abandoned transactions left on the server.
    //
    pub(super) async fn execute(&self) {
        let mut conns = vec![];

        for shard in self.cluster.shards() {
            conns.push(shard.primary(&Request::default()).await.unwrap());
        }

        let txn = TwoPcTransaction::new();

        for conn in &mut conns {
            conn.execute("BEGIN").await.unwrap();
        }

        let _guard = self
            .manager
            .transaction_state(txn.clone(), &self.cluster.identifier(), TwoPcPhase::Phase1)
            .await
            .unwrap();

        for (shard, conn) in conns.iter_mut().enumerate() {
            conn.execute(phase_control(&txn, shard, TwoPcPhase::Phase1))
                .await
                .unwrap();
        }

        let _guard = self
            .manager
            .transaction_state(txn.clone(), &self.cluster.identifier(), TwoPcPhase::Phase2)
            .await
            .unwrap();

        for (shard, conn) in conns.iter_mut().enumerate() {
            conn.execute(phase_control(&txn, shard, TwoPcPhase::Phase2))
                .await
                .unwrap();
        }

        self.manager.done(txn).await.unwrap();
    }

    pub(super) async fn new(
        wal_segment_size: usize,
        checkpoint_interval: Option<Duration>,
    ) -> Self {
        crate::logger();
        config::load_test_sharded();

        let manager = Manager::init();
        let tmp = TempDir::new().unwrap();
        manager
            .enable_wal(
                &tmp.path().to_owned(),
                checkpoint_interval,
                wal_segment_size,
                Duration::ZERO,
            )
            .await
            .unwrap();
        let cluster = databases()
            .cluster(("pgdog", Some("pgdog")))
            .expect("test database is not configured");

        Self {
            cluster,
            manager,
            wal_segment_size,
            checkpoint_interval,
            tmp,
        }
    }

    pub(super) async fn get_segment(&self, number: u64) -> Segment {
        Segment::load(&Segment::path(self.tmp.path(), number))
            .await
            .unwrap()
    }

    pub(super) fn pool_transactions(&self) -> usize {
        self.cluster
            .shards()
            .iter()
            .flat_map(|shard| shard.pool_iter())
            .map(|pool| pool.state().stats.counts.xact_count)
            .sum()
    }

    pub(super) async fn prepared_transactions(&self) -> usize {
        let transaction = TwoPcTransaction::new().to_string();
        let prefix = transaction.rsplit_once('_').unwrap().0;
        let mut transactions = HashSet::new();

        for shard in self.cluster.shards() {
            let mut server = shard.primary(&Request::default()).await.unwrap();
            let records: Vec<DataRow> = server
                .fetch_all("SELECT gid FROM pg_prepared_xacts")
                .await
                .unwrap();

            transactions.extend(
                records
                    .into_iter()
                    .filter_map(|record| record.get_text(0))
                    .filter(|transaction| transaction.starts_with(prefix)),
            );
        }

        transactions.len()
    }

    pub(super) async fn recover(&self) -> Manager {
        let manager = Manager::init();
        manager
            .enable_wal(
                &self.tmp.path().to_owned(),
                self.checkpoint_interval,
                self.wal_segment_size,
                Duration::ZERO,
            )
            .await
            .unwrap();
        manager
    }

    pub(super) async fn shutdown(&self) {
        self.manager.shutdown().await;
    }
}
