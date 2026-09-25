use std::time::Duration;

use tokio::select;
use tokio::time::{Instant, MissedTickBehavior};
use tokio::try_join;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::replication_progress::{ReplicationProgressShardUpdater, ReplicationShardProgress};
use super::{Lsn, ReplicationData, ReplicationSlotGuard, Table};
use crate::backend::Cluster;
use crate::backend::replication::logical::Error;
use crate::backend::replication::logical::subscriber::stream::StreamSubscriber;
use crate::net::replication::ReplicationMeta;
use crate::util::{safe_interval, safe_sleep};

/// Runs the replication stream from a single shard (slot)
/// to the destination cluster.
#[derive(Debug)]
pub(crate) struct ReplicationStream {
    source_name: String,
    dest_cluster: Cluster,
    updater: ReplicationProgressShardUpdater,
}

impl ReplicationStream {
    pub(crate) fn new(
        source: &Cluster,
        dest: &Cluster,
        updater: ReplicationProgressShardUpdater,
    ) -> Self {
        Self {
            source_name: source.name().to_owned(),
            dest_cluster: dest.clone(),
            updater,
        }
    }

    pub(crate) fn progress(&self) -> ReplicationShardProgress {
        self.updater.snapshot()
    }

    pub(crate) async fn run(
        &self,
        slot: &mut ReplicationSlotGuard,
        tables: Vec<Table>,
        stop: &CancellationToken,
    ) -> Result<(), Error> {
        let mut stream = StreamSubscriber::new(&self.dest_cluster, tables);
        stream.set_current_lsn(slot.lsn().lsn);
        self.updater.update(|p| {
            p.advance_applied_lsn(slot.lsn());
            p.started = Some(Instant::now());
        });
        let result = self.replicate(slot, &mut stream, stop).await;
        let final_lsn = Lsn::from_i64(stream.status_update().last_applied);
        let missed = stream.missed_rows();
        self.updater.update(|p| {
            p.advance_applied_lsn(final_lsn);
            p.missed_rows.merge(missed);
        });
        result
    }

    async fn update_progress(
        &self,
        slot: &mut ReplicationSlotGuard,
        stream: &mut StreamSubscriber,
    ) -> Result<(), Error> {
        let missed = stream.missed_rows();
        let applied = Lsn::from_i64(stream.status_update().last_applied);
        let bytes_sharded = stream.bytes_sharded();
        let rows_sharded = stream.rows_sharded();
        let origin_lsn = Lsn::from_i64(stream.lsn());
        self.updater.update(|p| {
            p.advance_applied_lsn(applied);
            p.missed_rows.merge(missed);
            p.bytes_sharded = bytes_sharded;
            p.rows_sharded = rows_sharded;
            p.origin_lsn = origin_lsn;
        });
        if missed.non_zero() {
            warn!(
                "replication {} => {} has missing rows: {}",
                self.source_name,
                self.dest_cluster.name(),
                missed
            );
        }
        let lag = slot.replication_lag().await?;
        self.updater.update(|p| p.replication_lag = Some(lag));
        Ok(())
    }

    async fn replicate(
        &self,
        slot: &mut ReplicationSlotGuard,
        stream: &mut StreamSubscriber,
        stop: &CancellationToken,
    ) -> Result<(), Error> {
        let mut check_lag = safe_interval(Duration::from_secs(1));
        check_lag.set_missed_tick_behavior(MissedTickBehavior::Delay);
        slot.start_replication().await?;

        let max_attempts = self
            .dest_cluster
            .resharding_replication_retry_max_attempts();
        let delay = self.dest_cluster.resharding_replication_retry_min_delay();

        let mut attempt = 0usize;

        loop {
            let stopping = slot.stopped();

            select! {
                biased;

                _ = stop.cancelled(), if !stopping => {
                    slot.stop_replication().await?;
                }

                _ = check_lag.tick() => {
                    if let Err(err) = self.update_progress(slot, stream).await {
                        self.updater.update(|p| p.replication_lag = None);
                        warn!(
                            "[replication] progress update failed for slot \"{}\": {err}",
                            slot.name()
                        );
                    }
                }

                replication_data = slot.replicate(Duration::MAX) => {
                    // Returns Ok(true) when the slot is drained and the loop
                    // should break; Ok(false) to continue on next loop. All errors bubble up
                    // to the single retry/abort site below.
                    let done: Result<bool, Error> = async {
                        let Some(replication_data) = replication_data? else {
                            return Ok(true);
                        };
                        match replication_data {
                            ReplicationData::CopyData(data) => {
                                if let Some(ReplicationMeta::KeepAlive(ka)) =
                                    data.replication_meta()
                                {
                                    // Advance the lsn if we are not in the transaction currently
                                    // (we don't use transactions actually without streaming on protocol version 4,
                                    // but let it be as a safeguard).
                                    // If we got the keep-alive message and not the update message
                                    // then it's for the unrelated changes that advanced WAL.
                                    // Since it's unrelated we can advance our progress and
                                    // consider that lag replication
                                    let advanced = !stream.in_transaction()
                                        && ka.wal_end > stream.lsn()
                                        && stream.set_current_lsn(ka.wal_end);

                                    // Reply to walsender if it asked for reply or
                                    // if we advanced due to the WAL progress but
                                    // the update was not related
                                    if advanced || ka.reply() {
                                        slot.status_update(stream.status_update()).await?;
                                    }
                                    debug!(
                                        "origin at lsn {} [{}]",
                                        Lsn::from_i64(ka.wal_end),
                                        slot.addr()
                                    );
                                } else {
                                    if let Some(su) = stream.handle(data).await? {
                                        let applied = Lsn::from_i64(su.last_applied);
                                        slot.status_update(su).await?;
                                        self.updater.update(|p| {
                                            p.last_transaction = Some(Instant::now());
                                            p.advance_applied_lsn(applied);
                                        });
                                    }
                                    attempt = 0;
                                }
                                Ok(false)
                            }
                            ReplicationData::CopyDone => Ok(false),
                        }
                    }
                    .await;

                    match done {
                        Ok(true) => break,
                        Ok(false) => {}
                        Err(mut err) => loop {
                            if stop.is_cancelled()
                                || !err.is_retryable()
                                || (max_attempts != 0 && attempt >= max_attempts)
                            {
                                return Err(err);
                            }
                            attempt += 1;
                            warn!(
                                "[replication] error ({attempt}/{max_attempts}): {err}, reconnecting in {}ms",
                                delay.as_millis()
                            );
                            if stop
                                .run_until_cancelled(safe_sleep(delay))
                                .await
                                .is_none()
                            {
                                return Err(err);
                            }
                            let missed = stream.missed_rows();
                            self.updater.update(|p| p.missed_rows.merge(missed));
                            match try_join!(slot.reconnect(), stream.reconnect()) {
                                Ok(_) => break,
                                Err(reconnect_err) => {
                                    stream.reset_connections();
                                    err = reconnect_err;
                                }
                            }
                        },
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error as StdError, sync::Arc, time::Duration};

    use tokio::{
        task::JoinHandle,
        time::{sleep, timeout},
    };

    use crate::{
        backend::replication::logical::publisher::ReplicationSlot,
        backend::replication::logical::publisher::replication_progress::{
            ReplicationProgress, ReplicationShardProgress,
        },
        backend::{Server, server::test::test_server},
        config::config,
        util::random_string,
    };

    use super::*;

    type TestResult = Result<(), Box<dyn StdError>>;

    struct Fixture {
        source_table: String,
        destination_table: String,
        publication: String,
        slot_base: String,
        server: Server,
        source: Cluster,
        replication: Arc<ReplicationStream>,
        stop: CancellationToken,
        worker: Option<JoinHandle<Result<(), Error>>>,
    }

    impl Fixture {
        async fn new() -> Self {
            let suffix = random_string(12).to_lowercase();
            let source = Cluster::new_test_single_shard(&config());
            let progress = ReplicationProgress::new(1);
            let updater = progress.updater_for_shard(0);
            let replication = Arc::new(ReplicationStream::new(&source, &source, updater));
            Self {
                source_table: format!("replication_source_{suffix}"),
                destination_table: format!("replication_destination_{suffix}"),
                publication: format!("replication_publication_{suffix}"),
                slot_base: format!("replication_slot_{suffix}"),
                server: test_server().await,
                source,
                replication,
                stop: CancellationToken::new(),
                worker: None,
            }
        }

        async fn start(&mut self) -> TestResult {
            self.source.launch();
            for table in [&self.source_table, &self.destination_table] {
                self.server
                    .execute_checked(format!(
                        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, val TEXT NOT NULL)"
                    ))
                    .await?;
            }
            self.server
                .execute_checked(format!(
                    "CREATE PUBLICATION {} FOR TABLE {}",
                    self.publication, self.source_table
                ))
                .await?;
            let mut tables = Table::load(&self.publication, &mut self.server).await?;
            let table = tables.first_mut().ok_or("publication has no table")?;
            table.table.parent_name = self.destination_table.clone();
            table.table.parent_schema = table.table.schema.clone();
            let slot = ReplicationSlot::new_permanent(
                &self.publication,
                self.server.addr(),
                Some(self.slot_base.clone()),
                0,
            );
            slot.create().await?;
            let mut guard = slot.get_existing().await?;
            if self.replication.progress().replication_lag.is_some() {
                return Err("lag was measured before replication started".into());
            }
            let replication = Arc::clone(&self.replication);
            let stop = self.stop.clone();
            self.worker = Some(tokio::spawn(async move {
                let result = Box::pin(replication.run(&mut guard, tables, &stop)).await;
                drop(guard);
                let dropped = slot.drop_slot().await;
                result.and(dropped)
            }));
            Ok(())
        }

        async fn wait_for(
            &mut self,
            query: String,
            ready: impl Fn(&[String], ReplicationShardProgress) -> bool,
        ) -> TestResult {
            timeout(Duration::from_secs(10), async {
                loop {
                    let rows: Vec<String> = self.server.fetch_all(query.clone()).await?;
                    if ready(&rows, self.replication.progress()) {
                        return Ok::<(), Box<dyn StdError>>(());
                    }
                    if self.worker.as_ref().is_some_and(JoinHandle::is_finished) {
                        return Err("replication stopped before the expected result".into());
                    }
                    sleep(Duration::from_millis(20)).await;
                }
            })
            .await??;
            Ok(())
        }

        async fn stop(&mut self) -> TestResult {
            self.stop.cancel();
            let result = if let Some(worker) = self.worker.as_mut() {
                match timeout(Duration::from_secs(10), &mut *worker).await {
                    Ok(result) => result
                        .map_err(Box::<dyn StdError>::from)
                        .and_then(|result| result.map_err(Box::<dyn StdError>::from)),
                    Err(error) => {
                        worker.abort();
                        let _ = worker.await;
                        Err(error.into())
                    }
                }
            } else {
                Ok(())
            };
            self.worker.take();
            result
        }

        async fn cleanup(&mut self) -> TestResult {
            let mut result = self.stop().await;
            self.source.shutdown();
            let mut server = test_server().await;
            for query in [
                format!(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '{}_0'",
                    self.slot_base
                ),
                format!("DROP PUBLICATION IF EXISTS {}", self.publication),
                format!("DROP TABLE IF EXISTS {}", self.source_table),
                format!("DROP TABLE IF EXISTS {}", self.destination_table),
            ] {
                let cleanup: TestResult = async {
                    timeout(Duration::from_secs(10), server.execute_checked(query)).await??;
                    Ok(())
                }
                .await;
                result = result.and(cleanup);
            }
            result
        }
    }

    async fn with_fixture(test: impl AsyncFnOnce(&mut Fixture) -> TestResult) -> TestResult {
        crate::logger();
        let mut fixture = Fixture::new().await;
        let result: TestResult = async {
            timeout(Duration::from_secs(30), fixture.start()).await??;
            timeout(Duration::from_secs(30), test(&mut fixture)).await??;
            Ok(())
        }
        .await;
        let cleanup = fixture.cleanup().await;
        result.and(cleanup)
    }

    #[tokio::test]
    async fn replication_applies_dml_and_reports_progress() -> TestResult {
        with_fixture(async |fixture| {
            fixture
                .server
                .execute_checked(format!(
                    "INSERT INTO {} VALUES (1, 'alpha')",
                    fixture.source_table
                ))
                .await?;
            let query = format!("SELECT val FROM {} ORDER BY id", fixture.destination_table);
            fixture
                .wait_for(query.clone(), |rows, info| {
                    rows == ["alpha"] && info.last_transaction.is_some()
                })
                .await?;
            let first_transaction = fixture.replication.progress().last_transaction;

            fixture
                .server
                .execute_checked(format!(
                    "UPDATE {} SET val = 'beta' WHERE id = 1",
                    fixture.source_table
                ))
                .await?;
            fixture
                .wait_for(query.clone(), |rows, info| {
                    rows == ["beta"] && info.last_transaction > first_transaction
                })
                .await?;

            fixture
                .server
                .execute_checked(format!("DELETE FROM {} WHERE id = 1", fixture.source_table))
                .await?;
            fixture
                .wait_for(query, |rows, info| {
                    rows.is_empty() && info.replication_lag.is_some_and(|lag| lag >= 0)
                })
                .await
        })
        .await
    }

    #[tokio::test]
    async fn replication_cancellation_drops_slot() -> TestResult {
        with_fixture(async |fixture| {
            let slot_query = format!(
                "SELECT slot_name FROM pg_replication_slots WHERE slot_name = '{}_0'",
                fixture.slot_base
            );
            let before: Vec<String> = fixture.server.fetch_all(slot_query.clone()).await?;
            if before != [format!("{}_0", fixture.slot_base)] {
                return Err("replication slot is missing before cancellation".into());
            }
            fixture
                .server
                .execute_checked(format!(
                    "INSERT INTO {} VALUES (1, 'committed')",
                    fixture.source_table
                ))
                .await?;
            let query = format!("SELECT val FROM {} ORDER BY id", fixture.destination_table);
            fixture
                .wait_for(query.clone(), |rows, info| {
                    rows == ["committed"] && info.last_transaction.is_some()
                })
                .await?;

            fixture.stop().await?;
            let after: Vec<String> = fixture.server.fetch_all(slot_query).await?;
            if !after.is_empty() {
                return Err("replication slot remains after shutdown".into());
            }
            let rows: Vec<String> = fixture.server.fetch_all(query).await?;
            if rows != ["committed"] {
                return Err("committed destination row changed during shutdown".into());
            }
            Ok(())
        })
        .await
    }

    async fn missed_rows_kill_walsender(fixture: &mut Fixture) -> TestResult {
        let killed: Vec<String> = fixture
            .server
            .fetch_all(format!(
                "SELECT pg_terminate_backend(active_pid)::text FROM pg_replication_slots \
             WHERE slot_name = '{}_0' AND active_pid IS NOT NULL",
                fixture.slot_base
            ))
            .await?;
        if killed != ["true"] {
            return Err("replication connection was not terminated".into());
        }
        Ok(())
    }

    async fn missed_rows_cause_missed_update(
        fixture: &mut Fixture,
        id: i64,
        sentinel_id: i64,
    ) -> TestResult {
        fixture
            .server
            .execute_checked(format!(
                "DELETE FROM {} WHERE id = {id}; \
             UPDATE {} SET val = 'miss' WHERE id = {id}; \
             INSERT INTO {} VALUES ({sentinel_id}, 'sentinel')",
                fixture.destination_table, fixture.source_table, fixture.source_table
            ))
            .await?;
        let dest_query = format!(
            "SELECT id::text FROM {} ORDER BY id",
            fixture.destination_table
        );
        let sentinel = sentinel_id.to_string();
        fixture
            .wait_for(dest_query, |rows, _| rows.iter().any(|r| r == &sentinel))
            .await
    }
    #[tokio::test]
    async fn replication_missed_rows_survive_reconnect_and_accumulate() -> TestResult {
        with_fixture(async |fixture| {
            fixture
                .server
                .execute_checked(format!(
                    "INSERT INTO {} VALUES (1, 'row')",
                    fixture.source_table
                ))
                .await?;
            let dest_query = format!(
                "SELECT id::text FROM {} ORDER BY id",
                fixture.destination_table
            );
            fixture
                .wait_for(dest_query.clone(), |rows, _| rows.iter().any(|r| r == "1"))
                .await?;

            missed_rows_cause_missed_update(fixture, 1, 2).await?;
            missed_rows_kill_walsender(fixture).await?;

            fixture
                .server
                .execute_checked(format!(
                    "INSERT INTO {} VALUES (3, 'post_reconnect')",
                    fixture.source_table
                ))
                .await?;
            fixture
                .wait_for(dest_query.clone(), |rows, _| rows.iter().any(|r| r == "3"))
                .await?;

            if fixture.replication.progress().missed_rows.updates == 0 {
                return Err("missed update count was lost during reconnect".into());
            }

            missed_rows_cause_missed_update(fixture, 3, 4).await?;

            fixture.stop().await?;
            if fixture.replication.progress().missed_rows.updates < 2 {
                return Err("missed updates did not accumulate across reconnect".into());
            }
            Ok(())
        })
        .await
    }
}
