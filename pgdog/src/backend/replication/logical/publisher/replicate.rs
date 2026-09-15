use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use tokio::select;
use tokio::time::Instant;
use tokio::try_join;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::progress::Progress;
use super::{Lsn, ReplicationData, ReplicationSlot, Table};
use crate::backend::Cluster;
use crate::backend::replication::logical::Error;
use crate::backend::replication::logical::subscriber::stream::{MissedRows, StreamSubscriber};
use crate::net::replication::ReplicationMeta;
use crate::util::{safe_interval, safe_sleep};

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ReplicationInfo {
    pub(crate) replication_lag: Option<i64>,
    pub(crate) last_transaction: Option<Instant>,
    pub(crate) last_transaction_ms: Option<i64>,
    pub(crate) applied_lsn: Option<Lsn>,
    pub(crate) missed_rows: MissedRows,
}

#[derive(Debug)]
#[cfg_attr(test, derive(Default))]
pub(crate) struct Replication {
    source: Cluster,
    dest: Cluster,
    info: Mutex<ReplicationInfo>,
}

impl Replication {
    pub(crate) fn new(source: &Cluster, dest: &Cluster) -> Self {
        Self {
            source: source.clone(),
            dest: dest.clone(),
            info: Mutex::default(),
        }
    }

    pub(crate) fn info(&self) -> ReplicationInfo {
        *self.info.lock()
    }

    pub(crate) async fn run(
        &self,
        mut slot: ReplicationSlot,
        tables: Vec<Table>,
        stop: &CancellationToken,
    ) -> Result<(), Error> {
        let mut stream = StreamSubscriber::new(&self.dest, tables);
        stream.set_current_lsn(slot.lsn().lsn);
        self.info.lock().applied_lsn = Some(slot.lsn());
        let result = self.replicate(&mut slot, &mut stream, stop).await;
        let mut info = self.info.lock();
        info.applied_lsn = Some(Lsn::from_i64(stream.status_update().last_applied));
        info.missed_rows.merge(stream.missed_rows());
        result
    }

    async fn update_info(
        &self,
        slot: &mut ReplicationSlot,
        stream: &mut StreamSubscriber,
    ) -> Result<(), Error> {
        let lag = slot.replication_lag().await?;
        // W: what is missed rows and how do we track them?
        let missed = stream.missed_rows();
        {
            let mut info = self.info.lock();
            info.replication_lag = Some(lag);
            info.applied_lsn = Some(Lsn::from_i64(stream.status_update().last_applied));
            info.missed_rows.merge(missed);
        }
        if missed.non_zero() {
            warn!(
                "replication {} => {} has missing rows: {}",
                self.source.name(),
                self.dest.name(),
                missed
            );
        }
        Ok(())
    }

    async fn replicate(
        &self,
        slot: &mut ReplicationSlot,
        stream: &mut StreamSubscriber,
        stop: &CancellationToken,
    ) -> Result<(), Error> {
        let mut check_lag = safe_interval(Duration::from_secs(1));
        slot.start_replication().await?;

        let progress = Progress::new_stream();
        let max_attempts = self.dest.resharding_replication_retry_max_attempts();
        let delay = self.dest.resharding_replication_retry_min_delay();

        let mut attempt = 0usize;
        let mut stopping = false;

        loop {
            select! {
                _ = stop.cancelled(), if !stopping => {
                    // trigger the stop replication and enable the stopped flag
                    // to not call stop again but still drain the messages from
                    // slot to stream until the source closed by itself.
                    slot.stop_replication().await?;
                    stopping = true;
                }

                replication_data = slot.replicate(Duration::MAX) => {
                    // Returns Ok(true) when the slot is drained and the loop
                    // should break; Ok(false) to continue on next loop. All errors bubble up
                    // to the single retry/abort site below.
                    let done: Result<bool, Error> = async {
                        let Some(replication_data) = replication_data? else {
                            // no data - drop the slot and mark it as done
                            slot.drop_slot().await?;
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
                                        slot.server()?.addr()
                                    );
                                    progress.update(stream.bytes_sharded(), ka.wal_end);
                                } else {
                                    if let Some(su) = stream.handle(data).await? {
                                        slot.status_update(su).await?;
                                        let mut info = self.info.lock();
                                        info.last_transaction = Some(Instant::now());
                                        info.applied_lsn = Some(Lsn::from_i64(stream.status_update().last_applied));
                                        info.last_transaction_ms = SystemTime::now()
                                            .duration_since(UNIX_EPOCH)
                                            .ok()
                                            .and_then(|elapsed| elapsed.as_millis().try_into().ok());
                                    }
                                    attempt = 0;
                                    progress.update(stream.bytes_sharded(), stream.lsn());
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
                        Err(err)
                            if !stopping
                                && err.is_retryable()
                                && (max_attempts == 0 || attempt < max_attempts) =>
                        {
                            attempt += 1;
                            warn!(
                                "[replication] error ({attempt}/{max_attempts}): {err}, reconnecting in {}ms",
                                delay.as_millis()
                            );
                            safe_sleep(delay).await;
                            self.info.lock().missed_rows.merge(stream.missed_rows());
                            if let Err(reconnect_err) =
                                try_join!(slot.reconnect(), stream.reconnect())
                            {
                                if !reconnect_err.is_retryable() {
                                    return Err(reconnect_err);
                                }
                                stream.reset_connections();
                                warn!(
                                    "[replication] reconnect error ({attempt}/{max_attempts}): {reconnect_err}, will retry"
                                );
                            }
                        }
                        Err(err) => return Err(err),
                    }
                }

                _ = check_lag.tick() => {
                    self.update_info(slot, stream).await?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
impl Replication {
    pub(super) fn set_replication_lag(&self, lag: i64) {
        self.info.lock().replication_lag = Some(lag);
    }

    pub(super) fn set_last_transaction(&self, instant: Option<Instant>) {
        self.info.lock().last_transaction = instant;
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
        replication: Arc<Replication>,
        stop: CancellationToken,
        worker: Option<JoinHandle<Result<(), Error>>>,
    }

    impl Fixture {
        async fn new() -> Self {
            let suffix = random_string(12).to_lowercase();
            let source = Cluster::new_test_single_shard(&config());
            let replication = Arc::new(Replication::new(&source, &source));
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
            let mut slot = ReplicationSlot::replication(
                &self.publication,
                self.server.addr(),
                Some(self.slot_base.clone()),
                0,
            );
            slot.create_slot().await?;
            if self.replication.info().replication_lag.is_some() {
                return Err("lag was measured before replication started".into());
            }
            let replication = Arc::clone(&self.replication);
            let stop = self.stop.clone();
            self.worker = Some(tokio::spawn(async move {
                Box::pin(replication.run(slot, tables, &stop)).await
            }));
            Ok(())
        }

        async fn wait_for(
            &mut self,
            query: String,
            ready: impl Fn(&[String], ReplicationInfo) -> bool,
        ) -> TestResult {
            timeout(Duration::from_secs(10), async {
                loop {
                    let rows: Vec<String> = self.server.fetch_all(query.clone()).await?;
                    if ready(&rows, self.replication.info()) {
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
            let first_transaction = fixture.replication.info().last_transaction;

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

            if fixture.replication.info().missed_rows.counts().1 == 0 {
                return Err("missed update count was lost during reconnect".into());
            }

            missed_rows_cause_missed_update(fixture, 3, 4).await?;

            fixture.stop().await?;
            if fixture.replication.info().missed_rows.counts().1 < 2 {
                return Err("missed updates did not accumulate across reconnect".into());
            }
            Ok(())
        })
        .await
    }
}
