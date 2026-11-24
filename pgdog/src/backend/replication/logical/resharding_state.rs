use futures::future::try_join_all;
use futures::prelude::future::join_all;
use parking_lot::Mutex;
use pgdog_stats::{CommandKind, Databases, ReshardingDirection, ReshardingId, TaskId};
use std::{collections::HashMap, fmt::Display, sync::Arc, time::Duration};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::publisher::{Permanent, ReplicationSlot, Table, permanent_slot_name};
use super::run::{
    ConfigSnapshot, FORMAT_VERSION, ReshardingCommand, ReshardingKey, ReshardingRecord,
    ReshardingStore, Stage, TaskOwner, existing_slots,
};
use super::subscriber::replication_origin::ReplicationOrigin;
use super::tables_sync::tables_sync;
use super::*;
use crate::backend::Error as BackendError;
use crate::backend::pool::{Address, LbRequest};
use crate::config::config;
use crate::tasks;
use crate::util::safe_timeout;
use crate::{backend::Cluster, util::random_string};

/// Everything a resharding run shares between its tasks: the two clusters it
/// moves data between and the publication state behind a lock.
#[derive(Debug, Clone)]
pub(crate) struct ReshardingState {
    /// Cluster the data is copied from.
    pub(crate) source: Cluster,
    /// Cluster the data is copied to.
    pub(crate) destination: Cluster,
    /// Name of the publication.
    pub(crate) publication: String,
    /// Publication state shared by every task of this run.
    inner: Arc<Mutex<ReshardingStateInner>>,
    /// Slot name actually used: the supplied one or a generated one.
    slot_name: String,
    cleanup_on_cancel: bool,
    resume: bool,
}

#[derive(Debug, Default)]
struct ReshardingStateInner {
    /// Shard -> Tables mapping.
    tables: HashMap<usize, Vec<Table>>,
    /// Shard -> Replication slot mapping.
    slots: HashMap<usize, ReplicationSlot<Permanent>>,
    origins: Origins,
    direction: ReshardingDirection,
    run: Option<RecordedRun>,
}

#[derive(Debug)]
struct RecordedRun {
    store: Arc<ReshardingStore>,
    id: ReshardingId,
    stage: Stage,
    keep_on_stop: bool,
}

type Slots = HashMap<usize, ReplicationSlot<Permanent>>;
type Origins = HashMap<usize, Vec<ReplicationOrigin>>;

impl ReshardingStateInner {
    /// Take every slot out of this state.
    fn take_slots(&mut self) -> Slots {
        std::mem::take(&mut self.slots)
    }

    /// Take the slots out that was created by this state.
    fn take_owned_slots(&mut self) -> Slots {
        let mut slots = self.take_slots();
        slots.retain(|_, slot| !slot.existing());

        slots
    }

    /// Take every origin out of this state.
    fn take_origins(&mut self) -> Origins {
        std::mem::take(&mut self.origins)
    }

    fn keeps_slots(&self) -> bool {
        self.run
            .as_ref()
            .is_some_and(|run| run.stage > Stage::Initializing && run.keep_on_stop)
    }
}

impl Drop for ReshardingStateInner {
    fn drop(&mut self) {
        if self.keeps_slots() {
            return;
        }
        if self.slots.is_empty() && self.origins.is_empty() && self.run.is_none() {
            return;
        }
        let slots = self.take_owned_slots();
        let origins = self.take_origins();
        let run = self.run.take();
        tasks::spawn("resharding run cleanup", async move {
            if let Err(err) = close(run, slots, origins).await {
                warn!("resharding cleanup failed: {err}");
            }
        });
    }
}

#[bon::bon]
impl ReshardingState {
    #[builder(start_fn = builder, finish_fn = build)]
    pub(crate) fn init(
        source: &str,
        destination: &str,
        publication: &str,
        replication_slot: Option<String>,
        #[builder(default = true)] cleanup_on_cancel: bool,
        #[builder(default)] resume: bool,
    ) -> Result<Self, Error> {
        let slot_name = replication_slot
            .unwrap_or(format!("__pgdog_repl_{}", random_string(19).to_lowercase()));

        Ok(Self {
            source: databases().schema_owner(source)?,
            destination: databases().schema_owner(destination)?,
            publication: publication.to_owned(),
            inner: Arc::new(Mutex::new(ReshardingStateInner::default())),
            slot_name,
            cleanup_on_cancel,
            resume,
        })
    }
}

impl ReshardingState {
    /// Reload source/dest clusters.
    pub(crate) fn reload(&mut self) -> Result<(), Error> {
        self.source = databases().schema_owner(&self.source.identifier().database)?;
        self.destination = databases().schema_owner(&self.destination.identifier().database)?;
        Ok(())
    }

    pub(crate) fn replication_slot(&self) -> &str {
        &self.slot_name
    }

    pub(crate) fn databases(&self) -> Databases {
        Databases {
            source: self.source.identifier().database.clone(),
            destination: self.destination.identifier().database.clone(),
        }
    }

    pub(crate) fn direction(&self) -> ReshardingDirection {
        self.inner.lock().direction
    }

    pub(crate) fn flip_direction(&self) {
        let mut inner = self.inner.lock();
        inner.direction = match inner.direction {
            ReshardingDirection::Forward => ReshardingDirection::Reverse,
            ReshardingDirection::Reverse => ReshardingDirection::Forward,
        };
    }

    pub(crate) fn tables(&self) -> HashMap<usize, Vec<Table>> {
        self.inner.lock().tables.clone()
    }

    pub(crate) fn pop_tables(&self, shard: usize) -> Result<Vec<Table>, Error> {
        self.inner
            .lock()
            .tables
            .remove(&shard)
            .ok_or_else(|| Error::NoReplicationTables(shard))
    }

    pub(crate) fn set_tables(&self, tables: HashMap<usize, Vec<Table>>) {
        self.inner.lock().tables = tables;
    }
    /// Synchronize tables for all shards from the source publication,
    /// carrying over the LSN of every table already known.
    pub(crate) async fn sync_tables(&self) -> Result<(), Error> {
        let mut tables = tables_sync(
            &self.source,
            self.source.sharded_tables(),
            &self.publication,
        )
        .await?;

        let mut inner = self.inner.lock();

        for (shard, tables) in &mut tables {
            for table in tables {
                let existing = inner
                    .tables
                    .get(shard)
                    .and_then(|tables| tables.iter().find(|t| table.key_ref() == t.key_ref()));
                if let Some(existing) = existing {
                    table.lsn = existing.lsn;
                }
            }
        }

        inner.tables = tables;

        Ok(())
    }

    pub(crate) fn slot(&self, shard: usize) -> Result<ReplicationSlot<Permanent>, Error> {
        self.inner
            .lock()
            .slots
            .get(&shard)
            .cloned()
            .ok_or_else(|| Error::NoReplicationSlot(shard))
    }

    pub(crate) fn origins(&self, shard: usize) -> Result<Vec<ReplicationOrigin>, Error> {
        self.inner
            .lock()
            .origins
            .get(&shard)
            .cloned()
            .ok_or_else(|| Error::NoReplicationOrigins(shard))
    }

    /// Create permanent slots for each shard of the source.
    ///
    /// N.B.: These are not synchronized across multiple shards.
    /// If you're doing a cross-shard transaction, parts of it can be lost.
    ///
    /// TODO: Add support for 2-phase commit.
    pub(crate) async fn create_slots(&self, cancel: &CancellationToken) -> Result<(), Error> {
        self.create_slots_on(&self.source, cancel).await
    }

    /// End the run, then create the slots a rollback would replicate from, on the destination.
    pub(crate) async fn prepare_reverse_replication(
        &self,
        cancel: &CancellationToken,
    ) -> Result<(), Error> {
        self.finish_run().await?;
        if let Err(err) = Box::pin(self.drop_slots()).await {
            warn!(
                "resharding {}: forward slot cleanup failed, cutover continues: {err}; drop the remaining slots \"{}_<shard>\" on the source and origins \"__pgdog_origin_{}_<shard>\" on the destination manually",
                self.run_key(),
                self.slot_name,
                self.slot_name,
            );
        }
        self.create_slots_on(&self.destination, cancel).await
    }

    async fn create_slots_on(
        &self,
        cluster: &Cluster,
        cancel: &CancellationToken,
    ) -> Result<(), Error> {
        if !self.inner.lock().slots.is_empty() {
            return Err(Error::SlotsAlreadyCreated(self.slot_name.clone()));
        }
        let (publication, slot_name) = (&self.publication, &self.slot_name);

        if cancel.is_cancelled() {
            return Err(Error::ReplicationAborted);
        }

        join_all(
            cluster
                .shards()
                .iter()
                .enumerate()
                .map(|(number, shard)| async move {
                    let addr = shard.primary(&LbRequest::default()).await?.addr().clone();

                    let slot = ReplicationSlot::new_permanent(
                        publication,
                        &addr,
                        Some(slot_name.clone()),
                        number,
                    );
                    slot.mark_existing();
                    self.inner.lock().slots.insert(number, slot.clone());

                    slot.reuse_or_create().await?;
                    Ok(())
                }),
        )
        .await
        .into_iter()
        .collect()
    }

    async fn create_origins(&self) -> Result<(), Error> {
        let slots: Vec<(usize, String)> = self
            .inner
            .lock()
            .slots
            .iter()
            .map(|(number, slot)| (*number, slot.name().to_owned()))
            .collect();

        for (number, origins) in self.origins_for(slots).await? {
            self.inner.lock().origins.insert(number, origins.clone());
            try_join_all(origins.iter().map(ReplicationOrigin::recreate)).await?;
        }

        Ok(())
    }

    async fn origins_for(&self, slots: Vec<(usize, String)>) -> Result<Origins, Error> {
        let mut addresses = vec![];
        for shard in self.destination.shards() {
            addresses.push(shard.primary(&LbRequest::default()).await?.addr().clone());
        }

        Ok(slots
            .into_iter()
            .map(|(number, slot_name)| {
                let origins = addresses
                    .iter()
                    .enumerate()
                    .map(|(shard, address)| {
                        ReplicationOrigin::builder()
                            .name(&slot_name)
                            .address(address)
                            .shard(shard)
                            .build()
                    })
                    .collect();
                (number, origins)
            })
            .collect())
    }

    pub(crate) async fn prepare_replication(
        &self,
        cancel: &CancellationToken,
    ) -> Result<(), Error> {
        self.sync_tables().await?;

        if self.inner.lock().slots.is_empty() {
            self.create_slots(cancel).await?;
        }

        let has_origins = {
            let inner = self.inner.lock();
            inner
                .slots
                .keys()
                .all(|number| inner.origins.contains_key(number))
        };

        if !has_origins {
            self.create_origins().await?;
        }

        self.set_stage(Stage::Replication).await
    }

    /// Remove every slot this state holds, and its origins.
    pub(crate) async fn drop_slots(&self) -> Result<(), Error> {
        let (slots, origins) = {
            let mut inner = self.inner.lock();
            (inner.take_slots(), inner.take_origins())
        };
        remove_objects(slots, origins).await
    }

    /// Forget the slots without removing them from the backend, and remove
    /// the origins.
    pub(crate) fn detach_slots(&self) {
        let origins = {
            let mut inner = self.inner.lock();
            inner.take_slots();
            inner.take_origins()
        };
        remove_origins(origins);
    }

    pub(crate) fn cleanup_on_cancel(&self) -> bool {
        self.cleanup_on_cancel
    }

    pub(crate) async fn open_run(
        &self,
        command: ReshardingCommand,
        task_id: TaskId,
    ) -> Result<(), Error> {
        let store = self.lock_run().await?;
        let row = if self.resume {
            Box::pin(self.resume_run(&store, command, task_id)).await
        } else {
            Box::pin(self.start_new_run(&store, command, task_id)).await
        };
        Box::pin(self.accept_run(store, row, task_id, self.resume)).await
    }

    async fn lock_run(&self) -> Result<ReshardingStore, Error> {
        ReshardingStore::open(&self.state_address().await?, self.run_key()).await
    }

    async fn start_new_run(
        &self,
        store: &ReshardingStore,
        command: ReshardingCommand,
        task_id: TaskId,
    ) -> Result<ReshardingRecord, Error> {
        let key = store.key();
        if let Some(record) = store.load().await? {
            return Err(Error::ReshardingExists {
                key: key.to_string(),
                stage: record.stage.to_string(),
                command: record.command.kind(),
            });
        }
        let existing = existing_slots(&self.source, &self.publication, &self.slot_name)
            .await?
            .len();
        if existing != 0 && existing != self.source.shards().len() {
            return Err(Error::ReshardingSlotsPartial(self.slot_name.clone()));
        }
        let record = ReshardingRecord {
            id: ReshardingId::generate(),
            key: key.clone(),
            command,
            slot_name: self.slot_name.clone(),
            stage: Stage::Initializing,
            config: self.config_snapshot()?,
            owner: TaskOwner::current(task_id),
            format_version: FORMAT_VERSION,
            started_at: std::time::SystemTime::now(),
            resumed_at: None,
        };
        store.insert(&record).await?;
        Ok(record)
    }

    async fn resume_run(
        &self,
        store: &ReshardingStore,
        command: ReshardingCommand,
        task_id: TaskId,
    ) -> Result<ReshardingRecord, Error> {
        let key = store.key();
        let record = store
            .load()
            .await?
            .ok_or_else(|| Error::NoResharding(key.to_string()))?;
        self.check_resumed(&record, command, &self.config_snapshot()?)?;
        Self::resumable(&record)?;
        let (slots, origins) = futures::try_join!(
            self.load_slots(&record.slot_name),
            self.load_origins(&record.slot_name)
        )?;
        store
            .resume(
                record.id,
                &TaskOwner::current(task_id),
                std::time::SystemTime::now(),
            )
            .await?;
        let mut inner = self.inner.lock();
        inner.slots = slots;
        inner.origins = origins;
        drop(inner);
        Ok(record)
    }

    async fn load_slots(&self, slot_name: &str) -> Result<Slots, Error> {
        try_join_all(
            self.source
                .shards()
                .iter()
                .enumerate()
                .map(|(number, shard)| async move {
                    let addr = shard.primary(&LbRequest::default()).await?.addr().clone();
                    let slot = ReplicationSlot::new_permanent(
                        &self.publication,
                        &addr,
                        Some(slot_name.to_owned()),
                        number,
                    );
                    slot.verify_exists().await?;
                    Ok::<_, Error>((number, slot))
                }),
        )
        .await
        .map(Slots::from_iter)
    }

    async fn load_origins(&self, slot_name: &str) -> Result<Origins, Error> {
        let names = (0..self.source.shards().len())
            .map(|number| (number, permanent_slot_name(slot_name, number)))
            .collect();
        let origins = self.origins_for(names).await?;
        try_join_all(
            origins
                .values()
                .flatten()
                .map(ReplicationOrigin::verify_exists),
        )
        .await?;
        Ok(origins)
    }

    fn resumable(record: &ReshardingRecord) -> Result<(), Error> {
        if record.stage < Stage::Replication {
            return Err(Error::ReshardingNotResumable {
                key: record.key.to_string(),
                stage: record.stage.to_string(),
            });
        }
        match record.command {
            ReshardingCommand::Replicate { .. }
            | ReshardingCommand::Reshard {
                replicate_only: true,
                ..
            } => Ok(()),
            ReshardingCommand::Reshard { .. } => {
                Err(Error::ReshardingCopyNotResumable(record.key.to_string()))
            }
        }
    }

    async fn accept_run(
        &self,
        store: ReshardingStore,
        row: Result<ReshardingRecord, Error>,
        task_id: TaskId,
        resumed: bool,
    ) -> Result<(), Error> {
        let key = store.key();
        let ReshardingRecord { id, stage, .. } = match row {
            Ok(record) => record,
            Err(err) => {
                warn!("resharding {key}: refused to start: {err}");
                unlock(&store).await;
                return Err(err);
            }
        };
        info!(
            "resharding {key}: {} (stage {stage})",
            if resumed { "resumed" } else { "started" },
        );
        run::scan::check_other_runs(store.address(), key).await;

        let store = Arc::new(store);
        store.start_heartbeat(task_id);
        self.inner.lock().run = Some(RecordedRun {
            store,
            id,
            stage,
            keep_on_stop: resumed || !self.cleanup_on_cancel,
        });
        Ok(())
    }

    fn config_snapshot(&self) -> Result<ConfigSnapshot, Error> {
        ConfigSnapshot::capture(&self.source, &self.destination, &config().config)
    }

    fn check_resumed(
        &self,
        record: &ReshardingRecord,
        command: ReshardingCommand,
        config: &ConfigSnapshot,
    ) -> Result<(), RecordMismatch> {
        if record.command != command {
            return Err(RecordMismatch::Command {
                stored: command_text(&record.command),
                given: command_text(&command),
            });
        }
        if record.slot_name != self.slot_name {
            return Err(RecordMismatch::Slot {
                stored: record.slot_name.clone(),
                given: self.slot_name.clone(),
            });
        }
        record
            .config
            .validate_equal(config)
            .map_err(RecordMismatch::Config)
    }

    pub(crate) fn has_run(&self) -> bool {
        self.inner.lock().run.is_some()
    }

    pub(crate) async fn set_stage(&self, stage: Stage) -> Result<(), Error> {
        let (store, id) = match &self.inner.lock().run {
            Some(run) if stage > run.stage => (run.store.clone(), run.id),
            _ => return Ok(()),
        };
        store.set_stage(id, stage).await?;
        if let Some(run) = self.inner.lock().run.as_mut() {
            run.stage = stage;
        }
        Ok(())
    }

    pub(crate) async fn cancel_run(&self) -> Result<(), Error> {
        let (keep, slots, origins, run) = {
            let mut inner = self.inner.lock();
            let keep = inner.keeps_slots();
            let slots = if keep {
                inner.take_slots()
            } else {
                inner.take_owned_slots()
            };
            (keep, slots, inner.take_origins(), inner.run.take())
        };
        if keep {
            if let Some(RecordedRun { store, .. }) = run {
                unlock(&store).await;
                info!(
                    "resharding {} paused, replication slots and origins kept",
                    store.key()
                );
            }
            return Ok(());
        }
        close(run, slots, origins).await
    }

    pub(crate) async fn complete_run(&self) -> Result<(), Error> {
        let (slots, origins, run) = {
            let mut inner = self.inner.lock();
            (inner.take_slots(), inner.take_origins(), inner.run.take())
        };
        close(run, slots, origins).await
    }

    pub(crate) async fn finish_run(&self) -> Result<(), Error> {
        let run = self
            .inner
            .lock()
            .run
            .as_ref()
            .map(|run| (run.store.clone(), run.id));
        let Some((store, id)) = run else {
            return Ok(());
        };
        store.delete(id).await?;
        self.inner.lock().run = None;
        unlock(&store).await;
        Ok(())
    }

    pub(crate) fn run_key(&self) -> ReshardingKey {
        ReshardingKey::new(self.databases(), &self.publication)
    }

    async fn state_address(&self) -> Result<Address, Error> {
        run::state_address(&self.destination).await
    }

    pub(crate) async fn check_record(
        &self,
        command: CommandKind,
        slot: Option<&str>,
    ) -> Result<ReshardingRecord, Error> {
        let key = self.run_key();
        let mut server = ReshardingStore::connect(&self.state_address().await?).await?;
        let record = ReshardingStore::read(&mut server, &key)
            .await?
            .ok_or_else(|| Error::NoResharding(key.to_string()))?;
        record_matches(&record, command, slot)?;
        if let Some(owner) = ReshardingStore::lock_owner(&mut server, &key).await? {
            return Err(Error::ReshardingLocked {
                key: key.to_string(),
                owner,
            });
        }
        Ok(record)
    }

    #[allow(dead_code)]
    pub(crate) async fn check_resume(
        &mut self,
        command: CommandKind,
        slot: Option<&str>,
    ) -> Result<ReshardingRecord, Error> {
        let record = self.check_record(command, slot).await?;
        Self::resumable(&record)?;
        self.slot_name = record.slot_name.clone();
        Ok(record)
    }

    pub(crate) async fn abort_run(
        &self,
        command: CommandKind,
        slot: Option<&str>,
    ) -> Result<ReshardingRecord, Error> {
        let store = self.lock_run().await?;
        let result = async {
            let key = store.key();
            let record = store
                .load()
                .await?
                .ok_or_else(|| Error::NoResharding(key.to_string()))?;
            record_matches(&record, command, slot)?;
            let slots = existing_slots(&self.source, &self.publication, &record.slot_name).await?;
            let names = (0..self.source.shards().len())
                .map(|number| (number, permanent_slot_name(&record.slot_name, number)))
                .collect();
            let origins = self.origins_for(names).await?;
            remove_objects(slots, origins).await?;
            store.delete(record.id).await?;
            Ok(record)
        }
        .await;
        unlock(&store).await;
        result
    }
}

fn command_text(command: &ReshardingCommand) -> String {
    serde_json::to_string(command).expect("command is valid JSON")
}

fn record_matches(
    record: &ReshardingRecord,
    command: CommandKind,
    slot: Option<&str>,
) -> Result<(), RecordMismatch> {
    if record.command.kind() != command {
        return Err(RecordMismatch::Command {
            stored: record.command.to_string(),
            given: command.to_string(),
        });
    }
    match slot {
        Some(slot) if slot != record.slot_name => Err(RecordMismatch::Slot {
            stored: record.slot_name.clone(),
            given: slot.to_owned(),
        }),
        _ => Ok(()),
    }
}

async fn unlock(store: &ReshardingStore) {
    if let Err(err) = store.unlock().await {
        warn!(
            "resharding {}: failed to release the lock: {err}",
            store.key()
        );
    }
}

async fn remove_objects(slots: Slots, origins: Origins) -> Result<(), Error> {
    let slots = remove_slots(slots);
    let origins = remove_origins(origins);
    slots.await?.and(origins.await?)
}

/// Drop the slots and origins, then the run's row, and release its lock.
async fn close(run: Option<RecordedRun>, slots: Slots, origins: Origins) -> Result<(), Error> {
    let result = async {
        remove_objects(slots, origins).await?;
        if let Some(run) = &run {
            run.store.delete(run.id).await?;
        }
        Ok(())
    }
    .await;
    if let Some(run) = run {
        unlock(&run.store).await;
    }
    result
}

fn remove_slots(slots: Slots) -> JoinHandle<Result<(), Error>> {
    tasks::spawn("replication slot cleanup", async move {
        let timeout =
            Duration::from_millis(config().config.resharding.replication_slot_drop_timeout);
        let removals = slots.into_values().map(|slot| async move {
            let slot_name = slot.name().to_owned();
            match safe_timeout(timeout, slot.drop_slot()).await {
                Ok(Err(Error::Backend(BackendError::ExecutionError(error))))
                    if error.code == "42704" =>
                {
                    warn!("replication slot \"{slot_name}\" was already gone");
                    Ok(())
                }
                Ok(result) => result,
                Err(_) => Err(Error::SlotDropTimeout(slot_name)),
            }
        });

        let result: Result<Vec<_>, _> = join_all(removals).await.into_iter().collect();
        if let Err(err) = &result {
            warn!("failed to drop replication slots: {err}");
        }

        result.map(|_| ())
    })
}

fn remove_origins(origins: Origins) -> JoinHandle<Result<(), Error>> {
    tasks::spawn("replication origin cleanup", async move {
        let removals = origins
            .into_values()
            .flatten()
            .map(|origin| async move { origin.drop_origin().await });

        let result: Result<Vec<_>, _> = join_all(removals).await.into_iter().collect();
        if let Err(err) = &result {
            warn!("failed to drop replication origins: {err}");
        }

        result.map(|_| ())
    })
}

impl Display for ReshardingState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> {}",
            self.source.identifier().database,
            self.destination.identifier().database
        )
    }
}

#[cfg(test)]
mod test {
    use super::super::publisher::test::{setup_publication, setup_publication_tables};
    use super::*;
    use crate::backend::Server;
    use crate::config::config;
    use pgdog_stats::TaskId;

    async fn slot_count(server: &mut Server, name: &str) -> i64 {
        let rows: Vec<i64> = server
            .fetch_all(format!(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{name}%'"
            ))
            .await
            .unwrap();
        rows[0]
    }

    async fn wait_until_removed(server: &mut Server, name: &str) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while slot_count(server, name).await != 0 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
    }

    async fn inner_with_slots(source: &Cluster, slot_name: &str) -> ReshardingStateInner {
        let mut inner = ReshardingStateInner::default();

        for (number, shard) in source.shards().iter().enumerate() {
            let addr = shard
                .primary(&LbRequest::default())
                .await
                .unwrap()
                .addr()
                .clone();
            let slot = ReplicationSlot::new_permanent(
                "publication_test",
                &addr,
                Some(slot_name.to_owned()),
                number,
            );

            slot.reuse_or_create().await.unwrap();

            inner.slots.insert(number, slot);
        }

        inner
    }

    fn state_for(source: &Cluster, slot_name: &str) -> ReshardingState {
        state_with_publication(source, slot_name, "publication_test")
    }

    fn state_with_publication(
        source: &Cluster,
        slot_name: &str,
        publication: &str,
    ) -> ReshardingState {
        ReshardingState {
            source: source.clone(),
            destination: source.clone(),
            publication: publication.into(),
            inner: Arc::new(Mutex::new(ReshardingStateInner::default())),
            slot_name: slot_name.to_owned(),
            cleanup_on_cancel: true,
            resume: false,
        }
    }

    #[tokio::test]
    async fn create_slots_aborts_on_cancelled_token() {
        crate::logger();
        let mut publication = setup_publication().await;
        let source = Cluster::new_test(&config());
        source.launch();

        let cancel = CancellationToken::new();
        cancel.cancel();

        let state = state_for(&source, "cancelled_token_slot");
        let result = state.create_slots(&cancel).await;

        assert!(
            matches!(result, Err(Error::ReplicationAborted)),
            "slot creation must abort on a cancelled token; got: {result:?}"
        );
        assert_eq!(
            slot_count(&mut publication.server, "cancelled_token_slot").await,
            0
        );

        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn prepare_replication_creates_a_missing_named_slot() {
        crate::logger();
        let mut publication = setup_publication().await;
        let name = "prepare_named_slot";
        let source = Cluster::new_test(&config());
        source.launch();

        let state = state_for(&source, name);
        state
            .prepare_replication(&CancellationToken::new())
            .await
            .unwrap();

        assert!(slot_count(&mut publication.server, name).await > 0);

        state.drop_slots().await.unwrap();
        wait_until_removed(&mut publication.server, name).await;

        source.shutdown();
        publication.cleanup().await;
    }

    async fn origin_count(server: &mut Server, name: &str) -> i64 {
        let rows: Vec<i64> = server
            .fetch_all(format!(
                "SELECT count(*) FROM pg_replication_origin WHERE roname LIKE '__pgdog_origin_{name}%'"
            ))
            .await
            .unwrap();
        rows[0]
    }

    #[tokio::test]
    async fn prepare_creates_origins_dropped_with_slots() {
        crate::logger();
        let mut publication = setup_publication().await;
        let name = "state_origins_drop";
        let source = Cluster::new_test(&config());
        source.launch();
        let shards = source.shards().len();

        let state = state_for(&source, name);
        state.create_slots(&CancellationToken::new()).await.unwrap();
        assert_eq!(origin_count(&mut publication.server, name).await, 0);

        state
            .prepare_replication(&CancellationToken::new())
            .await
            .unwrap();

        assert_eq!(state.origins(0).unwrap().len(), shards);
        assert_eq!(
            origin_count(&mut publication.server, name).await,
            (shards * shards) as i64
        );

        state.drop_slots().await.unwrap();
        assert_eq!(origin_count(&mut publication.server, name).await, 0);
        wait_until_removed(&mut publication.server, name).await;

        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn detaching_slots_drops_origins_and_keeps_slots() {
        crate::logger();
        let mut publication = setup_publication().await;
        let name = "state_origins_detach";
        let source = Cluster::new_test(&config());
        source.launch();

        let state = state_for(&source, name);
        state
            .prepare_replication(&CancellationToken::new())
            .await
            .unwrap();
        assert!(origin_count(&mut publication.server, name).await > 0);
        let slots = slot_count(&mut publication.server, name).await;
        assert!(slots > 0);

        state.detach_slots();
        tokio::time::timeout(Duration::from_secs(10), async {
            while origin_count(&mut publication.server, name).await != 0 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(slot_count(&mut publication.server, name).await, slots);

        publication
            .server
            .execute_checked(format!(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE '{name}%'"
            ))
            .await
            .unwrap();
        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn dropping_state_removes_slots_it_created() {
        crate::logger();
        let mut publication = setup_publication().await;
        let name = "state_drop_removes";
        let source = Cluster::new_test(&config());
        source.launch();

        let inner = inner_with_slots(&source, name).await;
        assert!(slot_count(&mut publication.server, name).await > 0);

        drop(inner);
        wait_until_removed(&mut publication.server, name).await;

        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn dropping_state_keeps_a_slot_it_reused() {
        crate::logger();
        let mut publication = setup_publication().await;
        let name = "state_drop_keeps";
        let source = Cluster::new_test(&config());
        source.launch();

        let owner = inner_with_slots(&source, name).await;
        let created = slot_count(&mut publication.server, name).await;
        assert!(created > 0);

        let reused = inner_with_slots(&source, name).await;
        drop(reused);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(slot_count(&mut publication.server, name).await, created);

        drop(owner);
        wait_until_removed(&mut publication.server, name).await;

        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn take_owned_slots_keeps_a_slot_it_reused() {
        crate::logger();
        let mut publication = setup_publication().await;
        let name = "state_cleanup_keeps";
        let source = Cluster::new_test(&config());
        source.launch();

        let owner = inner_with_slots(&source, name).await;
        let created = slot_count(&mut publication.server, name).await;
        assert!(created > 0);

        let mut reused = inner_with_slots(&source, name).await;
        assert!(reused.take_owned_slots().is_empty());
        assert_eq!(slot_count(&mut publication.server, name).await, created);

        drop(owner);
        wait_until_removed(&mut publication.server, name).await;

        source.shutdown();
        publication.cleanup().await;
    }

    async fn resharding_rows(server: &mut Server, name: &str) -> i64 {
        let rows: Vec<i64> = server
            .fetch_all(format!(
                "SELECT count(*) FROM pgdog.resharding WHERE slot_name = '{name}'"
            ))
            .await
            .unwrap();
        rows[0]
    }

    async fn remove_resumable(server: &mut Server, name: &str) {
        server
            .execute_checked(format!(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE '{name}%'"
            ))
            .await
            .unwrap();
        server
            .execute_checked(format!(
                "SELECT pg_replication_origin_drop(roname) FROM pg_replication_origin WHERE roname LIKE '__pgdog_origin_{name}%'"
            ))
            .await
            .unwrap();
        server
            .execute_checked(format!(
                "DO $$ BEGIN IF to_regclass('pgdog.resharding') IS NOT NULL THEN \
                DELETE FROM pgdog.resharding WHERE slot_name = '{name}'; END IF; END $$"
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn paused_resumable_run_keeps_slots_origins_and_record() {
        crate::logger();
        let mut publication = setup_publication().await;
        let name = "state_resumable_pause";
        let source = Cluster::new_test(&config());
        source.launch();
        let shards = source.shards().len() as i64;
        remove_resumable(&mut publication.server, name).await;

        let mut state = state_for(&source, name);
        state.cleanup_on_cancel = false;
        start(&state, 1).await.unwrap();
        assert!(!state.keeps_slots());
        assert_eq!(
            stored_stage(&mut publication.server, name).await,
            Stage::Initializing.to_string()
        );

        state
            .prepare_replication(&CancellationToken::new())
            .await
            .unwrap();
        assert!(state.keeps_slots());
        assert_eq!(
            stored_stage(&mut publication.server, name).await,
            Stage::Replication.to_string()
        );
        assert_eq!(resharding_rows(&mut publication.server, name).await, 1);
        state.cancel_run().await.unwrap();
        assert!(!state.keeps_slots());
        assert!(!state.has_run());
        assert_paused_state_is_empty(&state);
        drop(state);

        assert_eq!(slot_count(&mut publication.server, name).await, shards);
        assert_eq!(
            origin_count(&mut publication.server, name).await,
            shards * shards
        );
        assert_eq!(resharding_rows(&mut publication.server, name).await, 1);

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }

    const REPLICATE: ReshardingCommand = ReshardingCommand::Replicate {
        auto_cutover: false,
    };

    fn keyed_state(source: &Cluster, name: &str, cleanup_on_cancel: bool) -> ReshardingState {
        let mut state = state_with_publication(source, name, &format!("{name}_pub"));
        state.cleanup_on_cancel = cleanup_on_cancel;
        state
    }

    impl ReshardingState {
        fn keeps_slots(&self) -> bool {
            self.inner.lock().keeps_slots()
        }
    }

    fn assert_paused_state_is_empty(state: &ReshardingState) {
        let inner = state.inner.lock();
        assert!(inner.slots.is_empty());
        assert!(inner.origins.is_empty());
        assert!(inner.run.is_none());
    }

    async fn terminate_state_backend(server: &mut Server, key: &ReshardingKey) {
        let killed: Vec<String> = server
            .fetch_all(format!(
                "SELECT pg_terminate_backend(pid)::text FROM pg_locks \
                 WHERE locktype = 'advisory' AND objsubid = 2 \
                 AND objid = (hashtext('{}')::bigint & 4294967295)::oid",
                lock_name(key)
            ))
            .await
            .unwrap();
        assert_eq!(killed, ["true"]);
    }

    fn lock_name(key: &ReshardingKey) -> String {
        format!(
            "{}/{}/{}",
            key.databases.source, key.databases.destination, key.publication
        )
    }

    async fn start(state: &ReshardingState, task_id: u64) -> Result<(), Error> {
        state.open_run(REPLICATE, TaskId::new(task_id)).await
    }

    async fn resume(
        state: &ReshardingState,
        command: ReshardingCommand,
        task_id: u64,
    ) -> Result<(), Error> {
        let mut checked = state.clone();
        checked.resume = true;
        let slot = checked.slot_name.clone();
        checked.check_resume(command.kind(), Some(&slot)).await?;
        checked.open_run(command, TaskId::new(task_id)).await
    }

    async fn recreate_keyed_publication(server: &mut Server, name: &str, create: bool) {
        server
            .execute_checked(format!("DROP PUBLICATION IF EXISTS {name}_pub"))
            .await
            .unwrap();
        if create {
            server
                .execute_checked(format!(
                    "CREATE PUBLICATION {name}_pub FOR TABLE publication_test_one"
                ))
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn finish_reconnects_after_state_connection_loss() {
        crate::logger();
        let mut publication = setup_publication().await;
        let name = "state_finish_reconnects";
        let source = Cluster::new_test(&config());
        source.launch();
        remove_resumable(&mut publication.server, name).await;
        recreate_keyed_publication(&mut publication.server, name, true).await;

        let state = keyed_state(&source, name, false);
        start(&state, 1).await.unwrap();
        state
            .prepare_replication(&CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(resharding_rows(&mut publication.server, name).await, 1);

        terminate_state_backend(&mut publication.server, &state.run_key()).await;

        state.finish_run().await.unwrap();
        assert!(!state.keeps_slots());
        assert!(!state.has_run());
        assert_eq!(resharding_rows(&mut publication.server, name).await, 0);
        drop(state);
        wait_until_removed(&mut publication.server, name).await;

        remove_resumable(&mut publication.server, name).await;
        recreate_keyed_publication(&mut publication.server, name, false).await;
        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn cancel_removes_the_record_when_a_slot_is_already_gone() {
        crate::logger();
        let mut publication = setup_publication().await;
        let name = "state_slot_gone";
        let source = Cluster::new_test(&config());
        source.launch();
        remove_resumable(&mut publication.server, name).await;
        recreate_keyed_publication(&mut publication.server, name, true).await;

        let state = keyed_state(&source, name, true);
        start(&state, 1).await.unwrap();
        state
            .prepare_replication(&CancellationToken::new())
            .await
            .unwrap();
        publication
            .server
            .execute_checked(format!("SELECT pg_drop_replication_slot('{name}_0')"))
            .await
            .unwrap();

        state.cancel_run().await.unwrap();

        assert_eq!(slot_count(&mut publication.server, name).await, 0);
        assert_eq!(origin_count(&mut publication.server, name).await, 0);
        assert_eq!(resharding_rows(&mut publication.server, name).await, 0);

        recreate_keyed_publication(&mut publication.server, name, false).await;
        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn other_unfinished_runs_do_not_block_a_new_run() {
        crate::logger();
        let mut publication = setup_publication().await;
        let paused_name = "state_other_paused";
        let new_name = "state_other_new";
        let source = Cluster::new_test(&config());
        source.launch();
        for name in [paused_name, new_name] {
            remove_resumable(&mut publication.server, name).await;
        }

        let paused = keyed_state(&source, paused_name, false);
        start(&paused, 1).await.unwrap();
        paused
            .create_slots(&CancellationToken::new())
            .await
            .unwrap();
        paused.set_stage(Stage::Replication).await.unwrap();
        paused.cancel_run().await.unwrap();

        let other = keyed_state(&source, new_name, false);
        start(&other, 3).await.unwrap();
        assert_eq!(resharding_rows(&mut publication.server, new_name).await, 1);
        other.create_slots(&CancellationToken::new()).await.unwrap();
        assert_eq!(
            resharding_rows(&mut publication.server, paused_name).await,
            1
        );
        assert_eq!(resharding_rows(&mut publication.server, new_name).await, 1);
        other.finish_run().await.unwrap();

        for name in [paused_name, new_name] {
            remove_resumable(&mut publication.server, name).await;
        }
        source.shutdown();
        publication.cleanup().await;
    }

    async fn pause_after_replication(state: &ReshardingState) {
        start(state, 1).await.unwrap();
        state
            .prepare_replication(&CancellationToken::new())
            .await
            .unwrap();
        state.cancel_run().await.unwrap();
    }

    async fn abort(state: &ReshardingState) -> ReshardingRecord {
        state
            .abort_run(CommandKind::Replicate, Some(state.replication_slot()))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn abort_drops_every_slot_of_the_run() {
        crate::logger();
        let name = "state_abort";
        let mut publication =
            setup_publication_tables(&format!("{name}_pub"), &["state_abort_one"]).await;
        let source = Cluster::new_test(&config());
        source.launch();
        let shards = source.shards().len() as i64;
        remove_resumable(&mut publication.server, name).await;

        let state = keyed_state(&source, name, false);
        start(&state, 1).await.unwrap();
        state
            .prepare_replication(&CancellationToken::new())
            .await
            .unwrap();
        for locked in [
            state
                .check_record(CommandKind::Replicate, None)
                .await
                .map(drop),
            state
                .abort_run(CommandKind::Replicate, None)
                .await
                .map(drop),
        ] {
            assert!(
                matches!(locked, Err(Error::ReshardingLocked { .. })),
                "{locked:?}"
            );
        }
        state.cancel_run().await.unwrap();
        assert_eq!(slot_count(&mut publication.server, name).await, shards);

        for (command, slot) in [
            (CommandKind::Reshard, None),
            (CommandKind::Replicate, Some("other")),
        ] {
            let mismatch = state.abort_run(command, slot).await.map(drop);
            assert!(
                matches!(mismatch, Err(Error::RecordMismatch(_))),
                "{mismatch:?}"
            );
        }
        state
            .check_record(CommandKind::Replicate, None)
            .await
            .unwrap();

        abort(&state).await;
        wait_until_removed(&mut publication.server, name).await;
        assert_eq!(origin_count(&mut publication.server, name).await, 0);
        assert_eq!(resharding_rows(&mut publication.server, name).await, 0);
        assert!(matches!(
            state.check_record(CommandKind::Replicate, None).await,
            Err(Error::NoResharding(_))
        ));

        let owner = keyed_state(&source, name, true);
        owner.create_slots(&CancellationToken::new()).await.unwrap();
        owner.detach_slots();

        let state = keyed_state(&source, name, false);
        pause_after_replication(&state).await;
        abort(&state).await;
        wait_until_removed(&mut publication.server, name).await;
        assert_eq!(origin_count(&mut publication.server, name).await, 0);
        assert_eq!(resharding_rows(&mut publication.server, name).await, 0);

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn resume_state_refuses_parallel_and_unfinished_runs() {
        crate::logger();
        let mut publication = setup_publication().await;
        let name = "state_resume_refuse";
        let source = Cluster::new_test(&config());
        source.launch();
        remove_resumable(&mut publication.server, name).await;
        let state = keyed_state(&source, name, false);

        start(&state, 1).await.unwrap();
        state.create_slots(&CancellationToken::new()).await.unwrap();
        state.set_stage(Stage::Replication).await.unwrap();

        let parallel = start(&keyed_state(&source, name, false), 2).await;
        assert!(
            matches!(parallel, Err(Error::ReshardingLocked { .. })),
            "{parallel:?}"
        );

        state.cancel_run().await.unwrap();
        let unfinished = start(&keyed_state(&source, name, false), 3).await;
        assert!(
            matches!(unfinished, Err(Error::ReshardingExists { .. })),
            "{unfinished:?}"
        );
        assert_eq!(resharding_rows(&mut publication.server, name).await, 1);

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn dropped_plain_run_removes_its_record() {
        crate::logger();
        let name = "state_plain_drop";
        let mut publication =
            setup_publication_tables(&format!("{name}_pub"), &["state_plain_drop_one"]).await;
        let source = Cluster::new_test(&config());
        source.launch();
        remove_resumable(&mut publication.server, name).await;

        let state = keyed_state(&source, name, true);
        start(&state, 1).await.unwrap();
        state
            .prepare_replication(&CancellationToken::new())
            .await
            .unwrap();
        assert!(slot_count(&mut publication.server, name).await > 0);
        drop(state);

        tokio::time::timeout(Duration::from_secs(10), async {
            while resharding_rows(&mut publication.server, name).await != 0 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the row of a dropped run must be deleted");
        assert_eq!(slot_count(&mut publication.server, name).await, 0);
        assert_eq!(origin_count(&mut publication.server, name).await, 0);

        let again = keyed_state(&source, name, true);
        tokio::time::timeout(Duration::from_secs(10), async {
            while let Err(Error::ReshardingLocked { .. }) = start(&again, 2).await {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the lock of a dropped run must be released");
        assert!(again.has_run());
        again.finish_run().await.unwrap();

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }

    async fn stored_stage(server: &mut Server, name: &str) -> String {
        server
            .fetch_all::<String>(format!(
                "SELECT stage FROM pgdog.resharding WHERE slot_name = '{name}'"
            ))
            .await
            .unwrap()
            .remove(0)
    }

    #[tokio::test]
    async fn stored_stage_moves_only_forward() {
        crate::logger();
        let mut publication = setup_publication().await;
        let name = "state_stage_forward";
        let source = Cluster::new_test(&config());
        source.launch();
        remove_resumable(&mut publication.server, name).await;

        let state = keyed_state(&source, name, true);
        state.open_run(RESHARD, TaskId::new(1)).await.unwrap();
        assert_eq!(
            stored_stage(&mut publication.server, name).await,
            Stage::Initializing.to_string()
        );
        for (write, stored) in [
            (Stage::CopyTables, Stage::CopyTables),
            (Stage::Replication, Stage::Replication),
            (Stage::CopyTables, Stage::Replication),
            (Stage::Initializing, Stage::Replication),
        ] {
            state.set_stage(write).await.unwrap();
            assert_eq!(
                stored_stage(&mut publication.server, name).await,
                stored.to_string(),
                "after writing {write}"
            );
        }
        state.complete_run().await.unwrap();

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }

    const RESHARD: ReshardingCommand = ReshardingCommand::Reshard {
        skip_schema_sync: false,
        replicate_only: false,
        sync_only: false,
        auto_cutover: false,
        tables_only: false,
    };

    async fn origin_progress(server: &mut Server, origin: &str) -> String {
        let rows: Vec<String> = server
            .fetch_all(format!(
                "SELECT pg_replication_origin_progress('{origin}', false)::text"
            ))
            .await
            .unwrap();
        rows[0].clone()
    }

    #[tokio::test]
    async fn resume_needs_a_matching_record() {
        crate::logger();
        let name = "state_resume_match";
        let mut publication = keyed_publication(name).await;
        let source = Cluster::new_test(&config());
        source.launch();
        remove_resumable(&mut publication.server, name).await;

        let missing = resume(&keyed_state(&source, name, false), REPLICATE, 1).await;
        assert!(
            matches!(missing, Err(Error::NoResharding(_))),
            "{missing:?}"
        );

        let state = keyed_state(&source, name, false);
        start(&state, 1).await.unwrap();
        state
            .prepare_replication(&CancellationToken::new())
            .await
            .unwrap();
        state.cancel_run().await.unwrap();

        let other_command = resume(
            &keyed_state(&source, name, false),
            ReshardingCommand::Replicate { auto_cutover: true },
            2,
        )
        .await;
        assert!(
            matches!(
                other_command,
                Err(Error::RecordMismatch(RecordMismatch::Command { .. }))
            ),
            "{other_command:?}"
        );

        let mut other_slot = keyed_state(&source, name, false);
        other_slot.slot_name = "other".into();
        let other_slot = resume(&other_slot, REPLICATE, 3).await;
        assert!(
            matches!(
                other_slot,
                Err(Error::RecordMismatch(RecordMismatch::Slot { .. }))
            ),
            "{other_slot:?}"
        );

        let resumed = keyed_state(&source, name, false);
        resume(&resumed, REPLICATE, 7).await.unwrap();
        let owners: Vec<i64> = publication
            .server
            .fetch_all(format!(
                "SELECT task_id FROM pgdog.resharding \
                 WHERE slot_name = '{name}' AND resumed_at IS NOT NULL"
            ))
            .await
            .unwrap();
        assert_eq!(owners, [7]);
        resumed.finish_run().await.unwrap();
        assert_eq!(resharding_rows(&mut publication.server, name).await, 0);

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn checked_resume_uses_the_stored_slot() {
        crate::logger();
        let name = "state_check_resume";
        let mut publication = keyed_publication(name).await;
        let source = Cluster::new_test(&config());
        source.launch();
        remove_resumable(&mut publication.server, name).await;
        let cancel = CancellationToken::new();

        let state = keyed_state(&source, name, false);
        start(&state, 1).await.unwrap();
        state.prepare_replication(&cancel).await.unwrap();
        state.cancel_run().await.unwrap();

        let mut wrong_slot = keyed_state(&source, name, false);
        let mismatch = wrong_slot
            .check_resume(CommandKind::Replicate, Some("other"))
            .await;
        assert!(
            matches!(
                mismatch,
                Err(Error::RecordMismatch(RecordMismatch::Slot { .. }))
            ),
            "{mismatch:?}"
        );

        let mut checked = keyed_state(&source, name, false);
        checked.slot_name = "other".into();
        checked.resume = true;
        checked
            .check_resume(CommandKind::Replicate, None)
            .await
            .unwrap();
        assert_eq!(checked.replication_slot(), name);
        checked.open_run(REPLICATE, TaskId::new(2)).await.unwrap();
        assert!(checked.has_run());
        checked.cancel_run().await.unwrap();
        assert_eq!(resharding_rows(&mut publication.server, name).await, 1);

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn resumed_run_loads_slots_and_never_recreates_them() {
        crate::logger();
        let name = "state_resume_load";
        let mut publication =
            setup_publication_tables(&format!("{name}_pub"), &["state_resume_load_one"]).await;
        let source = Cluster::new_test(&config());
        source.launch();
        let shards = source.shards().len() as i64;
        remove_resumable(&mut publication.server, name).await;
        let cancel = CancellationToken::new();

        let state = keyed_state(&source, name, false);
        start(&state, 1).await.unwrap();
        state.prepare_replication(&cancel).await.unwrap();
        state.cancel_run().await.unwrap();

        let resumed = keyed_state(&source, name, false);
        resume(&resumed, REPLICATE, 2).await.unwrap();
        for number in 0..source.shards().len() {
            assert!(resumed.slot(number).unwrap().existing());
            assert_eq!(
                resumed.origins(number).unwrap().len(),
                source.shards().len()
            );
        }
        resumed.prepare_replication(&cancel).await.unwrap();
        assert_eq!(slot_count(&mut publication.server, name).await, shards);
        resumed.cancel_run().await.unwrap();
        assert_eq!(slot_count(&mut publication.server, name).await, shards);
        assert_eq!(resharding_rows(&mut publication.server, name).await, 1);

        publication
            .server
            .execute_checked(format!("SELECT pg_drop_replication_slot('{name}_0')"))
            .await
            .unwrap();
        for task_id in [3, 4] {
            let resumed = keyed_state(&source, name, false);
            let missing_slot = resume(&resumed, REPLICATE, task_id).await;
            assert!(
                matches!(missing_slot, Err(Error::MissingReplicationSlot(_))),
                "{missing_slot:?}"
            );
            assert!(!resumed.has_run());
            assert_paused_state_is_empty(&resumed);
            assert_eq!(slot_count(&mut publication.server, name).await, shards - 1);
            assert_eq!(resharding_rows(&mut publication.server, name).await, 1);
        }

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn resumed_run_needs_every_origin_and_keeps_progress() {
        crate::logger();
        let name = "state_resume_reuse";
        let mut publication =
            setup_publication_tables(&format!("{name}_pub"), &["state_resume_reuse_one"]).await;
        let source = Cluster::new_test(&config());
        source.launch();
        let shards = source.shards().len() as i64;
        let last = shards - 1;
        remove_resumable(&mut publication.server, name).await;
        let kept = format!("__pgdog_origin_{name}_0_0");
        let lost = format!("__pgdog_origin_{name}_{last}_{last}");
        let cancel = CancellationToken::new();

        let state = keyed_state(&source, name, false);
        start(&state, 1).await.unwrap();
        state.prepare_replication(&cancel).await.unwrap();
        state.cancel_run().await.unwrap();
        publication
            .server
            .execute_checked(format!(
                "SELECT pg_replication_origin_advance('{kept}', '0/2000')"
            ))
            .await
            .unwrap();

        let resumed = keyed_state(&source, name, false);
        resume(&resumed, REPLICATE, 2).await.unwrap();
        assert_eq!(
            origin_progress(&mut publication.server, &kept).await,
            "0/2000"
        );
        resumed.cancel_run().await.unwrap();

        publication
            .server
            .execute_checked(format!("SELECT pg_replication_origin_drop('{lost}')"))
            .await
            .unwrap();
        for task_id in [3, 4] {
            let refused = keyed_state(&source, name, false);
            let missing_origin = resume(&refused, REPLICATE, task_id).await;
            assert!(
                matches!(missing_origin, Err(Error::MissingReplicationOrigin(_))),
                "{missing_origin:?}"
            );
            assert!(!refused.has_run());
            assert_eq!(
                origin_count(&mut publication.server, name).await,
                shards * shards - 1
            );
            assert_eq!(resharding_rows(&mut publication.server, name).await, 1);
        }

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }

    async fn keyed_publication(name: &str) -> super::super::publisher::test::PublicationTest {
        setup_publication_tables(&format!("{name}_pub"), &[&format!("{name}_one")]).await
    }

    async fn wait_for_lock(server: &mut Server, key: &ReshardingKey) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let locked: Vec<String> = server
                    .fetch_all(format!(
                        "SELECT pg_try_advisory_lock(hashtext('pgdog.resharding'), hashtext('{}'))::text",
                        lock_name(key)
                    ))
                    .await
                    .unwrap();
                if locked == ["true"] {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the lock of a terminated backend must be released");
    }

    async fn release_lock(server: &mut Server, key: &ReshardingKey) {
        server
            .execute_checked(format!(
                "SELECT pg_advisory_unlock(hashtext('pgdog.resharding'), hashtext('{}'))",
                lock_name(key)
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cancel_after_lost_state_connection_removes_the_record() {
        crate::logger();
        let name = "state_lost_cancel";
        let mut publication = keyed_publication(name).await;
        let source = Cluster::new_test(&config());
        source.launch();
        remove_resumable(&mut publication.server, name).await;

        let state = keyed_state(&source, name, true);
        start(&state, 1).await.unwrap();
        state
            .prepare_replication(&CancellationToken::new())
            .await
            .unwrap();
        terminate_state_backend(&mut publication.server, &state.run_key()).await;

        state.cancel_run().await.unwrap();
        assert!(!state.has_run());
        assert_eq!(slot_count(&mut publication.server, name).await, 0);
        assert_eq!(origin_count(&mut publication.server, name).await, 0);
        assert_eq!(resharding_rows(&mut publication.server, name).await, 0);

        let again = keyed_state(&source, name, true);
        start(&again, 2).await.unwrap();
        again.finish_run().await.unwrap();

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn cancel_keeps_the_record_when_another_session_takes_the_lock() {
        crate::logger();
        let name = "state_lost_taken";
        let mut publication = keyed_publication(name).await;
        let source = Cluster::new_test(&config());
        source.launch();
        remove_resumable(&mut publication.server, name).await;

        let state = keyed_state(&source, name, true);
        let key = state.run_key();
        start(&state, 1).await.unwrap();
        state
            .prepare_replication(&CancellationToken::new())
            .await
            .unwrap();
        terminate_state_backend(&mut publication.server, &key).await;
        wait_for_lock(&mut publication.server, &key).await;

        let cancelled = state.cancel_run().await;
        assert!(
            matches!(cancelled, Err(Error::ReshardingLocked { .. })),
            "{cancelled:?}"
        );
        assert!(!state.has_run());
        assert_eq!(slot_count(&mut publication.server, name).await, 0);
        assert_eq!(origin_count(&mut publication.server, name).await, 0);
        assert_eq!(resharding_rows(&mut publication.server, name).await, 1);

        release_lock(&mut publication.server, &key).await;
        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn complete_run_drops_slots_and_origins_then_the_record() {
        crate::logger();
        let name = "state_complete";
        let mut publication = keyed_publication(name).await;
        let source = Cluster::new_test(&config());
        source.launch();
        remove_resumable(&mut publication.server, name).await;

        let state = keyed_state(&source, name, false);
        start(&state, 1).await.unwrap();
        state
            .prepare_replication(&CancellationToken::new())
            .await
            .unwrap();

        state.complete_run().await.unwrap();
        assert!(!state.keeps_slots());
        assert_paused_state_is_empty(&state);
        assert_eq!(slot_count(&mut publication.server, name).await, 0);
        assert_eq!(origin_count(&mut publication.server, name).await, 0);
        assert_eq!(resharding_rows(&mut publication.server, name).await, 0);

        let again = keyed_state(&source, name, false);
        start(&again, 2).await.unwrap();
        again.finish_run().await.unwrap();

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn user_slots_stay_on_cancel_of_a_live_run_and_of_a_resumed_run() {
        crate::logger();
        let name = "state_user_slots_stay";
        let mut publication = keyed_publication(name).await;
        let source = Cluster::new_test(&config());
        source.launch();
        let shards = source.shards().len() as i64;
        remove_resumable(&mut publication.server, name).await;
        let cancel = CancellationToken::new();

        let owner = keyed_state(&source, name, true);
        owner.create_slots(&cancel).await.unwrap();
        owner.detach_slots();

        let fresh = keyed_state(&source, name, true);
        start(&fresh, 1).await.unwrap();
        fresh.prepare_replication(&cancel).await.unwrap();
        assert!(fresh.inner.lock().take_owned_slots().is_empty());
        fresh.cancel_run().await.unwrap();
        assert_eq!(slot_count(&mut publication.server, name).await, shards);
        assert_eq!(origin_count(&mut publication.server, name).await, 0);
        assert_eq!(resharding_rows(&mut publication.server, name).await, 0);

        let paused = keyed_state(&source, name, false);
        start(&paused, 2).await.unwrap();
        paused.prepare_replication(&cancel).await.unwrap();
        paused.cancel_run().await.unwrap();

        let resumed = keyed_state(&source, name, true);
        resume(&resumed, REPLICATE, 3).await.unwrap();
        resumed.prepare_replication(&cancel).await.unwrap();
        resumed.cancel_run().await.unwrap();
        assert_eq!(slot_count(&mut publication.server, name).await, shards);
        assert_eq!(resharding_rows(&mut publication.server, name).await, 1);

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn partial_user_slots_refuse_a_new_run() {
        crate::logger();
        let mut publication = setup_publication().await;
        let name = "state_partial_slots";
        let source = Cluster::new_test(&config());
        source.launch();
        remove_resumable(&mut publication.server, name).await;
        publication
            .server
            .execute_checked(format!(
                "SELECT pg_create_logical_replication_slot('{name}_0', 'pgoutput')"
            ))
            .await
            .unwrap();

        for task_id in [1, 2] {
            let refused = start(&keyed_state(&source, name, true), task_id).await;
            assert!(
                matches!(refused, Err(Error::ReshardingSlotsPartial(_))),
                "{refused:?}"
            );
        }
        assert_eq!(resharding_rows(&mut publication.server, name).await, 0);
        assert_eq!(slot_count(&mut publication.server, name).await, 1);

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn crashed_resumable_run_keeps_its_objects_and_resumes() {
        crate::logger();
        let name = "state_crash";
        let mut publication = keyed_publication(name).await;
        let source = Cluster::new_test(&config());
        source.launch();
        let shards = source.shards().len() as i64;
        remove_resumable(&mut publication.server, name).await;
        let cancel = CancellationToken::new();

        let state = keyed_state(&source, name, false);
        start(&state, 1).await.unwrap();
        state.prepare_replication(&cancel).await.unwrap();
        drop(state);

        let resumed = keyed_state(&source, name, false);
        tokio::time::timeout(Duration::from_secs(10), async {
            while let Err(Error::ReshardingLocked { .. }) = resume(&resumed, REPLICATE, 2).await {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the lock of a crashed run must be released");
        assert!(resumed.has_run());
        assert_eq!(slot_count(&mut publication.server, name).await, shards);
        assert_eq!(
            origin_count(&mut publication.server, name).await,
            shards * shards
        );
        assert_eq!(resharding_rows(&mut publication.server, name).await, 1);

        resumed.prepare_replication(&cancel).await.unwrap();
        resumed.cancel_run().await.unwrap();
        assert_eq!(slot_count(&mut publication.server, name).await, shards);

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn resumed_run_keeps_its_stored_stage() {
        crate::logger();
        let name = "state_resume_stage";
        let mut publication = keyed_publication(name).await;
        let source = Cluster::new_test(&config());
        source.launch();
        remove_resumable(&mut publication.server, name).await;

        let state = keyed_state(&source, name, false);
        start(&state, 1).await.unwrap();
        state
            .prepare_replication(&CancellationToken::new())
            .await
            .unwrap();
        state.cancel_run().await.unwrap();

        let resumed = keyed_state(&source, name, false);
        resume(&resumed, REPLICATE, 2).await.unwrap();
        assert_eq!(
            resumed.inner.lock().run.as_ref().map(|run| run.stage),
            Some(Stage::Replication)
        );
        resumed.finish_run().await.unwrap();

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn run_stopped_while_initializing_drops_its_slots_and_row() {
        crate::logger();
        let mut publication = setup_publication().await;
        let name = "state_stopped_initializing";
        let source = Cluster::new_test(&config());
        source.launch();
        let shards = source.shards().len() as i64;
        remove_resumable(&mut publication.server, name).await;

        let state = keyed_state(&source, name, false);
        start(&state, 1).await.unwrap();
        assert!(state.has_run());
        state.create_slots(&CancellationToken::new()).await.unwrap();
        assert_eq!(slot_count(&mut publication.server, name).await, shards);
        assert!(!state.keeps_slots());
        state.cancel_run().await.unwrap();
        assert!(!state.has_run());
        assert_eq!(slot_count(&mut publication.server, name).await, 0);
        assert_eq!(resharding_rows(&mut publication.server, name).await, 0);

        let dropped = keyed_state(&source, name, false);
        start(&dropped, 2).await.unwrap();
        drop(dropped);

        let again = keyed_state(&source, name, false);
        tokio::time::timeout(Duration::from_secs(10), async {
            while let Err(Error::ReshardingLocked { .. }) = start(&again, 3).await {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the lock of a dropped run must be released");
        assert!(again.has_run());
        assert_eq!(slot_count(&mut publication.server, name).await, 0);
        assert_eq!(resharding_rows(&mut publication.server, name).await, 1);
        again.finish_run().await.unwrap();

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn copy_tables_row_refuses_resume() {
        crate::logger();
        let mut publication = setup_publication().await;
        let name = "state_copy_resume";
        let source = Cluster::new_test(&config());
        source.launch();
        let shards = source.shards().len() as i64;
        remove_resumable(&mut publication.server, name).await;

        let state = keyed_state(&source, name, false);
        state.open_run(RESHARD, TaskId::new(1)).await.unwrap();
        state.create_slots(&CancellationToken::new()).await.unwrap();
        state.set_stage(Stage::CopyTables).await.unwrap();
        state.cancel_run().await.unwrap();
        assert_eq!(slot_count(&mut publication.server, name).await, shards);

        let resumed = keyed_state(&source, name, false);
        let refused = resume(&resumed, RESHARD, 2).await;
        assert!(
            matches!(refused, Err(Error::ReshardingNotResumable { .. })),
            "{refused:?}"
        );
        assert!(!resumed.has_run());
        assert_eq!(resharding_rows(&mut publication.server, name).await, 1);
        assert_eq!(
            stored_stage(&mut publication.server, name).await,
            Stage::CopyTables.to_string()
        );

        state.abort_run(CommandKind::Reshard, None).await.unwrap();
        wait_until_removed(&mut publication.server, name).await;
        assert_eq!(resharding_rows(&mut publication.server, name).await, 0);

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn cutover_ends_the_run() {
        crate::logger();
        let name = "state_cutover";
        let mut publication = keyed_publication(name).await;
        let source = Cluster::new_test(&config());
        source.launch();
        let shards = source.shards().len() as i64;
        remove_resumable(&mut publication.server, name).await;
        let cancel = CancellationToken::new();

        let state = keyed_state(&source, name, false);
        start(&state, 1).await.unwrap();
        state.prepare_replication(&cancel).await.unwrap();
        assert_eq!(resharding_rows(&mut publication.server, name).await, 1);

        state.prepare_reverse_replication(&cancel).await.unwrap();
        state.prepare_replication(&cancel).await.unwrap();
        assert_eq!(resharding_rows(&mut publication.server, name).await, 0);
        assert_eq!(slot_count(&mut publication.server, name).await, shards);
        assert_eq!(
            origin_count(&mut publication.server, name).await,
            shards * shards
        );
        assert!(!state.keeps_slots());
        assert!(matches!(
            state.check_record(CommandKind::Replicate, None).await,
            Err(Error::NoResharding(_))
        ));

        state.cancel_run().await.unwrap();
        wait_until_removed(&mut publication.server, name).await;
        assert_eq!(origin_count(&mut publication.server, name).await, 0);

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }

    async fn lock_pid(server: &mut Server, key: &ReshardingKey) -> Option<i32> {
        server
            .fetch_all::<i32>(format!(
                "SELECT pid FROM pg_locks \
                 WHERE locktype = 'advisory' AND granted AND objsubid = 2 \
                 AND objid = (hashtext('{}')::bigint & 4294967295)::oid",
                lock_name(key)
            ))
            .await
            .unwrap()
            .first()
            .copied()
    }

    #[tokio::test]
    async fn heartbeat_takes_the_lock_back_after_connection_loss() {
        crate::logger();
        let name = "state_heartbeat_relock";
        let mut publication = keyed_publication(name).await;
        let source = Cluster::new_test(&config());
        source.launch();
        remove_resumable(&mut publication.server, name).await;
        let mut updated = (*config()).clone();
        updated.config.resharding.state_heartbeat_interval = 100;
        crate::config::set(updated).unwrap();

        let state = keyed_state(&source, name, false);
        let key = state.run_key();
        start(&state, 1).await.unwrap();
        let old = lock_pid(&mut publication.server, &key).await.unwrap();
        terminate_state_backend(&mut publication.server, &key).await;

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match lock_pid(&mut publication.server, &key).await {
                    Some(pid) if pid != old => break,
                    _ => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            }
        })
        .await
        .expect("the heartbeat must take the lock back");
        assert!(state.has_run());
        assert_eq!(resharding_rows(&mut publication.server, name).await, 1);

        state.finish_run().await.unwrap();
        assert_eq!(resharding_rows(&mut publication.server, name).await, 0);

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }
}
