use futures::future::try_join_all;
use futures::prelude::future::join_all;
use parking_lot::Mutex;
use pgdog_stats::{Databases, ReshardingDirection, TaskId};
use std::{
    collections::HashMap,
    fmt::Display,
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::publisher::{Permanent, ReplicationSlot, Table};
use super::run::{
    CommandKind, ConfigSnapshot, FORMAT_VERSION, ReshardingCommand, ReshardingId, ReshardingKey,
    ReshardingRecord, ReshardingStore, RunObjects, Stage, TaskOwner, existing_slots,
};
use super::subscriber::connect_address;
use super::subscriber::replication_origin::ReplicationOrigin;
use super::tables_sync::tables_sync;
use super::*;
use crate::backend::pool::{Address, Request};
use crate::tasks;
use crate::util::safe_timeout;
use crate::{backend::Cluster, util::random_string};

const SLOT_DROP_TIMEOUT: Duration = Duration::from_secs(30);

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
}

#[derive(Debug, bon::Builder)]
struct ReshardingStateInner {
    /// Name of the publication.
    #[builder(into)]
    publication: String,
    /// Shard -> Tables mapping.
    #[builder(skip)]
    tables: HashMap<usize, Vec<Table>>,
    /// Shard -> Replication slot mapping.
    #[builder(skip)]
    slots: HashMap<usize, ReplicationSlot<Permanent>>,
    #[builder(skip)]
    origins: Origins,
    #[builder(into)]
    slot_name: String,
    #[builder(skip)]
    direction: ReshardingDirection,
    #[builder(default = true)]
    cleanup_on_cancel: bool,
    #[builder(default)]
    resume: bool,
    #[builder(skip)]
    slot_objects: RunObjects,
    #[builder(skip)]
    origin_objects: RunObjects,
    #[builder(skip)]
    store: Option<Arc<ReshardingStore>>,
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
        !self.cleanup_on_cancel && self.store.is_some()
    }
}

impl Drop for ReshardingStateInner {
    fn drop(&mut self) {
        if self.keeps_slots() {
            return;
        }
        remove_slots(self.take_owned_slots());
        remove_origins(self.take_origins());
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

        let inner = ReshardingStateInner::builder()
            .publication(publication)
            .slot_name(slot_name.clone())
            .cleanup_on_cancel(cleanup_on_cancel)
            .resume(resume)
            .build();

        Ok(Self {
            source: databases().schema_owner(source)?,
            destination: databases().schema_owner(destination)?,
            publication: publication.to_owned(),
            inner: Arc::new(Mutex::new(inner)),
            slot_name,
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
        let objects = self.inner.lock().slot_objects;
        self.create_slots_on(&self.source, objects, cancel).await
    }

    /// Create the slots a rollback would replicate from, on the destination.
    pub(crate) async fn create_reverse_slots(
        &self,
        cancel: &CancellationToken,
    ) -> Result<(), Error> {
        self.create_slots_on(&self.destination, RunObjects::Create, cancel)
            .await
    }

    async fn create_slots_on(
        &self,
        cluster: &Cluster,
        objects: RunObjects,
        cancel: &CancellationToken,
    ) -> Result<(), Error> {
        let (publication, slot_name) = {
            let inner = self.inner.lock();
            if !inner.slots.is_empty() {
                return Err(Error::SlotsAlreadyCreated(inner.slot_name.clone()));
            }
            (inner.publication.clone(), inner.slot_name.clone())
        };

        if cancel.is_cancelled() {
            return Err(Error::ReplicationAborted);
        }

        join_all(cluster.shards().iter().enumerate().map(|(number, shard)| {
            let publication = publication.clone();
            let slot_name = slot_name.clone();

            async move {
                let addr = shard.primary(&Request::default()).await?.addr().clone();

                let slot =
                    ReplicationSlot::new_permanent(&publication, &addr, Some(slot_name), number);
                self.inner.lock().slots.insert(number, slot.clone());

                match objects {
                    RunObjects::Load { .. } => slot.verify_exists().await?,
                    RunObjects::Create | RunObjects::Reuse { .. } => slot.reuse_or_create().await?,
                }
                if objects.owned() {
                    slot.mark_owned();
                }
                Ok(())
            }
        }))
        .await
        .into_iter()
        .collect()
    }

    async fn create_origins(&self) -> Result<(), Error> {
        let (slots, objects): (Vec<(usize, String)>, RunObjects) = {
            let inner = self.inner.lock();
            let slots = inner
                .slots
                .iter()
                .map(|(number, slot)| (*number, slot.name().to_owned()))
                .collect();
            (slots, inner.origin_objects)
        };

        for (number, origins) in self.origins_for(slots).await? {
            self.inner.lock().origins.insert(number, origins.clone());
            match objects {
                RunObjects::Create => {
                    try_join_all(origins.iter().map(ReplicationOrigin::recreate_origin)).await?
                }
                RunObjects::Reuse { .. } => {
                    try_join_all(origins.iter().map(ReplicationOrigin::create_origin)).await?
                }
                RunObjects::Load { .. } => {
                    try_join_all(origins.iter().map(ReplicationOrigin::verify_exists)).await?
                }
            };
        }

        Ok(())
    }

    async fn origins_for(&self, slots: Vec<(usize, String)>) -> Result<Origins, Error> {
        let mut addresses = vec![];
        for shard in self.destination.shards() {
            addresses.push(shard.primary(&Request::default()).await?.addr().clone());
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

        Ok(())
    }

    /// Remove every slot this state holds, and its origins.
    pub(crate) async fn drop_slots(&self) -> Result<(), Error> {
        let (slots, origins) = {
            let mut inner = self.inner.lock();
            (inner.take_slots(), inner.take_origins())
        };

        let slots = remove_slots(slots);
        let origins = remove_origins(origins);
        slots.await?.and(origins.await?)
    }

    /// Remove only the slots this state created, and every origin.
    pub(crate) async fn drop_slots_if_owned(&self) -> Result<(), Error> {
        let (slots, origins) = {
            let mut inner = self.inner.lock();
            (inner.take_owned_slots(), inner.take_origins())
        };

        let slots = remove_slots(slots);
        let origins = remove_origins(origins);
        slots.await?.and(origins.await?)
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
        self.inner.lock().cleanup_on_cancel
    }

    pub(crate) async fn start_run(
        &self,
        command: ReshardingCommand,
        stage: Stage,
        task_id: TaskId,
    ) -> Result<(), Error> {
        let key = self.run_key();
        let address = self.state_address().await?;
        let store = ReshardingStore::open(&address, key.clone()).await?;
        let (slot_objects, origin_objects) =
            match self.record_run(&store, command, stage, task_id).await {
                Ok(objects) => objects,
                Err(err) => {
                    warn!("resharding {key}: refused to start: {err}");
                    unlock(&store).await;
                    return Err(err);
                }
            };
        info!(
            "resharding {key}: {} ({command}, slots {slot_objects:?}, origins {origin_objects:?})",
            if self.inner.lock().resume {
                "resumed"
            } else {
                "started"
            },
        );
        run::scan::check_other_runs(&address, &key).await;

        let mut inner = self.inner.lock();
        inner.store = Some(Arc::new(store));
        inner.slot_objects = slot_objects;
        inner.origin_objects = origin_objects;
        Ok(())
    }

    async fn record_run(
        &self,
        store: &ReshardingStore,
        command: ReshardingCommand,
        stage: Stage,
        task_id: TaskId,
    ) -> Result<(RunObjects, RunObjects), Error> {
        let key = store.key();
        let resume = self.inner.lock().resume;
        match (resume, store.load().await?) {
            (false, Some(record)) => Err(Error::ReshardingExists {
                key: key.to_string(),
                stage: record.stage.to_string(),
            }),
            (true, None) => Err(Error::NoResharding(key.to_string())),
            (true, Some(record)) => {
                self.check_resumed(&record, command)?;
                store.set_owner(&TaskOwner::current(task_id)).await?;
                Ok((
                    RunObjects::resumed(
                        record.stage,
                        record.command.slots_stage(),
                        !record.slot_existed,
                    ),
                    RunObjects::resumed(record.stage, record.command.origins_stage(), false),
                ))
            }
            (false, None) => {
                let existing = existing_slots(&self.source, &self.publication, &self.slot_name)
                    .await?
                    .len();
                let slot_existed = if existing == 0 {
                    false
                } else if existing == self.source.shards().len() {
                    true
                } else {
                    return Err(Error::ReshardingSlotsPartial(self.slot_name.clone()));
                };

                store
                    .insert(&ReshardingRecord {
                        id: ReshardingId::generate(),
                        key: key.clone(),
                        command,
                        slot_name: self.slot_name.clone(),
                        slot_existed,
                        stage,
                        config: self.config_snapshot()?,
                        owner: TaskOwner::current(task_id),
                        format_version: FORMAT_VERSION,
                        updated_at: std::time::SystemTime::now(),
                    })
                    .await?;
                Ok((RunObjects::Create, RunObjects::Create))
            }
        }
    }

    fn config_snapshot(&self) -> Result<ConfigSnapshot, Error> {
        ConfigSnapshot::capture(
            &self.source,
            &self.destination,
            &crate::config::config().config,
        )
    }

    fn check_resumed(
        &self,
        record: &ReshardingRecord,
        command: ReshardingCommand,
    ) -> Result<(), Error> {
        if record.command != command {
            return Err(Error::ReshardingMismatch {
                field: "command",
                stored: command_text(&record.command),
                given: command_text(&command),
            });
        }
        if record.slot_name != self.slot_name {
            return Err(Error::ReshardingMismatch {
                field: "slot",
                stored: record.slot_name.clone(),
                given: self.slot_name.clone(),
            });
        }
        record
            .config
            .validate_equal(&self.config_snapshot()?)
            .map_err(Error::ConfigChanged)
    }

    pub(crate) fn keeps_slots(&self) -> bool {
        self.inner.lock().keeps_slots()
    }

    pub(crate) fn run_store(&self) -> Option<Weak<ReshardingStore>> {
        self.inner.lock().store.as_ref().map(Arc::downgrade)
    }

    pub(crate) async fn set_stage(&self, stage: Stage) -> Result<(), Error> {
        let store = self.inner.lock().store.clone();
        match store {
            Some(store) => store.set_stage(stage).await,
            None => Ok(()),
        }
    }

    pub(crate) async fn cancel_run(&self) -> Result<(), Error> {
        if self.keeps_slots() {
            let paused = {
                let mut inner = self.inner.lock();
                drop((inner.take_slots(), inner.take_origins()));
                inner.store.take()
            };
            if let Some(store) = paused {
                unlock(&store).await;
                info!(
                    "resharding {} paused, replication slots and origins kept",
                    store.key()
                );
            }
            return Ok(());
        }
        let cleanup = match self.drop_slots_if_owned().await {
            Ok(()) => self.finish_run().await,
            Err(err) => Err(err),
        };
        if cleanup.is_err() {
            self.release_run().await;
        }
        cleanup
    }

    pub(crate) async fn release_run(&self) {
        let store = {
            let mut inner = self.inner.lock();
            inner.slot_objects = RunObjects::Create;
            inner.origin_objects = RunObjects::Create;
            inner.store.take()
        };
        if let Some(store) = store {
            unlock(&store).await;
        }
    }

    pub(crate) async fn finish_run(&self) -> Result<(), Error> {
        let store = self.inner.lock().store.clone();
        let Some(store) = store else {
            return Ok(());
        };
        store.delete().await?;
        self.release_run().await;
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
        let mut server = connect_address(&self.state_address().await?).await?;
        let record = ReshardingStore::read(&mut server, &key)
            .await?
            .ok_or_else(|| Error::NoResharding(key.to_string()))?;
        record_matches(&record, command, slot)?;
        Ok(record)
    }

    pub(crate) async fn open_for_abort(
        &self,
        command: CommandKind,
        slot: Option<&str>,
    ) -> Result<(ReshardingStore, ReshardingRecord), Error> {
        let key = self.run_key();
        let store = ReshardingStore::open(&self.state_address().await?, key.clone()).await?;
        let record = store
            .load()
            .await?
            .ok_or_else(|| Error::NoResharding(key.to_string()))?;
        record_matches(&record, command, slot)?;
        Ok((store, record))
    }

    pub(crate) async fn adopt_run(&self, record: &ReshardingRecord) -> Result<(), Error> {
        let slots = if record.slot_existed {
            Slots::new()
        } else {
            existing_slots(&self.source, &self.publication, &record.slot_name).await?
        };
        let names = (0..self.source.shards().len())
            .map(|number| (number, format!("{}_{number}", record.slot_name)))
            .collect();
        let origins = self.origins_for(names).await?;

        let mut inner = self.inner.lock();
        inner.slots = slots;
        inner.origins = origins;
        Ok(())
    }
}

fn command_text(command: &ReshardingCommand) -> String {
    serde_json::to_string(command).expect("command is valid JSON")
}

fn record_matches(
    record: &ReshardingRecord,
    command: CommandKind,
    slot: Option<&str>,
) -> Result<(), Error> {
    if record.command.kind() != command {
        return Err(Error::ReshardingMismatch {
            field: "command",
            stored: record.command.to_string(),
            given: command.to_string(),
        });
    }
    match slot {
        Some(slot) if slot != record.slot_name => Err(Error::ReshardingMismatch {
            field: "slot",
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

fn remove_slots(slots: Slots) -> JoinHandle<Result<(), Error>> {
    tasks::spawn("replication slot cleanup", async move {
        let removals = slots.into_values().map(|slot| async move {
            let slot_name = slot.name().to_owned();
            safe_timeout(SLOT_DROP_TIMEOUT, slot.drop_slot())
                .await
                .unwrap_or(Err(Error::SlotDropTimeout(slot_name)))
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
        let mut inner = ReshardingStateInner::builder()
            .publication("publication_test")
            .slot_name(slot_name)
            .build();

        for (number, shard) in source.shards().iter().enumerate() {
            let addr = shard
                .primary(&Request::default())
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
        let inner = ReshardingStateInner::builder()
            .publication("publication_test")
            .slot_name(slot_name)
            .build();

        ReshardingState {
            source: source.clone(),
            destination: source.clone(),
            publication: "publication_test".into(),
            inner: Arc::new(Mutex::new(inner)),
            slot_name: slot_name.to_owned(),
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

        let state = state_for(&source, name);
        state.inner.lock().cleanup_on_cancel = false;
        start(&state, 1).await.unwrap();
        assert!(state.keeps_slots());
        assert_eq!(resharding_rows(&mut publication.server, name).await, 1);

        state
            .prepare_replication(&CancellationToken::new())
            .await
            .unwrap();
        state.cancel_run().await.unwrap();
        assert!(!state.keeps_slots());
        assert!(state.run_store().is_none());
        drop(state);
        tokio::time::sleep(Duration::from_secs(1)).await;

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
        let mut state = state_for(source, name);
        state.publication = format!("{name}_pub");
        state.inner.lock().cleanup_on_cancel = cleanup_on_cancel;
        state
    }

    async fn start(state: &ReshardingState, task_id: u64) -> Result<(), Error> {
        state
            .start_run(REPLICATE, Stage::Replication, TaskId::new(task_id))
            .await
    }

    #[tokio::test]
    async fn other_unfinished_runs_only_warn() {
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
        paused.cancel_run().await.unwrap();
        publication
            .server
            .execute_checked(format!(
                "UPDATE pgdog.resharding SET updated_at = now() - interval '10 minutes' \
                 WHERE slot_name = '{paused_name}'"
            ))
            .await
            .unwrap();

        let again = start(&keyed_state(&source, paused_name, false), 2).await;
        assert!(
            matches!(again, Err(Error::ReshardingExists { .. })),
            "{again:?}"
        );

        let other = keyed_state(&source, new_name, false);
        start(&other, 3).await.unwrap();
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
        let (store, record) = state
            .open_for_abort(CommandKind::Replicate, Some(state.replication_slot()))
            .await
            .unwrap();
        state.adopt_run(&record).await.unwrap();
        state.drop_slots().await.unwrap();
        store.delete().await.unwrap();
        record
    }

    #[tokio::test]
    async fn abort_drops_only_what_the_run_created() {
        crate::logger();
        let name = "state_abort";
        let mut publication =
            setup_publication_tables(&format!("{name}_pub"), &["state_abort_one"]).await;
        let source = Cluster::new_test(&config());
        source.launch();
        let shards = source.shards().len() as i64;
        remove_resumable(&mut publication.server, name).await;

        let state = keyed_state(&source, name, false);
        pause_after_replication(&state).await;
        assert_eq!(slot_count(&mut publication.server, name).await, shards);

        for (command, slot) in [
            (CommandKind::Reshard, None),
            (CommandKind::Replicate, Some("other")),
        ] {
            let mismatch = state.check_record(command, slot).await;
            assert!(
                matches!(mismatch, Err(Error::ReshardingMismatch { .. })),
                "{mismatch:?}"
            );
        }
        state
            .check_record(CommandKind::Replicate, None)
            .await
            .unwrap();

        assert!(!abort(&state).await.slot_existed);
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
        assert!(abort(&state).await.slot_existed);
        assert_eq!(slot_count(&mut publication.server, name).await, shards);
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
    async fn finished_resume_state_frees_the_key() {
        crate::logger();
        let mut publication = setup_publication().await;
        let name = "state_resume_finish";
        let source = Cluster::new_test(&config());
        source.launch();
        remove_resumable(&mut publication.server, name).await;
        let state = keyed_state(&source, name, false);

        start(&state, 1).await.unwrap();
        assert!(state.keeps_slots());

        state.finish_run().await.unwrap();
        assert!(!state.keeps_slots());
        assert!(state.run_store().is_none());
        assert_eq!(resharding_rows(&mut publication.server, name).await, 0);

        start(&state, 2).await.unwrap();
        assert!(state.keeps_slots());
        state.finish_run().await.unwrap();

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn cancelled_plain_run_drops_owned_slots_and_record() {
        crate::logger();
        let name = "state_plain_cancel";
        let mut publication =
            setup_publication_tables(&format!("{name}_pub"), &["state_plain_cancel_one"]).await;
        let source = Cluster::new_test(&config());
        source.launch();
        remove_resumable(&mut publication.server, name).await;
        let state = keyed_state(&source, name, true);

        start(&state, 1).await.unwrap();
        assert!(!state.keeps_slots());
        assert!(state.run_store().is_some());
        assert_eq!(resharding_rows(&mut publication.server, name).await, 1);
        assert!(matches!(
            start(&keyed_state(&source, name, true), 2).await,
            Err(Error::ReshardingLocked { .. })
        ));

        state
            .prepare_replication(&CancellationToken::new())
            .await
            .unwrap();
        assert!(slot_count(&mut publication.server, name).await > 0);

        state.cancel_run().await.unwrap();
        assert_eq!(slot_count(&mut publication.server, name).await, 0);
        assert_eq!(origin_count(&mut publication.server, name).await, 0);
        assert_eq!(resharding_rows(&mut publication.server, name).await, 0);

        start(&state, 3).await.unwrap();
        state.finish_run().await.unwrap();

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

    fn resumed_state(source: &Cluster, name: &str, cleanup_on_cancel: bool) -> ReshardingState {
        let state = keyed_state(source, name, cleanup_on_cancel);
        state.inner.lock().resume = true;
        state
    }

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
        let mut publication = setup_publication().await;
        let name = "state_resume_match";
        let source = Cluster::new_test(&config());
        source.launch();
        remove_resumable(&mut publication.server, name).await;

        let missing = start(&resumed_state(&source, name, false), 1).await;
        assert!(
            matches!(missing, Err(Error::NoResharding(_))),
            "{missing:?}"
        );

        let state = keyed_state(&source, name, false);
        start(&state, 1).await.unwrap();
        state.cancel_run().await.unwrap();

        let other_command = resumed_state(&source, name, false)
            .start_run(
                ReshardingCommand::Replicate { auto_cutover: true },
                Stage::Replication,
                TaskId::new(2),
            )
            .await;
        assert!(
            matches!(
                other_command,
                Err(Error::ReshardingMismatch {
                    field: "command",
                    ..
                })
            ),
            "{other_command:?}"
        );

        let mut other_slot = resumed_state(&source, name, false);
        other_slot.slot_name = "other".into();
        let other_slot = start(&other_slot, 3).await;
        assert!(
            matches!(
                other_slot,
                Err(Error::ReshardingMismatch { field: "slot", .. })
            ),
            "{other_slot:?}"
        );

        let resumed = resumed_state(&source, name, false);
        start(&resumed, 7).await.unwrap();
        let owners: Vec<i64> = publication
            .server
            .fetch_all(format!(
                "SELECT task_id FROM pgdog.resharding WHERE slot_name = '{name}'"
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
    async fn resumed_run_loads_slots_and_origins_after_their_stage() {
        crate::logger();
        let name = "state_resume_load";
        let mut publication =
            setup_publication_tables(&format!("{name}_pub"), &["state_resume_load_one"]).await;
        let source = Cluster::new_test(&config());
        source.launch();
        let shards = source.shards().len() as i64;
        remove_resumable(&mut publication.server, name).await;
        let origin = format!("__pgdog_origin_{name}_0_0");
        let cancel = CancellationToken::new();

        let state = keyed_state(&source, name, false);
        state
            .start_run(RESHARD, Stage::Replication, TaskId::new(1))
            .await
            .unwrap();
        state.prepare_replication(&cancel).await.unwrap();
        state.cancel_run().await.unwrap();
        publication
            .server
            .execute_checked(format!(
                "SELECT pg_replication_origin_advance('{origin}', '0/1000')"
            ))
            .await
            .unwrap();

        let resumed = resumed_state(&source, name, false);
        resumed
            .start_run(RESHARD, Stage::Replication, TaskId::new(2))
            .await
            .unwrap();
        resumed.prepare_replication(&cancel).await.unwrap();
        assert_eq!(
            origin_progress(&mut publication.server, &origin).await,
            "0/1000"
        );
        assert_eq!(slot_count(&mut publication.server, name).await, shards);
        assert_eq!(
            origin_count(&mut publication.server, name).await,
            shards * shards
        );
        resumed.cancel_run().await.unwrap();

        publication
            .server
            .execute_checked(format!("SELECT pg_replication_origin_drop('{origin}')"))
            .await
            .unwrap();
        let resumed = resumed_state(&source, name, false);
        resumed
            .start_run(RESHARD, Stage::Replication, TaskId::new(3))
            .await
            .unwrap();
        let missing_origin = resumed.prepare_replication(&cancel).await;
        assert!(
            matches!(missing_origin, Err(Error::MissingReplicationOrigin(_))),
            "{missing_origin:?}"
        );
        assert_eq!(
            origin_count(&mut publication.server, name).await,
            shards * shards - 1
        );
        resumed.cancel_run().await.unwrap();

        publication
            .server
            .execute_checked(format!("SELECT pg_drop_replication_slot('{name}_0')"))
            .await
            .unwrap();
        let resumed = resumed_state(&source, name, false);
        resumed
            .start_run(RESHARD, Stage::Replication, TaskId::new(4))
            .await
            .unwrap();
        let missing_slot = resumed.prepare_replication(&cancel).await;
        assert!(
            matches!(missing_slot, Err(Error::MissingReplicationSlot(_))),
            "{missing_slot:?}"
        );
        assert_eq!(slot_count(&mut publication.server, name).await, shards - 1);
        resumed.cancel_run().await.unwrap();

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }

    #[tokio::test]
    async fn resumed_run_at_their_stage_reuses_slots_and_origins() {
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
                "SELECT pg_replication_origin_advance('{kept}', '0/2000'); \
                 SELECT pg_replication_origin_drop('{lost}')"
            ))
            .await
            .unwrap();

        let resumed = resumed_state(&source, name, true);
        start(&resumed, 2).await.unwrap();
        resumed.prepare_replication(&cancel).await.unwrap();
        assert_eq!(
            origin_progress(&mut publication.server, &kept).await,
            "0/2000"
        );
        assert_eq!(
            origin_count(&mut publication.server, name).await,
            shards * shards
        );

        resumed.cancel_run().await.unwrap();
        assert_eq!(slot_count(&mut publication.server, name).await, 0);
        assert_eq!(origin_count(&mut publication.server, name).await, 0);
        assert_eq!(resharding_rows(&mut publication.server, name).await, 0);

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }
}
