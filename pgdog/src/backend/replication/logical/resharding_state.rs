use futures::future::try_join_all;
use futures::prelude::future::join_all;
use parking_lot::Mutex;
use pgdog_stats::{Databases, ReshardingDirection};
use std::{collections::HashMap, fmt::Display, sync::Arc, time::Duration};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::publisher::{Permanent, ReplicationSlot, Table};
use super::subscriber::replication_origin::ReplicationOrigin;
use super::tables_sync::tables_sync;
use super::*;
use crate::backend::pool::Request;
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
    cleanup_on_cancel: bool,
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
    #[builder(skip)]
    keep_slots: bool,
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
}

impl Drop for ReshardingStateInner {
    fn drop(&mut self) {
        if self.keep_slots {
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
    ) -> Result<Self, Error> {
        let slot_name = replication_slot
            .unwrap_or(format!("__pgdog_repl_{}", random_string(19).to_lowercase()));

        let inner = ReshardingStateInner::builder()
            .publication(publication)
            .slot_name(slot_name.clone())
            .build();

        Ok(Self {
            source: databases().schema_owner(source)?,
            destination: databases().schema_owner(destination)?,
            publication: publication.to_owned(),
            inner: Arc::new(Mutex::new(inner)),
            slot_name,
            cleanup_on_cancel,
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

    /// Create the slots a rollback would replicate from, on the destination.
    pub(crate) async fn create_reverse_slots(
        &self,
        cancel: &CancellationToken,
    ) -> Result<(), Error> {
        self.create_slots_on(&self.destination, cancel).await
    }

    async fn create_slots_on(
        &self,
        cluster: &Cluster,
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

                slot.reuse_or_create().await
            }
        }))
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

        let mut addresses = vec![];
        for shard in self.destination.shards() {
            addresses.push(shard.primary(&Request::default()).await?.addr().clone());
        }

        for (number, slot_name) in slots {
            let origins: Vec<ReplicationOrigin> = addresses
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
            self.inner.lock().origins.insert(number, origins.clone());

            try_join_all(origins.iter().map(ReplicationOrigin::recreate_origin)).await?;
        }

        Ok(())
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

    pub(crate) fn is_resumable(&self) -> bool {
        !self.cleanup_on_cancel
    }

    pub(crate) fn set_keep_slots(&self, keep: bool) {
        self.inner.lock().keep_slots = keep;
    }

    pub(crate) fn forget_slots(&self) {
        let forgotten = {
            let mut inner = self.inner.lock();
            (inner.take_slots(), inner.take_origins())
        };
        drop(forgotten);
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
    use super::super::publisher::test::setup_publication;
    use super::super::resumable::{ReshardingCommand, ResumeState, Stage};
    use super::*;
    use crate::backend::Server;
    use crate::config::config;

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
            cleanup_on_cancel: true,
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
        let resumable = ResumeState::load_or_create(
            &state,
            ReshardingCommand::Replicate {
                auto_cutover: false,
            },
            Stage::Replication,
        )
        .await
        .unwrap();
        assert!(resumable.keeps_slots());
        assert_eq!(resharding_rows(&mut publication.server, name).await, 1);

        state
            .prepare_replication(&CancellationToken::new())
            .await
            .unwrap();
        resumable.pause(&state);
        assert!(!resumable.keeps_slots());
        assert!(resumable.run_store().is_none());
        drop(resumable);
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

    fn resumable_state(source: &Cluster, name: &str) -> ReshardingState {
        let mut state = state_for(source, name);
        state.publication = format!("{name}_pub");
        state.cleanup_on_cancel = false;
        state
    }

    async fn load_when_unlocked(state: &ReshardingState) -> Result<ResumeState, Error> {
        for _ in 0..50 {
            match ResumeState::load_or_create(state, REPLICATE, Stage::Replication).await {
                Err(Error::ReshardingLocked { .. }) => {
                    tokio::time::sleep(Duration::from_millis(100)).await
                }
                other => return other,
            }
        }
        panic!("the resharding lock was not released");
    }

    #[tokio::test]
    async fn resume_state_refuses_parallel_and_unfinished_runs() {
        crate::logger();
        let mut publication = setup_publication().await;
        let name = "state_resume_refuse";
        let source = Cluster::new_test(&config());
        source.launch();
        remove_resumable(&mut publication.server, name).await;
        let state = resumable_state(&source, name);

        let running = ResumeState::load_or_create(&state, REPLICATE, Stage::Replication)
            .await
            .unwrap();

        let parallel = ResumeState::load_or_create(&state, REPLICATE, Stage::Replication).await;
        assert!(
            matches!(parallel, Err(Error::ReshardingLocked { .. })),
            "{parallel:?}"
        );

        running.pause(&state);
        let unfinished = load_when_unlocked(&state).await;
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
        let state = resumable_state(&source, name);

        let first = ResumeState::load_or_create(&state, REPLICATE, Stage::Replication)
            .await
            .unwrap();
        assert!(state.inner.lock().keep_slots);

        first.finish(&state).await.unwrap();
        assert!(!first.keeps_slots());
        assert!(!state.inner.lock().keep_slots);
        assert_eq!(resharding_rows(&mut publication.server, name).await, 0);

        let second = load_when_unlocked(&state).await.unwrap();
        assert!(second.keeps_slots());
        second.finish(&state).await.unwrap();

        remove_resumable(&mut publication.server, name).await;
        source.shutdown();
        publication.cleanup().await;
    }
}
