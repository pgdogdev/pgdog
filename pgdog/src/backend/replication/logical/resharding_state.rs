use futures::prelude::future::join_all;
use parking_lot::Mutex;
use pgdog_stats::Databases;
use std::{collections::HashMap, fmt::Display, sync::Arc, time::Duration};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::publisher::{Permanent, ReplicationSlot, Table};
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
    #[builder(into)]
    slot_name: String,
}

type Slots = HashMap<usize, ReplicationSlot<Permanent>>;

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
}

impl Drop for ReshardingStateInner {
    fn drop(&mut self) {
        remove_slots(self.take_owned_slots());
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

    pub(crate) fn tables(&self) -> HashMap<usize, Vec<Table>> {
        self.inner.lock().tables.clone()
    }

    pub(crate) fn pop_tables(&self, shard: usize) -> Result<Vec<Table>, Error> {
        self.inner
            .lock()
            .tables
            .remove(&shard)
            .ok_or(Error::NoReplicationTables(shard))
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
            .ok_or(Error::NoReplicationSlot(shard))
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

    pub(crate) async fn prepare_replication(
        &self,
        cancel: &CancellationToken,
    ) -> Result<(), Error> {
        self.sync_tables().await?;

        let ready = !self.inner.lock().slots.is_empty();

        if ready {
            return Ok(());
        }

        self.create_slots(cancel).await
    }

    /// Remove every slot this state holds.
    pub(crate) async fn drop_slots(&self) -> Result<(), Error> {
        let slots = self.inner.lock().take_slots();

        remove_slots(slots).await?
    }

    /// Remove only the slots this state created.
    pub(crate) async fn drop_slots_if_owned(&self) -> Result<(), Error> {
        let slots = self.inner.lock().take_owned_slots();

        remove_slots(slots).await?
    }

    /// Forget the slots without removing them from the backend.
    pub(crate) fn detach_slots(&self) {
        self.inner.lock().take_slots();
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
}
