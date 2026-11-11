use std::collections::HashMap;
use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use tracing::info;

use super::super::Error;
use super::super::publisher::{Permanent, ReplicationSlot};
use super::super::resharding_state::ReshardingState;
use super::{
    ConfigSnapshot, FORMAT_VERSION, ReshardingCommand, ReshardingKey, ReshardingRecord,
    ReshardingStore, Stage,
};
use crate::backend::{Cluster, pool::Request};
use crate::config::config;

#[derive(Debug, Clone, Default)]
pub(crate) struct ResumeState {
    store: Arc<Mutex<Option<Arc<ReshardingStore>>>>,
}

impl ResumeState {
    pub(crate) async fn load_or_create(
        state: &ReshardingState,
        command: ReshardingCommand,
        stage: Stage,
    ) -> Result<Self, Error> {
        if !state.is_resumable() {
            return Ok(Self::default());
        }

        let key = ReshardingKey::new(state.databases(), &state.publication);
        let slot_name = state.replication_slot();
        let addr = state
            .destination
            .shards()
            .first()
            .ok_or_else(|| Error::NoPrimary)?
            .primary(&Request::default())
            .await?
            .addr()
            .clone();

        let store = ReshardingStore::open(&addr, key.clone()).await?;
        if let Some(record) = store.load().await? {
            return Err(Error::ReshardingExists {
                key: key.to_string(),
                stage: record.stage.to_string(),
            });
        }

        let existing = existing_slots(&state.source, &state.publication, slot_name)
            .await?
            .len();
        let slot_existed = if existing == 0 {
            false
        } else if existing == state.source.shards().len() {
            true
        } else {
            return Err(Error::ResumableSlotsPartial(slot_name.to_owned()));
        };

        store
            .insert(&ReshardingRecord {
                key,
                command,
                slot_name: slot_name.to_owned(),
                slot_existed,
                stage,
                config: ConfigSnapshot::capture(
                    &state.source,
                    &state.destination,
                    &config().config,
                )?,
                format_version: FORMAT_VERSION,
            })
            .await?;

        state.set_keep_slots(true);
        Ok(Self {
            store: Arc::new(Mutex::new(Some(Arc::new(store)))),
        })
    }

    pub(crate) fn keeps_slots(&self) -> bool {
        self.store.lock().is_some()
    }

    fn store(&self) -> Option<Arc<ReshardingStore>> {
        self.store.lock().clone()
    }

    pub(crate) fn run_store(&self) -> Option<Weak<ReshardingStore>> {
        self.store().map(|store| Arc::downgrade(&store))
    }

    pub(crate) async fn set_stage(&self, stage: Stage) -> Result<(), Error> {
        match self.store() {
            Some(store) => store.set_stage(stage).await,
            None => Ok(()),
        }
    }

    pub(crate) fn pause(&self, state: &ReshardingState) {
        let Some(store) = self.store.lock().take() else {
            return;
        };
        info!(
            "resharding {} paused, replication slots and origins kept",
            store.key()
        );
        drop(store);
        state.forget_slots();
    }

    pub(crate) async fn finish(&self, state: &ReshardingState) -> Result<(), Error> {
        let Some(store) = self.store() else {
            return Ok(());
        };
        store.delete().await?;
        self.store.lock().take();
        state.set_keep_slots(false);
        Ok(())
    }
}

pub(crate) async fn existing_slots(
    source: &Cluster,
    publication: &str,
    slot_name: &str,
) -> Result<HashMap<usize, ReplicationSlot<Permanent>>, Error> {
    let mut slots = HashMap::new();
    for (number, shard) in source.shards().iter().enumerate() {
        let addr = shard.primary(&Request::default()).await?.addr().clone();
        let slot =
            ReplicationSlot::new_permanent(publication, &addr, Some(slot_name.to_owned()), number);
        match slot.verify_exists().await {
            Ok(()) => {
                slots.insert(number, slot);
            }
            Err(Error::MissingReplicationSlot(_)) => (),
            Err(err) => return Err(err),
        }
    }
    Ok(slots)
}
