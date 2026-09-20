use crate::tasks;
use crate::{backend::Cluster, util::random_string};
use pgdog_stats::Databases;
use std::{fmt::Display, sync::Arc};
use tokio::sync::{Mutex, MutexGuard};
use tracing::warn;

use super::*;

#[derive(Debug, Clone)]
pub(crate) struct Orchestrator {
    pub(crate) source: Cluster,
    pub(crate) destination: Cluster,
    pub(crate) publication: String,
    publisher: Arc<Mutex<Publisher>>,
    replication_slot: String,
}

/// A handle to a publication's replication slots, decoupled from the rest of
/// the orchestrator. Awaiting [`PublicationGuard::cleanup`] drops every slot
/// the publisher still owns — a no-op once `replicate` has handed them off to
/// the streaming tasks. Dropping an armed guard schedules cleanup when its
/// owning task is aborted; successful migrations must disarm it.
pub(crate) struct PublicationGuard {
    publisher: Option<Arc<Mutex<Publisher>>>,
}

impl PublicationGuard {
    /// Drop any replication slots the publisher still owns.
    pub(crate) async fn cleanup(mut self) -> Result<(), Error> {
        let publisher = self.publisher.as_ref().expect("publication guard is armed");
        let result = Box::pin(publisher.lock().await.cleanup()).await;
        self.publisher.take();
        result
    }

    /// Preserve the slots when a migration completes successfully.
    pub(crate) fn disarm(mut self) {
        self.publisher.take();
    }
}

impl Drop for PublicationGuard {
    fn drop(&mut self) {
        let Some(publisher) = self.publisher.take() else {
            return;
        };
        tasks::spawn("replication slot cleanup", async move {
            if let Err(err) = Box::pin(publisher.lock().await.cleanup()).await {
                warn!("failed to clean up replication slots after an aborted migration: {err}");
            }
        });
    }
}

impl Orchestrator {
    /// Create new orchestrator.
    pub(crate) fn new(
        source: &str,
        destination: &str,
        publication: &str,
        replication_slot: Option<String>,
    ) -> Result<Self, Error> {
        let source = databases().schema_owner(source)?;
        let destination = databases().schema_owner(destination)?;

        let replication_slot = replication_slot
            .unwrap_or(format!("__pgdog_repl_{}", random_string(19).to_lowercase()));

        let mut orchestrator = Self {
            source,
            destination,
            publication: publication.to_owned(),
            publisher: Arc::new(Mutex::new(Publisher::default())),
            replication_slot,
        };

        orchestrator.refresh_publisher();

        Ok(orchestrator)
    }

    /// Reload source/dest cluster references from the live databases registry.
    pub(crate) fn refresh(&mut self) -> Result<(), Error> {
        self.source = databases().schema_owner(&self.source.identifier().database)?;
        self.destination = databases().schema_owner(&self.destination.identifier().database)?;
        Ok(())
    }

    /// Replace the publisher entirely (discards LSN state). Only valid
    /// before any replication slot exists, e.g. after the pre-data schema sync.
    pub(crate) fn refresh_publisher(&mut self) {
        let publisher = Publisher::new(&self.publication, self.replication_slot.clone());
        self.publisher = Arc::new(Mutex::new(publisher));
    }

    pub(crate) fn replication_slot(&self) -> &str {
        &self.replication_slot
    }

    pub(crate) async fn publisher(&self) -> MutexGuard<'_, Publisher> {
        self.publisher.lock().await
    }

    /// Take a [`PublicationGuard`] over this orchestrator's replication slots.
    pub(crate) fn publication_guard(&self) -> PublicationGuard {
        PublicationGuard {
            publisher: Some(self.publisher.clone()),
        }
    }

    /// The two ends of the migration this orchestrator drives.
    pub(crate) fn databases(&self) -> Databases {
        Databases {
            source: self.source.identifier().database.clone(),
            destination: self.destination.identifier().database.clone(),
        }
    }
}

impl Display for Orchestrator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> {}",
            self.source.identifier().database,
            self.destination.identifier().database
        )
    }
}
