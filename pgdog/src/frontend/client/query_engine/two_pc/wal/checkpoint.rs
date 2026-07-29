//! Recycle WAL segments we no longer need.

use crate::net::Error;
use crate::tasks;
use std::time::Duration;
use std::{io::ErrorKind, path::PathBuf};
use tokio::fs::remove_file;
use tokio::select;
use tokio::time::{Instant, sleep};
use tracing::{debug, error, info};

use super::super::{Manager, TwoPcTransaction};
use super::*;

pub(crate) struct Checkpointer {
    wal_directory: PathBuf,
}

impl Checkpointer {
    /// Create checkpointer that will run on the specified WAL directory.
    ///
    /// # Arguments
    ///
    /// - `wal_directory`: WAL directory.
    ///
    pub(crate) fn new(wal_directory: &PathBuf) -> Self {
        Self {
            wal_directory: wal_directory.clone(),
        }
    }

    pub(crate) fn spawn(wal_directory: PathBuf, manager: Manager, checkpoint_interval: Duration) {
        tasks::spawn("2pc wal checkpointer", async move {
            let shutdown = tasks::shutdown_signal();

            debug!("[2pc] checkpointer started");

            loop {
                let checkpointer = Self::new(&wal_directory);

                select! {
                    result = checkpointer.run_once(&manager) => {
                        if let Err(err) = result {
                            error!("[2pc] checkpoint error: {err}");
                        }
                    },
                    _ = shutdown.cancelled() => { break; },
                }

                select! {
                    _ = sleep(checkpoint_interval) => {},
                    _ = shutdown.cancelled() => { break; },
                }
            }

            debug!("[2pc] checkpointer shut down");
        });
    }

    pub(super) async fn run_once(&self, manager: &Manager) -> Result<(), Error> {
        let segments = SegmentRegistry::get()
            .inactive()
            .into_iter()
            .map(|id| (id, Segment::path(&self.wal_directory, id)))
            .collect::<Vec<_>>();

        let now = Instant::now();

        for (id, path) in &segments {
            let transactions = Self::read_tids(path).await?;
            let can_remove = transactions
                .iter()
                .all(|transaction| manager.transaction(&transaction).is_none());

            if can_remove {
                debug!(
                    r#"[2pc] checkpointer removing segment "{}""#,
                    path.display()
                );
                match remove_file(path).await {
                    Ok(()) => {}
                    Err(err) if err.kind() == ErrorKind::NotFound => {}
                    Err(err) => return Err(err.into()),
                }

                SegmentRegistry::get().remove(*id);
            }
        }

        if !segments.is_empty() {
            info!(
                "[2pc] checkpointer removed {} WAL segments in {:.3}s",
                segments.len(),
                now.elapsed().as_secs_f32()
            );
        }

        Ok(())
    }

    async fn read_tids(path: &PathBuf) -> Result<Vec<TwoPcTransaction>, Error> {
        let segment = Segment::load(path).await?;

        let mut tids = vec![];

        for record in segment.records {
            if let Ok(record) = Records::try_from(record) {
                match record {
                    Records::Add(record) => tids.push(record.transaction),
                    Records::Remove(record) => tids.push(record.transaction),
                }
            }
        }

        Ok(tids)
    }
}
