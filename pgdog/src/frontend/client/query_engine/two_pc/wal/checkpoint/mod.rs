//! Delete WAL segments we no longer need.

use crate::net::Error;
use crate::tasks;
use crate::util::safe_sleep;
use std::time::Duration;
use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
};
use tokio::fs::remove_file;
use tokio::select;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use super::super::{Manager, TwoPcTransaction};
use super::*;

mod selection;
use selection::SegmentDependencies;

// In charge of automatically removing WAL segments
// that refer to transactions we already comitted/rolled back.
#[derive(Debug, Clone)]
pub(crate) struct Checkpointer {
    wal_directory: PathBuf,
    manager: Manager,
    checkpoint_interval: Duration,
    shutdown: CancellationToken,
}

impl Checkpointer {
    /// Create checkpointer that will run on the specified WAL directory.
    ///
    /// # Arguments
    ///
    /// - `wal_directory`: Where the WAL segments live.
    /// - `manager: 2pc manager.
    /// - `checkpoint_interval`: How often to run the checkpointer.
    ///
    pub(crate) fn new(
        wal_directory: &Path,
        manager: Manager,
        checkpoint_interval: Duration,
    ) -> Self {
        Self {
            wal_directory: wal_directory.to_path_buf(),
            manager,
            checkpoint_interval,
            shutdown: CancellationToken::new(),
        }
    }

    /// Run the checkpointer.
    ///
    /// Note to caller: don't call this more than once.
    /// I don't have to add an atomic to gate this, right?
    ///
    pub(crate) fn spawn(&self) {
        let checkpointer = self.clone();

        tasks::spawn("2pc wal checkpointer", async move {
            info!("[2pc] checkpointer started");

            loop {
                select! {
                    result = checkpointer.run_once() => {
                        if let Err(err) = result {
                            error!("[2pc] checkpoint error: {err}");
                        }
                    },
                    _ = checkpointer.shutdown.cancelled() => { break; }
                }

                select! {
                    _ = safe_sleep(checkpointer.checkpoint_interval) => {},
                    _ = checkpointer.shutdown.cancelled() => { break; },
                }
            }

            info!("[2pc] checkpointer shut down");
        });
    }

    // Clean up unused segments, retaining the identities needed by every
    // retained phase record, even when its transaction has already finished.
    async fn run_once(&self) -> Result<(), Error> {
        let now = Instant::now();
        let mut segments = Vec::new();
        for id in SegmentRegistry::get().inactive() {
            let path = Segment::path(&self.wal_directory, id);
            match Segment::load(&path).await {
                Ok(segment) => segments.push(SegmentDependencies::new(segment)?),
                // A previous checkpoint may have been interrupted after unlink
                // but before directory fsync or registry removal. Finish that
                // durability barrier before considering any dependencies.
                Err(Error::Io(err)) if err.kind() == ErrorKind::NotFound => {
                    self.remove_segment(id).await?;
                }
                Err(err) => return Err(err),
            }
        }

        let candidates = selection::candidates(&segments, &self.manager);
        for id in &candidates {
            self.remove_segment(*id).await?;
        }

        if !candidates.is_empty() {
            info!(
                "[2pc] checkpointer removed {} WAL segments in {:.3}s",
                candidates.len(),
                now.elapsed().as_secs_f32()
            );
        }

        Ok(())
    }

    // Candidates are deleted newest first. Persist each unlink before removing
    // an older identity segment, including when retrying an interrupted unlink.
    async fn remove_segment(&self, id: u64) -> Result<(), Error> {
        let path = Segment::path(&self.wal_directory, id);
        debug!(
            r#"[2pc] checkpointer removing segment "{}""#,
            path.display()
        );
        match remove_file(&path).await {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
        #[cfg(unix)]
        {
            let directory = tokio::fs::File::open(&self.wal_directory).await?;
            directory.sync_all().await?;
        }
        SegmentRegistry::get().remove(id);
        Ok(())
    }

    /// Ask the checkpointer to shut down immediately.
    /// You don't have to wait for it to shut down. The WAL
    /// can be left in a dirty state and recovery will take care of it.
    pub(super) fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

#[cfg(test)]
mod tests;
