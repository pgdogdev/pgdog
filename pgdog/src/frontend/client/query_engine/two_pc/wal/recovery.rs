//! Handle recovering the state
//! of the 2pc manager from WAL segments.

use super::{super::Manager, EXTENSION, Segment, SegmentRegistry, SegmentStatus, WalWriter};
use crate::net::Error;
use std::{io::ErrorKind, path::PathBuf};
use tokio::fs::{read_dir, remove_file};
use tokio::time::Instant;
use tracing::{info, warn};

pub(crate) struct Recovery {
    pub(crate) path: PathBuf,
    pub(super) files: Vec<(u64, PathBuf)>,
}

impl Recovery {
    /// Load all segments from the WAL directory in preparation
    /// for recovery.
    ///
    /// # Arguments
    ///
    /// - `path`: WAL directory
    ///
    pub(crate) async fn new(path: &PathBuf) -> Result<Self, Error> {
        let mut dir = read_dir(path).await?;
        let mut files = vec![];

        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();

            let mut skip = path
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext != EXTENSION)
                .unwrap_or(true);

            let segment_id = path
                .file_stem()
                .and_then(|prefix| prefix.to_str())
                .and_then(|prefix| prefix.parse::<u64>().ok());

            if segment_id.is_none() {
                skip = true;
            }

            if skip {
                warn!(
                    r#"[2pc] recovery ignoring unknown file "{}""#,
                    path.display()
                );
            } else if let Some(segment_id) = segment_id {
                files.push((segment_id, path));
            }
        }

        files.sort_by_key(|(segment_id, _)| *segment_id);

        Ok(Self {
            path: path.clone(),
            files,
        })
    }

    /// Run the recovery.
    ///
    /// # Arguments
    ///
    /// - `manager`: 2pc manager.
    ///
    pub(crate) async fn run(
        self,
        manager: &Manager,
        segment_size: usize,
    ) -> Result<WalWriter, Error> {
        let mut current_segment_id = 0;
        let mut size = 0;
        let start = Instant::now();

        info!("[2pc] recovery replaying {} WAL segments", self.files.len());

        for (segment_file_id, file) in &self.files {
            // The filename reserves this segment ID even when the contents are
            // incomplete. A new writer must never reuse an existing ID.
            current_segment_id = *segment_file_id;

            let segment = match Segment::load(file).await {
                Ok(segment) => segment,
                Err(Error::Io(err)) if err.kind() == ErrorKind::UnexpectedEof => {
                    warn!(
                        r#"[2pc] recovery skipping WAL segment with incomplete header "{}""#,
                        file.display()
                    );
                    match remove_file(file).await {
                        Ok(()) => info!(
                            r#"[2pc] recovery removed WAL segment with incomplete header "{}""#,
                            file.display()
                        ),
                        Err(err) if err.kind() == ErrorKind::NotFound => {}
                        Err(err) => warn!(
                            r#"[2pc] recovery couldn't remove incomplete WAL segment "{}": {}"#,
                            file.display(),
                            err
                        ),
                    }
                    continue;
                }
                Err(err) => return Err(err),
            };

            if *segment_file_id != segment.segment_id {
                panic!("segment ID doesn't match file number");
            }

            size += segment.size();
            segment.replay(manager)?;

            // These can be checkpointed,
            // we are going to create a new segment at the end of
            // recovery.
            SegmentRegistry::get().record(segment.segment_id, SegmentStatus::Inactive);
        }

        info!(
            "[2pc] recovery replayed {:.3}MB in {:.3}s",
            size as f64 / 1024.0 / 1024.0,
            start.elapsed().as_secs_f32()
        );

        // Tell the manager to cleanup all transactions
        // in its state.
        manager.cleanup_all();

        // Create the writer with a brand new segment.
        WalWriter::new(&self.path, current_segment_id + 1, segment_size).await
    }
}
