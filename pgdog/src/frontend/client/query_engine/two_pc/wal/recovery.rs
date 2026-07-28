//! Handle recovering the state
//! of the 2pc manager from WAL segments.

use super::{super::Manager, EXTENSION, Segment, WalWriter};
use crate::net::Error;
use std::path::PathBuf;
use tokio::fs::read_dir;
use tokio::time::Instant;
use tracing::{info, warn};

pub(crate) struct Recovery {
    pub(crate) path: PathBuf,
    pub(super) files: Vec<PathBuf>,
}

impl Recovery {
    pub(crate) async fn new(path: &PathBuf) -> Result<Self, Error> {
        let mut dir = read_dir(path).await?;
        let mut files = vec![];

        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            let mut skip = false;

            if let Some(extenstion) = path.extension() {
                let extension = extenstion
                    .to_str()
                    .expect("wal 2pc extension must be utf-8");

                if !EXTENSION.contains(extension) {
                    skip = true
                }
            } else {
                skip = true;
            }

            if let Some(prefix) = path.file_prefix() {
                if skip {
                    continue;
                }

                files.push((
                    prefix
                        .to_str()
                        .expect("wal 2pc prefix must be utf-8")
                        .to_string(),
                    path.clone(),
                ))
            } else {
                skip = true;
            }

            if skip {
                warn!(
                    r#"[2pc] recovery ignoring unknown file "{}""#,
                    path.display()
                );
            }
        }

        files.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(Self {
            path: path.clone(),
            files: files.into_iter().map(|file| file.1).collect(),
        })
    }

    pub(crate) async fn run(self, manager: &Manager) -> Result<WalWriter, Error> {
        let mut counter = 0;
        let mut size = 0;
        let start = Instant::now();

        info!("[2pc] recovery replaying {} WAL segments", self.files.len());

        for file in &self.files {
            let segment = Segment::load(&file).await?;
            counter = segment.counter;
            size += segment.size();

            segment.replay(manager)?;
        }

        info!(
            "[2pc] recovery replayed {:.3}MB in {:.3}s",
            size as f64 / 1024.0 / 1024.0,
            start.elapsed().as_secs_f32()
        );

        // Tell the manager to cleanup all transactions
        // in its state.
        manager.cleanup_all();

        WalWriter::new(&self.path, counter + 1).await
    }
}
