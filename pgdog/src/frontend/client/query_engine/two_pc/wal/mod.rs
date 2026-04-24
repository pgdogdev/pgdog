//! Two-phase commit write-ahead log.
//!
//! See [`record`] for the on-disk record format.

mod error;
mod record;
mod recovery;
mod segment;
mod writer;

use std::path::Path;

use tokio::io::AsyncWriteExt;

pub use error::Error;
pub use writer::Wal;

use super::Manager;

impl Wal {
    /// Probe the configured WAL directory, replay any existing log into
    /// `manager`, and spawn the writer task. Returns `Err` if the
    /// directory isn't usable or recovery fails; the caller is
    /// responsible for deciding whether to continue running without WAL
    /// durability.
    ///
    /// Multiple PgDog instances must be configured with distinct
    /// directories; sharing one between processes will corrupt the log.
    ///
    /// TODO: acquire a flock on `<dir>/.lock` (with our PID + start time
    /// written into it for diagnostics) so accidental sharing is caught
    /// at startup with a clear error rather than silently corrupting.
    pub async fn open(manager: &Manager) -> Result<Wal, Error> {
        let dir = &crate::config::config()
            .config
            .general
            .two_phase_commit_wal_dir;
        probe(dir).await?;
        let segment = recovery::recover_transactions(manager, dir).await?;
        Ok(Wal::new(segment))
    }
}

/// Diagnose whether `dir` is usable as a WAL directory by exercising the
/// operations the writer and recovery code will perform: directory
/// existence, listing, creating + writing + fsync of a probe file.
async fn probe(dir: &Path) -> Result<(), Error> {
    tokio::fs::create_dir_all(dir)
        .await
        .map_err(|source| Error::DirNotAccessible {
            dir: dir.to_path_buf(),
            source,
        })?;

    let _ = tokio::fs::read_dir(dir)
        .await
        .map_err(|source| Error::DirNotReadable {
            dir: dir.to_path_buf(),
            source,
        })?;

    let probe_path = dir.join(".probe");
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&probe_path)
        .await
        .map_err(|source| Error::DirNotWritable {
            dir: dir.to_path_buf(),
            source,
        })?;
    file.write_all(b"pgdog wal probe\n")
        .await
        .map_err(|source| Error::DirNotWritable {
            dir: dir.to_path_buf(),
            source,
        })?;
    file.sync_all()
        .await
        .map_err(|source| Error::DirNotWritable {
            dir: dir.to_path_buf(),
            source,
        })?;
    drop(file);
    let _ = tokio::fs::remove_file(&probe_path).await;

    Ok(())
}
