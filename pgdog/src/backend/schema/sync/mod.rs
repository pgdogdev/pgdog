pub mod config;
pub mod error;
pub mod pg_dump;
pub mod progress;

use std::ops::Deref;

pub use config::ShardConfig;
pub use error::Error;
pub use pg_dump::{PgDump, Statement, SyncState};
use tracing::info;

use crate::backend::{replication::status::ReplicationTracker, Cluster};

pub async fn schema_sync(
    source: &Cluster,
    destination: &Cluster,
    publication: &str,
    state: SyncState,
    dry_run: bool,
    ignore_errors: bool,
) -> Result<(), Error> {
    ReplicationTracker::start(source, destination);

    let dump = PgDump::new(&source, &publication);

    let output = dump.dump().await?;

    if state == SyncState::PreData {
        ShardConfig::sync_all(&destination).await?;
    }

    if dry_run {
        let queries = output.statements(state)?;
        for query in queries {
            info!("{}", query.deref());
        }
    } else {
        output.restore(&destination, ignore_errors, state).await?;
    }

    Ok(())
}
