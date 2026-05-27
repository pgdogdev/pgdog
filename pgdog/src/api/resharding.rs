use crate::backend::replication::Error;
use crate::backend::replication::orchestrator::Orchestrator;

pub struct Options {
    /// Source database name.
    from_database: String,

    /// Destination database.
    to_database: String,

    /// Publication name.
    publication: String,

    /// Name of the replication slot to create/use.
    replication_slot: Option<String>,

    /// Replicate or copy data over.
    replicate_only: bool,

    /// Replicate or copy data over.
    sync_only: bool,

    /// Don't perform pre-data schema sync.
    skip_schema_sync: bool,
}

pub fn reshard(options: Options) -> Result<(), Error> {
    let Options {
        from_database,
        to_database,
        publication,
        replication_slot,
        replicate_only,
        sync_only,
        skip_schema_sync,
    } = options;

    let orchestrator =
        Orchestrator::new(&from_database, &to_database, &publication, replication_slot);

    Ok(())
}
