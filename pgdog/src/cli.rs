use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tokio::{select, signal::ctrl_c};
use tracing::{info, warn};

use crate::api::Task;
use crate::api::resharding::ReshardTask;
use crate::api::run_task;
use crate::api::schema_sync::{SchemaSyncPhase, SchemaSyncTask};
use crate::api::tasks_storage;
use crate::backend::databases::databases;
use crate::backend::replication::orchestrator::Orchestrator;
use crate::backend::schema::sync::config::ShardConfig;
use crate::frontend::router::cli::RouterCli;

/// PgDog is a PostgreSQL pooler, proxy, load balancer and query router.
#[derive(Parser, Debug)]
#[command(name = "", version = concat!("PgDog v", env!("GIT_HASH")))]
pub(crate) struct Cli {
    /// Path to the configuration file. Default: "pgdog.toml"
    #[arg(short, long, default_value = "pgdog.toml")]
    pub(crate) config: PathBuf,
    /// Path to the users.toml file. Default: "users.toml"
    #[arg(short, long, default_value = "users.toml")]
    pub(crate) users: PathBuf,
    /// Connection URL.
    #[arg(short, long)]
    pub(crate) database_url: Option<Vec<String>>,
    /// Subcommand.
    #[command(subcommand)]
    pub(crate) command: Option<Commands>,
}

#[derive(Subcommand, Debug, Clone)]
pub(crate) enum Commands {
    /// Start PgDog.
    Run {
        /// Size of the connection pool.
        #[arg(short, long)]
        pool_size: Option<usize>,

        /// Minimum number of idle connections to maintain open.
        #[arg(short, long)]
        min_pool_size: Option<usize>,

        /// Run the pooler in session mode.
        #[arg(short, long)]
        session_mode: Option<bool>,
    },

    /// Generate a SCRAM-SHA-256 hash from a plaintext password.
    /// Output can be stored directly in users.toml.
    Hash {
        /// The plaintext password to hash.
        password: String,
    },

    /// Execute the router on the queries.
    Route {
        /// User in users.toml.
        #[arg(short, long)]
        user: String,

        /// Database in pgdog.toml.
        #[arg(short, long)]
        database: String,

        /// Path to the file containing the queries.
        #[arg(short, long)]
        file: PathBuf,
    },

    /// Check configuration files for errors.
    Configcheck,

    /// Copy data from source to destination cluster
    /// using logical replication.
    DataSync {
        /// Source database name.
        #[arg(long)]
        from_database: String,

        /// Publication name.
        #[arg(long)]
        publication: String,

        /// Destination database.
        #[arg(long)]
        to_database: String,

        /// Replicate or copy data over.
        #[arg(long, default_value = "false")]
        replicate_only: bool,

        /// Replicate or copy data over.
        #[arg(long, default_value = "false")]
        sync_only: bool,

        /// Name of the replication slot to create/use.
        #[arg(long)]
        replication_slot: Option<String>,

        /// Don't perform pre-data schema sync.
        #[arg(long)]
        skip_schema_sync: bool,
    },

    /// Copy schema from source to destination cluster.
    SchemaSync {
        /// Source database name.
        #[arg(long)]
        from_database: String,
        /// Publication name.
        #[arg(long)]
        publication: String,

        /// Destination database.
        #[arg(long)]
        to_database: String,

        /// Dry run. Print schema commands, don't actually execute them.
        #[arg(long)]
        dry_run: bool,

        /// Ignore errors.
        #[arg(long)]
        ignore_errors: bool,

        /// Data sync has been complete.
        #[arg(long)]
        data_sync_complete: bool,

        /// Execute cutover statements.
        #[arg(long)]
        cutover: bool,
    },

    /// For testing purposes only.
    ///
    /// Performs the entire schema sync, data sync and replication flow
    /// with cutover trigger.
    ///
    /// Use for internal testing only. To do this in production,
    /// use the admin database RESHARD command.
    ///
    ReplicateAndCutover {
        /// Source database name.
        #[arg(long)]
        from_database: String,

        /// Destination database name.
        #[arg(long)]
        to_database: String,

        /// Publication name.
        #[arg(long)]
        publication: String,

        /// Replication slot name.
        #[arg(long)]
        replication_slot: Option<String>,
    },

    /// Perform cluster configuration steps
    /// required for sharded operations.
    Setup {
        /// Database name.
        #[arg(long)]
        database: String,
    },
}

/// Generate and print a SCRAM-SHA-256 hash from a plaintext password.
#[allow(clippy::print_stdout)]
pub(crate) fn hash_password(password: &str) {
    use rand::Rng;

    let salt: [u8; 16] = rand::rng().random();
    let iterations = std::num::NonZeroU32::new(4096).unwrap();
    println!(
        "{}",
        crate::auth::scram::generate_hash(password, iterations, &salt)
    );
}

/// Run an api task to completion in the foreground, cancelling it on Ctrl-C so
/// it can wind down (e.g. stop replication) instead of the process being
/// hard-killed. Returns the task output, or its error/cancellation outcome.
async fn run_to_completion<T: Task + 'static>(
    task: T,
) -> Result<T::Output, Box<dyn std::error::Error>>
where
    T::Error: std::error::Error + 'static,
{
    let mut waiter = run_task(task);
    let id = waiter.id();

    loop {
        select! {
            result = &mut waiter => return Ok(result?),
            signal = ctrl_c() => {
                signal?;
                warn!("interrupt received, cancelling task {id}");
                tasks_storage().cancel_task(id);
            }
        }
    }
}

/// FOR TESTING PURPOSES ONLY.
pub(crate) async fn replicate_and_cutover(
    commands: Commands,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Commands::ReplicateAndCutover {
        from_database,
        to_database,
        publication,
        replication_slot,
    } = commands
    {
        let orchestrator = Orchestrator::new(
            &from_database,
            &to_database,
            &publication,
            replication_slot.clone(),
        )?;

        run_to_completion(
            ReshardTask::builder()
                .orchestrator(orchestrator)
                .auto_cutover(true)
                .build(),
        )
        .await?;
    }

    Ok(())
}

pub(crate) async fn data_sync(commands: Commands) -> Result<(), Box<dyn std::error::Error>> {
    if let Commands::DataSync {
        from_database,
        to_database,
        publication,
        replicate_only,
        sync_only,
        replication_slot,
        skip_schema_sync,
    } = commands
    {
        let orchestrator =
            Orchestrator::new(&from_database, &to_database, &publication, replication_slot)?;

        run_to_completion(
            ReshardTask::builder()
                .orchestrator(orchestrator)
                .skip_schema_sync(skip_schema_sync)
                .replicate_only(replicate_only)
                .sync_only(sync_only)
                .build(),
        )
        .await?;
    }

    Ok(())
}

pub(crate) async fn schema_sync(commands: Commands) -> Result<(), Box<dyn std::error::Error>> {
    if let Commands::SchemaSync {
        from_database,
        to_database,
        publication,
        dry_run,
        ignore_errors,
        data_sync_complete,
        cutover,
    } = commands
    {
        let phase = if data_sync_complete {
            SchemaSyncPhase::Post
        } else if cutover {
            SchemaSyncPhase::Cutover
        } else {
            SchemaSyncPhase::Pre
        };

        let orchestrator = Orchestrator::new(&from_database, &to_database, &publication, None)?;

        run_to_completion(
            SchemaSyncTask::builder()
                .orchestrator(orchestrator)
                .phase(phase)
                .ignore_errors(ignore_errors)
                .dry_run(dry_run)
                .build(),
        )
        .await?;
    } else {
        return Ok(());
    }

    Ok(())
}

pub(crate) async fn setup(database: &str) -> Result<(), Box<dyn std::error::Error>> {
    let databases = databases();
    let schema_owner = databases.schema_owner(database)?;

    ShardConfig::sync_all(&schema_owner).await?;

    Ok(())
}

pub(crate) async fn route(commands: Commands) -> Result<(), Box<dyn std::error::Error>> {
    if let Commands::Route {
        user,
        database,
        file,
    } = commands
    {
        let cli = RouterCli::new(&database, &user, file).await?;
        let cmds = cli.run()?;

        for cmd in cmds {
            info!("{:?}", cmd);
        }
    }

    Ok(())
}
