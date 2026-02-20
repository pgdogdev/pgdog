use std::ops::Deref;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};
use std::fs::read_to_string;
use thiserror::Error;
use tokio::time::sleep;
use tokio::{select, signal::ctrl_c};
use tracing::{error, info, warn};

use crate::backend::databases::databases;
use crate::backend::replication::orchestrator::Orchestrator;
use crate::backend::schema::sync::config::ShardConfig;
use crate::backend::schema::sync::pg_dump::SyncState;
use crate::config::{Config, Users};
use crate::frontend::router::cli::RouterCli;

/// PgDog is a PostgreSQL pooler, proxy, load balancer and query router.
#[derive(Parser, Debug)]
#[command(name = "", version = concat!("PgDog v", env!("GIT_HASH")))]
pub struct Cli {
    /// Path to the configuration file. Default: "pgdog.toml"
    #[arg(short, long, default_value = "pgdog.toml")]
    pub config: PathBuf,
    /// Path to the users.toml file. Default: "users.toml"
    #[arg(short, long, default_value = "users.toml")]
    pub users: PathBuf,
    /// Connection URL.
    #[arg(short, long)]
    pub database_url: Option<Vec<String>>,
    /// Subcommand.
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Commands {
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

    /// Fingerprint a query.
    Fingerprint {
        #[arg(short, long)]
        query: Option<String>,
        #[arg(short, long)]
        path: Option<PathBuf>,
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

    /// Perform cluster configuration steps
    /// required for sharded operations.
    Setup {
        /// Database name.
        #[arg(long)]
        database: String,
    },
}

/// Fingerprint some queries.
#[allow(clippy::print_stdout)]
pub fn fingerprint(
    query: Option<String>,
    path: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(query) = query {
        let fingerprint = pg_query::fingerprint(&query)?;
        println!("{} [{}]", fingerprint.hex, fingerprint.value);
    } else if let Some(path) = path {
        let queries = read_to_string(path)?;
        for query in queries.split(";") {
            if query.trim().is_empty() {
                continue;
            }
            tracing::debug!("{}", query);
            if let Ok(fingerprint) = pg_query::fingerprint(query) {
                println!(
                    r#"
[[manual_query]]
fingerprint = "{}" #[{}]"#,
                    fingerprint.hex, fingerprint.value
                );
            }
        }
    }

    Ok(())
}

#[derive(Debug, Error)]
pub enum ConfigCheckError {
    #[error("need at least one of --config or --users")]
    MissingInput,

    #[error("I/O error on `{0}`: {1}")]
    Io(PathBuf, #[source] std::io::Error),

    #[error("TOML parse error in `{0}`: {1}")]
    Parse(PathBuf, #[source] toml::de::Error),

    #[error("{0:#?}")]
    Multiple(Vec<ConfigCheckError>),
}

/// Confirm that the configuration and users files are valid.
pub fn config_check(
    config_path: Option<PathBuf>,
    users_path: Option<PathBuf>,
) -> Result<(), ConfigCheckError> {
    if config_path.is_none() && users_path.is_none() {
        return Err(ConfigCheckError::MissingInput);
    }

    let mut errors: Vec<ConfigCheckError> = Vec::new();

    if let Some(path) = config_path {
        match read_to_string(&path) {
            Ok(s) => {
                if let Err(e) = toml::from_str::<Config>(&s) {
                    errors.push(ConfigCheckError::Parse(path.clone(), e));
                }
            }
            Err(e) => errors.push(ConfigCheckError::Io(path.clone(), e)),
        }
    }

    if let Some(path) = users_path {
        match read_to_string(&path) {
            Ok(s) => {
                if let Err(e) = toml::from_str::<Users>(&s) {
                    errors.push(ConfigCheckError::Parse(path.clone(), e));
                }
            }
            Err(e) => errors.push(ConfigCheckError::Io(path.clone(), e)),
        }
    }

    match errors.len() {
        0 => Ok(()),
        1 => Err(errors.into_iter().next().unwrap()),
        _ => Err(ConfigCheckError::Multiple(errors)),
    }
}

pub async fn data_sync(commands: Commands) -> Result<(), Box<dyn std::error::Error>> {
    use crate::backend::replication::logical::Error;

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
        let mut orchestrator = Orchestrator::new(
            &from_database,
            &to_database,
            &publication,
            replication_slot.as_ref().map(|s| s.as_str()),
        )?;
        orchestrator.load_schema().await?;

        if !skip_schema_sync {
            orchestrator.schema_sync_pre(true).await?;
        }

        if !replicate_only {
            select! {
                result = orchestrator.data_sync() => {
                    result?;
                }

                _ = ctrl_c() => {
                    warn!("abort signal received, waiting 5 seconds and performing cleanup");
                    sleep(Duration::from_secs(5)).await;

                    orchestrator.cleanup().await?;

                    return Err(Error::DataSyncAborted.into());
                }
            }
        }

        if !sync_only {
            let mut waiter = orchestrator.replicate().await?;

            select! {
                result = waiter.wait() => {
                    result?;
                }

                _ = ctrl_c() => {
                    warn!("abort signal received");

                    orchestrator.request_stop().await;

                    info!("waiting for replication to stop");

                    waiter.wait().await?;
                    orchestrator.cleanup().await?;

                    return Err(Error::DataSyncAborted.into());
                }
            }
        }
    } else {
        return Ok(());
    }

    Ok(())
}

#[allow(clippy::print_stdout)]
pub async fn schema_sync(commands: Commands) -> Result<(), Box<dyn std::error::Error>> {
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
        let mut orchestrator = Orchestrator::new(&from_database, &to_database, &publication, None)?;
        orchestrator.load_schema().await?;

        if dry_run {
            let state = if data_sync_complete {
                SyncState::PostData
            } else if cutover {
                SyncState::Cutover
            } else {
                SyncState::PreData
            };

            let schema = orchestrator.schema()?;
            for statement in schema.statements(state)? {
                println!("{}", statement.deref());
            }
            return Ok(());
        }

        if data_sync_complete {
            orchestrator.schema_sync_post(ignore_errors).await?;
        } else if cutover {
            orchestrator.schema_sync_cutover(ignore_errors).await?;
        } else {
            orchestrator.schema_sync_pre(ignore_errors).await?;
        }
    } else {
        return Ok(());
    }

    Ok(())
}

pub async fn setup(database: &str) -> Result<(), Box<dyn std::error::Error>> {
    let databases = databases();
    let schema_owner = databases.schema_owner(database)?;

    ShardConfig::sync_all(&schema_owner).await?;

    Ok(())
}

pub async fn route(commands: Commands) -> Result<(), Box<dyn std::error::Error>> {
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
