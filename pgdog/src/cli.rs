use std::ops::Deref;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use std::fs::read_to_string;
use thiserror::Error;
use tokio::{select, signal::ctrl_c};
use tracing::{error, info};

use crate::backend::schema::sync::config::ShardConfig;
use crate::backend::schema::sync::pg_dump::{PgDump, SyncState};
use crate::backend::{databases::databases, replication::logical::Publisher};
use crate::config::{config, Config, Users};
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
    let (source, destination, publication, replicate_only, sync_only, replication_slot) =
        if let Commands::DataSync {
            from_database,
            to_database,
            publication,
            replicate_only,
            sync_only,
            replication_slot,
        } = commands
        {
            let source = databases().schema_owner(&from_database)?;
            let dest = databases().schema_owner(&to_database)?;

            (
                source,
                dest,
                publication,
                replicate_only,
                sync_only,
                replication_slot,
            )
        } else {
            return Ok(());
        };

    let mut publication = Publisher::new(
        &source,
        &publication,
        config().config.general.query_parser_engine,
    );
    if replicate_only {
        if let Err(err) = publication.replicate(&destination, replication_slot).await {
            error!("{}", err);
        }
    } else {
        select! {
            result = publication.data_sync(&destination, !sync_only, replication_slot) => {
                if let Err(err) = result {
                    error!("{}", err);
                }
            }

            _ = ctrl_c() => (),

        }
    }

    Ok(())
}

#[allow(clippy::print_stdout)]
pub async fn schema_sync(commands: Commands) -> Result<(), Box<dyn std::error::Error>> {
    let (source, destination, publication, dry_run, ignore_errors, data_sync_complete, cutover) =
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
            let source = databases().schema_owner(&from_database)?;
            let dest = databases().schema_owner(&to_database)?;

            (
                source,
                dest,
                publication,
                dry_run,
                ignore_errors,
                data_sync_complete,
                cutover,
            )
        } else {
            return Ok(());
        };

    let dump = PgDump::new(&source, &publication);
    let output = dump.dump().await?;
    let state = if data_sync_complete {
        SyncState::PostData
    } else if cutover {
        SyncState::Cutover
    } else {
        SyncState::PreData
    };

    if state == SyncState::PreData {
        ShardConfig::sync_all(&destination).await?;
    }

    if dry_run {
        let queries = output.statements(state)?;
        for query in queries {
            println!("{}", query.deref());
        }
    } else {
        output.restore(&destination, ignore_errors, state).await?;
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
