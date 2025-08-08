use std::path::PathBuf;

use clap::{Parser, Subcommand};
use std::fs::read_to_string;
use thiserror::Error;
use tokio::{select, signal::ctrl_c};
use tracing::error;

use crate::backend::schema::sync::pg_dump::{PgDump, SyncState};
use crate::backend::{databases::databases, replication::logical::Publisher};
use crate::config::{Config, Users};

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
    /// Run pgDog.
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

    /// Check configuration.
    Configcheck {
        /// Path to the configuration file.
        #[arg(short, long)]
        config: Option<PathBuf>,
        /// Path to the users.toml file.
        #[arg(short, long)]
        users: Option<PathBuf>,
    },

    /// Copy data from source to destination cluster
    /// using logical replication.
    DataSync {
        /// Source database name.
        #[arg(long)]
        from_database: String,
        /// Source user name.
        #[arg(long)]
        from_user: String,
        /// Publication name.
        #[arg(long)]
        publication: String,

        /// Destination database.
        #[arg(long)]
        to_database: String,
        /// Destination user name.
        #[arg(long)]
        to_user: String,

        /// Replicate or copy data over.
        #[arg(long, default_value = "false")]
        replicate: bool,
    },

    /// Schema synchronization between source and destination clusters.
    SchemaSync {
        /// Source database name.
        #[arg(long)]
        from_database: String,
        /// Source user name.
        #[arg(long)]
        from_user: String,
        /// Publication name.
        #[arg(long)]
        publication: String,

        /// Destination database.
        #[arg(long)]
        to_database: String,
        /// Destination user name.
        #[arg(long)]
        to_user: String,

        /// Dry run. Print schema commands, don't actually execute them.
        #[arg(long)]
        dry_run: bool,

        /// Ignore errors.
        #[arg(long)]
        ignore_errors: bool,

        /// Data sync has been complete.
        #[arg(long)]
        data_sync_complete: bool,
    },
}

/// Fingerprint some queries.
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
    let (source, destination, publication, replicate) = if let Commands::DataSync {
        from_database,
        from_user,
        to_database,
        to_user,
        publication,
        replicate,
    } = commands
    {
        let source = databases().cluster((from_user.as_str(), from_database.as_str()))?;
        let dest = databases().cluster((to_user.as_str(), to_database.as_str()))?;

        (source, dest, publication, replicate)
    } else {
        return Ok(());
    };

    let mut publication = Publisher::new(&source, &publication);
    if replicate {
        if let Err(err) = publication.replicate(&destination).await {
            error!("{}", err);
        }
    } else {
        select! {
            result = publication.data_sync(&destination) => {
                if let Err(err) = result {
                    error!("{}", err);
                }
            }

            _ = ctrl_c() => (),

        }
    }

    Ok(())
}

pub async fn schema_sync(commands: Commands) -> Result<(), Box<dyn std::error::Error>> {
    let (source, destination, publication, dry_run, ignore_errors, data_sync_complete) =
        if let Commands::SchemaSync {
            from_database,
            from_user,
            to_database,
            to_user,
            publication,
            dry_run,
            ignore_errors,
            data_sync_complete,
        } = commands
        {
            let source = databases().cluster((from_user.as_str(), from_database.as_str()))?;
            let dest = databases().cluster((to_user.as_str(), to_database.as_str()))?;

            (
                source,
                dest,
                publication,
                dry_run,
                ignore_errors,
                data_sync_complete,
            )
        } else {
            return Ok(());
        };

    let dump = PgDump::new(&source, &publication);
    let output = dump.dump().await?;
    let state = if data_sync_complete {
        SyncState::PostData
    } else {
        SyncState::PreData
    };

    for output in output {
        if dry_run {
            let queries = output.statements(state)?;
            for query in queries {
                println!("{}", query);
            }
        } else {
            output.restore(&destination, ignore_errors, state).await?;
        }
    }

    Ok(())
}
