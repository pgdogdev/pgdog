use std::ops::Deref;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};
use std::fs::read_to_string;
use thiserror::Error;
use tokio::time::sleep;
use tokio::{select, signal::ctrl_c};
use tracing::{info, warn};

use crate::backend::databases::databases;
use crate::backend::replication::orchestrator::Orchestrator;
use crate::backend::schema::sync::config::ShardConfig;
use crate::backend::schema::sync::pg_dump::SyncState;
use crate::config::{Config, ConfigAndUsers, Role, Users};
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

    /// Generate a SCRAM-SHA-256 hash from a plaintext password.
    /// Output can be stored directly in users.toml.
    Hash {
        /// The plaintext password to hash.
        password: String,
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

    /// Open an interactive psql shell directly to a database server from the config.
    ///
    /// Looks up the database in pgdog.toml (and optionally the user in users.toml),
    /// resolves connection details, then exec-replaces this process with psql.
    /// Any arguments after `--` are forwarded verbatim to psql.
    ///
    /// Examples:
    ///   pgdog psql --database prod --user pgdog
    ///   pgdog psql --database prod --user pgdog -- -c "SELECT 1"
    ///   pgdog psql --database prod --user pgdog -- -f migration.sql
    Psql {
        /// Database name as configured in pgdog.toml.
        #[arg(short, long)]
        database: String,

        /// User name as configured in users.toml. When omitted, credentials are taken
        /// directly from the database entry in pgdog.toml.
        #[arg(short, long)]
        user: Option<String>,

        /// Shard number to connect to. Defaults to 0.
        #[arg(long, default_value = "0")]
        shard: usize,

        /// Extra arguments forwarded verbatim to psql (pass after `--`).
        #[arg(last = true)]
        psql_args: Vec<String>,
    },
}

/// Generate and print a SCRAM-SHA-256 hash from a plaintext password.
#[allow(clippy::print_stdout)]
pub fn hash_password(password: &str) {
    use rand::Rng;

    let salt: [u8; 16] = rand::rng().random();
    let iterations = std::num::NonZeroU32::new(4096).unwrap();
    println!(
        "{}",
        crate::auth::scram::generate_hash(password, iterations, &salt)
    );
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

/// FOR TESTING PURPOSES ONLY.
pub async fn replicate_and_cutover(commands: Commands) -> Result<(), Box<dyn std::error::Error>> {
    if let Commands::ReplicateAndCutover {
        from_database,
        to_database,
        publication,
        replication_slot,
    } = commands
    {
        let mut orchestrator = Orchestrator::new(
            &from_database,
            &to_database,
            &publication,
            replication_slot.clone(),
        )?;

        orchestrator.replicate_and_cutover().await?;
    }

    Ok(())
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
            replication_slot.clone(),
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

/// Open an interactive psql shell to a database server defined in the config.
///
/// Resolves connection details from pgdog.toml (and users.toml when `user` is given),
/// then exec-replaces this process with `psql`. On success this function never returns.
/// `extra_args` are forwarded verbatim to psql after the connection flags.
pub fn psql(
    database: &str,
    user: Option<&str>,
    shard: usize,
    extra_args: &[String],
    config: &ConfigAndUsers,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::process::CommandExt;

    // Prefer primary; fall back to any matching entry for the shard.
    let db = config
        .config
        .databases
        .iter()
        .find(|d| {
            d.name == database && d.shard == shard && matches!(d.role, Role::Primary | Role::Auto)
        })
        .or_else(|| {
            config
                .config
                .databases
                .iter()
                .find(|d| d.name == database && d.shard == shard)
        })
        .ok_or_else(|| {
            format!(
                "database '{}' (shard {}) not found in pgdog.toml",
                database, shard
            )
        })?;

    let db_name = db.database_name.as_deref().unwrap_or(&db.name);

    let (server_user, server_password) = if let Some(user_name) = user {
        let user_cfg = config
            .users
            .users
            .iter()
            .find(|u| {
                u.name == user_name
                    && (u.database == database
                        || u.all_databases
                        || u.databases.contains(&database.to_string()))
            })
            .ok_or_else(|| {
                format!(
                    "user '{}' for database '{}' not found in users.toml",
                    user_name, database
                )
            })?;

        let srv_user = db
            .user
            .as_deref()
            .or(user_cfg.server_user.as_deref())
            .unwrap_or(user_name);
        let srv_password = db
            .password
            .as_deref()
            .or(user_cfg.server_password.as_deref())
            .or(user_cfg.password.as_deref())
            .unwrap_or("");
        (srv_user.to_string(), srv_password.to_string())
    } else {
        let srv_user = db.user.as_deref().unwrap_or(database);
        let srv_password = db.password.as_deref().unwrap_or("");
        (srv_user.to_string(), srv_password.to_string())
    };

    let mut cmd = std::process::Command::new("psql");
    cmd.arg("-h")
        .arg(&db.host)
        .arg("-p")
        .arg(db.port.to_string())
        .arg("-U")
        .arg(&server_user)
        .arg("-d")
        .arg(db_name);

    if !server_password.is_empty() {
        cmd.env("PGPASSWORD", &server_password);
    }

    cmd.args(extra_args);

    // exec replaces the current process; returns only on failure.
    let err = cmd.exec();
    Err(Box::new(err))
}
