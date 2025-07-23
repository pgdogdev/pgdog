use std::path::PathBuf;

use clap::{Parser, Subcommand};
use std::fs::read_to_string;
use tokio::{select, signal::ctrl_c};
use tracing::error;

use crate::backend::{databases::databases, replication::logical::Publisher};

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
