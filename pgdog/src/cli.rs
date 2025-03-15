use std::path::PathBuf;

use clap::{Parser, Subcommand};
use std::fs::read_to_string;

/// pgDog is a PostgreSQL pooler, proxy, load balancer and
/// query router.
#[derive(Parser, Debug)]
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

#[derive(Subcommand, Debug)]
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

    Schema,
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
