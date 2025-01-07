use std::path::PathBuf;

use clap::Parser;

/// pgDog is a PostgreSQL pooler, proxy, load balancer and
/// query router.
#[derive(Parser, Debug)]
pub struct Cli {
    /// Path to the configuration file. Default: "pgdog.toml"
    #[arg(default_value = "pgdog.toml")]
    pub config: PathBuf,
    /// Path to the users.toml file. Default: "users.toml"
    #[arg(default_value = "users.toml")]
    pub users: PathBuf,
}
