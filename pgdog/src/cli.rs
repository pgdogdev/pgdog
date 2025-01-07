use std::path::PathBuf;

use clap::Parser;

/// pgDog is a PostgreSQL pooler, proxy, load balancer and
/// query router.
#[derive(Parser, Debug)]
pub struct Cli {
    /// Path to the configuration file. Default: "pgdog.toml"
    #[clap(default_value = "pgdog.toml")]
    config: PathBuf,
    /// Path to the users.toml file. Default: "users.toml"
    #[clap(default_value = "users.toml")]
    users: PathBuf,
}
