pub mod command;
pub mod database;
pub mod databases;
pub mod shard;

pub use command::Command;
pub use database::Database;
pub use shard::Shard;

use crate::backend::Server;

pub struct Connection {
    server: Option<Server>,
}
