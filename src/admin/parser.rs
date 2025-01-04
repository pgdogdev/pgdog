//! Admin command parser.

use super::{pause::Pause, Command, Error};

/// Admin command parser.
pub struct Parser;

impl Parser {
    /// Parse the query and return a command we can execute.
    pub fn parse(sql: &str) -> Result<impl Command, Error> {
        let sql = sql.trim().replace(";", "").to_lowercase();

        match sql.split(" ").next().ok_or(Error::Syntax)? {
            "pause" | "resume" => Pause::parse(&sql),
            _ => Err(Error::Syntax),
        }
    }
}
