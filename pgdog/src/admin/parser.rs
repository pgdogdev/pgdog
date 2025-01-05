//! Admin command parser.

use super::{pause::Pause, prelude::Message, reconnect::Reconnect, Command, Error};

/// Parser result.
pub enum ParseResult {
    Pause(Pause),
    Reconnect(Reconnect),
}

impl ParseResult {
    /// Execute command.
    pub async fn execute(&self) -> Result<Vec<Message>, Error> {
        use ParseResult::*;

        match self {
            Pause(pause) => pause.execute().await,
            Reconnect(reconnect) => reconnect.execute().await,
        }
    }

    /// Get command name.
    pub fn name(&self) -> String {
        use ParseResult::*;

        match self {
            Pause(pause) => pause.name(),
            Reconnect(reconnect) => reconnect.name(),
        }
    }
}

/// Admin command parser.
pub struct Parser;

impl Parser {
    /// Parse the query and return a command we can execute.
    pub fn parse(sql: &str) -> Result<ParseResult, Error> {
        let sql = sql.trim().replace(";", "").to_lowercase();

        Ok(match sql.split(" ").next().ok_or(Error::Syntax)? {
            "pause" | "resume" => ParseResult::Pause(Pause::parse(&sql)?),
            "reconnect" => ParseResult::Reconnect(Reconnect::parse(&sql)?),
            _ => return Err(Error::Syntax),
        })
    }
}
