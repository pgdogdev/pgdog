//! Admin command parser.

use super::{
    pause::Pause, prelude::Message, reconnect::Reconnect, show_clients::ShowClients, Command, Error,
};

use tracing::debug;

/// Parser result.
pub enum ParseResult {
    Pause(Pause),
    Reconnect(Reconnect),
    ShowClients(ShowClients),
}

impl ParseResult {
    /// Execute command.
    pub async fn execute(&self) -> Result<Vec<Message>, Error> {
        use ParseResult::*;

        match self {
            Pause(pause) => pause.execute().await,
            Reconnect(reconnect) => reconnect.execute().await,
            ShowClients(show_clients) => show_clients.execute().await,
        }
    }

    /// Get command name.
    pub fn name(&self) -> String {
        use ParseResult::*;

        match self {
            Pause(pause) => pause.name(),
            Reconnect(reconnect) => reconnect.name(),
            ShowClients(show_clients) => show_clients.name(),
        }
    }
}

/// Admin command parser.
pub struct Parser;

impl Parser {
    /// Parse the query and return a command we can execute.
    pub fn parse(sql: &str) -> Result<ParseResult, Error> {
        let sql = sql.trim().replace(";", "").to_lowercase();
        let mut iter = sql.split(" ");

        Ok(match iter.next().ok_or(Error::Syntax)?.trim() {
            "pause" | "resume" => ParseResult::Pause(Pause::parse(&sql)?),
            "reconnect" => ParseResult::Reconnect(Reconnect::parse(&sql)?),
            "show" => match iter.next().ok_or(Error::Syntax)?.trim() {
                "clients" => ParseResult::ShowClients(ShowClients::parse(&sql)?),
                command => {
                    debug!("unknown admin show command: '{}'", command);
                    return Err(Error::Syntax);
                }
            },
            command => {
                debug!("unknown admin command: {}", command);
                return Err(Error::Syntax);
            }
        })
    }
}
