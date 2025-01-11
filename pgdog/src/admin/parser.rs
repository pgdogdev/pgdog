//! Admin command parser.

use super::{
    pause::Pause, prelude::Message, reconnect::Reconnect, reload::Reload,
    show_clients::ShowClients, show_pools::ShowPools, Command, Error,
};

use tracing::debug;

/// Parser result.
pub enum ParseResult {
    Pause(Pause),
    Reconnect(Reconnect),
    ShowClients(ShowClients),
    Reload(Reload),
    ShowPools(ShowPools),
}

impl ParseResult {
    /// Execute command.
    pub async fn execute(&self) -> Result<Vec<Message>, Error> {
        use ParseResult::*;

        match self {
            Pause(pause) => pause.execute().await,
            Reconnect(reconnect) => reconnect.execute().await,
            ShowClients(show_clients) => show_clients.execute().await,
            Reload(reload) => reload.execute().await,
            ShowPools(show_pools) => show_pools.execute().await,
        }
    }

    /// Get command name.
    pub fn name(&self) -> String {
        use ParseResult::*;

        match self {
            Pause(pause) => pause.name(),
            Reconnect(reconnect) => reconnect.name(),
            ShowClients(show_clients) => show_clients.name(),
            Reload(reload) => reload.name(),
            ShowPools(show_pools) => show_pools.name(),
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
            "reload" => ParseResult::Reload(Reload::parse(&sql)?),
            "show" => match iter.next().ok_or(Error::Syntax)?.trim() {
                "clients" => ParseResult::ShowClients(ShowClients::parse(&sql)?),
                "pools" => ParseResult::ShowPools(ShowPools::parse(&sql)?),
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
