//! Admin command parser.

use super::{
    pause::Pause, prelude::Message, reconnect::Reconnect, reload::Reload,
    reset_query_cache::ResetQueryCache, set::Set, setup_schema::SetupSchema,
    show_clients::ShowClients, show_config::ShowConfig, show_lists::ShowLists,
    show_peers::ShowPeers, show_pools::ShowPools, show_prepared_statements::ShowPreparedStatements,
    show_query_cache::ShowQueryCache, show_servers::ShowServers, show_stats::ShowStats,
    show_version::ShowVersion, shutdown::Shutdown, Command, Error,
};

use tracing::debug;

/// Parser result.
pub enum ParseResult {
    Pause(Pause),
    Reconnect(Reconnect),
    ShowClients(ShowClients),
    Reload(Reload),
    ShowPools(ShowPools),
    ShowConfig(ShowConfig),
    ShowServers(ShowServers),
    ShowPeers(ShowPeers),
    ShowQueryCache(ShowQueryCache),
    ResetQueryCache(ResetQueryCache),
    ShowStats(ShowStats),
    ShowVersion(ShowVersion),
    SetupSchema(SetupSchema),
    Shutdown(Shutdown),
    ShowLists(ShowLists),
    ShowPrepared(ShowPreparedStatements),
    Set(Set),
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
            ShowConfig(show_config) => show_config.execute().await,
            ShowServers(show_servers) => show_servers.execute().await,
            ShowPeers(show_peers) => show_peers.execute().await,
            ShowQueryCache(show_query_cache) => show_query_cache.execute().await,
            ResetQueryCache(reset_query_cache) => reset_query_cache.execute().await,
            ShowStats(show_stats) => show_stats.execute().await,
            ShowVersion(show_version) => show_version.execute().await,
            SetupSchema(setup_schema) => setup_schema.execute().await,
            Shutdown(shutdown) => shutdown.execute().await,
            ShowLists(show_lists) => show_lists.execute().await,
            ShowPrepared(cmd) => cmd.execute().await,
            Set(set) => set.execute().await,
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
            ShowConfig(show_config) => show_config.name(),
            ShowServers(show_servers) => show_servers.name(),
            ShowPeers(show_peers) => show_peers.name(),
            ShowQueryCache(show_query_cache) => show_query_cache.name(),
            ResetQueryCache(reset_query_cache) => reset_query_cache.name(),
            ShowStats(show_stats) => show_stats.name(),
            ShowVersion(show_version) => show_version.name(),
            SetupSchema(setup_schema) => setup_schema.name(),
            Shutdown(shutdown) => shutdown.name(),
            ShowLists(show_lists) => show_lists.name(),
            ShowPrepared(show) => show.name(),
            Set(set) => set.name(),
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
            "shutdown" => ParseResult::Shutdown(Shutdown::parse(&sql)?),
            "reconnect" => ParseResult::Reconnect(Reconnect::parse(&sql)?),
            "reload" => ParseResult::Reload(Reload::parse(&sql)?),
            "show" => match iter.next().ok_or(Error::Syntax)?.trim() {
                "clients" => ParseResult::ShowClients(ShowClients::parse(&sql)?),
                "pools" => ParseResult::ShowPools(ShowPools::parse(&sql)?),
                "config" => ParseResult::ShowConfig(ShowConfig::parse(&sql)?),
                "servers" => ParseResult::ShowServers(ShowServers::parse(&sql)?),
                "peers" => ParseResult::ShowPeers(ShowPeers::parse(&sql)?),
                "query_cache" => ParseResult::ShowQueryCache(ShowQueryCache::parse(&sql)?),
                "stats" => ParseResult::ShowStats(ShowStats::parse(&sql)?),
                "version" => ParseResult::ShowVersion(ShowVersion::parse(&sql)?),
                "lists" => ParseResult::ShowLists(ShowLists::parse(&sql)?),
                "prepared" => ParseResult::ShowPrepared(ShowPreparedStatements::parse(&sql)?),
                command => {
                    debug!("unknown admin show command: '{}'", command);
                    return Err(Error::Syntax);
                }
            },
            "reset" => match iter.next().ok_or(Error::Syntax)?.trim() {
                "query_cache" => ParseResult::ResetQueryCache(ResetQueryCache::parse(&sql)?),
                command => {
                    debug!("unknown admin show command: '{}'", command);
                    return Err(Error::Syntax);
                }
            },
            "setup" => match iter.next().ok_or(Error::Syntax)?.trim() {
                "schema" => ParseResult::SetupSchema(SetupSchema::parse(&sql)?),
                command => {
                    debug!("unknown admin show command: '{}'", command);
                    return Err(Error::Syntax);
                }
            },
            // TODO: This is not ready yet. We have a race and
            // also the changed settings need to be propagated
            // into the pools.
            "set" => ParseResult::Set(Set::parse(&sql)?),
            command => {
                debug!("unknown admin command: {}", command);
                return Err(Error::Syntax);
            }
        })
    }
}
