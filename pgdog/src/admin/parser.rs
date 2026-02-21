//! Admin command parser.

use super::{
    ban::Ban, healthcheck::Healthcheck, maintenance_mode::MaintenanceMode, pause::Pause,
    prelude::Message, probe::Probe, reconnect::Reconnect, reload::Reload,
    reset_query_cache::ResetQueryCache, reshard::Reshard, set::Set, setup_schema::SetupSchema,
    show_client_memory::ShowClientMemory, show_clients::ShowClients, show_config::ShowConfig,
    show_instance_id::ShowInstanceId, show_lists::ShowLists, show_mirrors::ShowMirrors,
    show_peers::ShowPeers, show_pools::ShowPools, show_prepared_statements::ShowPreparedStatements,
    show_query_cache::ShowQueryCache, show_replication::ShowReplication,
    show_resharding::ShowResharding, show_server_memory::ShowServerMemory,
    show_servers::ShowServers, show_stats::ShowStats, show_transactions::ShowTransactions,
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
    ShowTransactions(ShowTransactions),
    ShowMirrors(ShowMirrors),
    ShowVersion(ShowVersion),
    ShowInstanceId(ShowInstanceId),
    SetupSchema(SetupSchema),
    Shutdown(Shutdown),
    ShowLists(ShowLists),
    ShowPrepared(ShowPreparedStatements),
    ShowReplication(ShowReplication),
    ShowResharding(ShowResharding),
    ShowServerMemory(ShowServerMemory),
    ShowClientMemory(ShowClientMemory),
    Set(Set),
    Ban(Ban),
    Probe(Probe),
    MaintenanceMode(MaintenanceMode),
    Healthcheck(Healthcheck),
    Reshard(Reshard),
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
            ShowTransactions(show_transactions) => show_transactions.execute().await,
            ShowMirrors(show_mirrors) => show_mirrors.execute().await,
            ShowVersion(show_version) => show_version.execute().await,
            ShowInstanceId(show_instance_id) => show_instance_id.execute().await,
            SetupSchema(setup_schema) => setup_schema.execute().await,
            Shutdown(shutdown) => shutdown.execute().await,
            ShowLists(show_lists) => show_lists.execute().await,
            ShowPrepared(cmd) => cmd.execute().await,
            ShowReplication(show_replication) => show_replication.execute().await,
            ShowResharding(cmd) => cmd.execute().await,
            ShowServerMemory(show_server_memory) => show_server_memory.execute().await,
            ShowClientMemory(show_client_memory) => show_client_memory.execute().await,
            Set(set) => set.execute().await,
            Ban(ban) => ban.execute().await,
            Probe(probe) => probe.execute().await,
            MaintenanceMode(maintenance_mode) => maintenance_mode.execute().await,
            Healthcheck(healthcheck) => healthcheck.execute().await,
            Reshard(reshard) => reshard.execute().await,
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
            ShowTransactions(show_transactions) => show_transactions.name(),
            ShowMirrors(show_mirrors) => show_mirrors.name(),
            ShowVersion(show_version) => show_version.name(),
            ShowInstanceId(show_instance_id) => show_instance_id.name(),
            SetupSchema(setup_schema) => setup_schema.name(),
            Shutdown(shutdown) => shutdown.name(),
            ShowLists(show_lists) => show_lists.name(),
            ShowPrepared(show) => show.name(),
            ShowReplication(show_replication) => show_replication.name(),
            ShowResharding(cmd) => cmd.name(),
            ShowServerMemory(show_server_memory) => show_server_memory.name(),
            ShowClientMemory(show_client_memory) => show_client_memory.name(),
            Set(set) => set.name(),
            Ban(ban) => ban.name(),
            Probe(probe) => probe.name(),
            MaintenanceMode(maintenance_mode) => maintenance_mode.name(),
            Healthcheck(healthcheck) => healthcheck.name(),
            Reshard(reshard) => reshard.name(),
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
            "ban" | "unban" => ParseResult::Ban(Ban::parse(&sql)?),
            "healthcheck" => ParseResult::Healthcheck(Healthcheck::parse(&sql)?),
            "show" => match iter.next().ok_or(Error::Syntax)?.trim() {
                "clients" => ParseResult::ShowClients(ShowClients::parse(&sql)?),
                "pools" => ParseResult::ShowPools(ShowPools::parse(&sql)?),
                "config" => ParseResult::ShowConfig(ShowConfig::parse(&sql)?),
                "servers" => ParseResult::ShowServers(ShowServers::parse(&sql)?),
                "server" => match iter.next().ok_or(Error::Syntax)?.trim() {
                    "memory" => ParseResult::ShowServerMemory(ShowServerMemory::parse(&sql)?),
                    command => {
                        debug!("unknown admin show server command: '{}'", command);
                        return Err(Error::Syntax);
                    }
                },
                "client" => match iter.next().ok_or(Error::Syntax)?.trim() {
                    "memory" => ParseResult::ShowClientMemory(ShowClientMemory::parse(&sql)?),
                    command => {
                        debug!("unknown admin show client command: '{}'", command);
                        return Err(Error::Syntax);
                    }
                },
                "peers" => ParseResult::ShowPeers(ShowPeers::parse(&sql)?),
                "query_cache" => ParseResult::ShowQueryCache(ShowQueryCache::parse(&sql)?),
                "stats" => ParseResult::ShowStats(ShowStats::parse(&sql)?),
                "transactions" => ParseResult::ShowTransactions(ShowTransactions::parse(&sql)?),
                "mirrors" => ParseResult::ShowMirrors(ShowMirrors::parse(&sql)?),
                "version" => ParseResult::ShowVersion(ShowVersion::parse(&sql)?),
                "instance_id" => ParseResult::ShowInstanceId(ShowInstanceId::parse(&sql)?),
                "lists" => ParseResult::ShowLists(ShowLists::parse(&sql)?),
                "prepared" => ParseResult::ShowPrepared(ShowPreparedStatements::parse(&sql)?),
                "replication" => ParseResult::ShowReplication(ShowReplication::parse(&sql)?),
                "resharding" => ParseResult::ShowResharding(ShowResharding::parse(&sql)?),
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
            "reshard" => ParseResult::Reshard(Reshard::parse(&sql)?),
            "probe" => ParseResult::Probe(Probe::parse(&sql)?),
            "maintenance" => ParseResult::MaintenanceMode(MaintenanceMode::parse(&sql)?),
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

#[cfg(test)]
mod tests {
    use super::{Error, ParseResult, Parser};

    #[test]
    fn parses_show_clients_command() {
        let result = Parser::parse("SHOW CLIENTS;");
        assert!(matches!(result, Ok(ParseResult::ShowClients(_))));
    }

    #[test]
    fn parses_reset_query_cache_command() {
        let result = Parser::parse("RESET QUERY_CACHE");
        assert!(matches!(result, Ok(ParseResult::ResetQueryCache(_))));
    }

    #[test]
    fn rejects_unknown_admin_command() {
        let result = Parser::parse("FOO BAR");
        assert!(matches!(result, Err(Error::Syntax)));
    }

    #[test]
    fn parses_show_server_memory_command() {
        let result = Parser::parse("SHOW SERVER MEMORY;");
        assert!(matches!(result, Ok(ParseResult::ShowServerMemory(_))));
    }

    #[test]
    fn parses_show_client_memory_command() {
        let result = Parser::parse("SHOW CLIENT MEMORY;");
        assert!(matches!(result, Ok(ParseResult::ShowClientMemory(_))));
    }
}
