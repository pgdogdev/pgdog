//! Admin command parser.

use crate::admin::show_guc::get_show_variable;

use super::*;

use tracing::debug;

/// Parser result.
pub enum ParseResult {
    Pause(Pause),
    Reconnect(Reconnect),
    ShowClients(ShowClients),
    Reload(Reload),
    ShowPools(ShowPools),
    ShowBans(ShowBans),
    ShowConfig(ShowConfig),
    ShowServers(ShowServers),
    ShowPeers(ShowPeers),
    ShowQueryCache(ShowQueryCache),
    ResetPrepared(ResetPrepared),
    ResetQueryCache(ResetQueryCache),
    ResetStats(ResetStats),
    ShowStats(ShowStats),
    ShowTransactions(ShowTransactions),
    ShowMirrors(ShowMirrors),
    ShowVersion(ShowVersion),
    ShowInstanceId(ShowInstanceId),
    SetupSchema(SetupSchema),
    Shutdown(Shutdown),
    ShowLists(ShowLists),
    ShowListeners(ShowListeners),
    ShowPrepared(ShowPreparedStatements),
    ShowReplication(ShowReplication),
    ShowServerMemory(ShowServerMemory),
    ShowClientMemory(ShowClientMemory),
    ShowTableCopies(ShowTableCopies),
    ShowReplicationSlots(ShowReplicationSlots),
    ShowSchemaSync(ShowSchemaSync),
    Set(Set),
    Ban(Ban),
    Probe(Probe),
    MaintenanceMode(MaintenanceMode),
    Healthcheck(Healthcheck),
    Reshard(Reshard),
    SchemaSync(SchemaSync),
    CopyData(CopyData),
    Replicate(Replicate),
    ShowTasks(ShowTasks),
    StopTask(StopTask),
    Cutover(Cutover),
    Guc(ShowGuc),
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
            ShowBans(show_bans) => show_bans.execute().await,
            ShowConfig(show_config) => show_config.execute().await,
            ShowServers(show_servers) => show_servers.execute().await,
            ShowPeers(show_peers) => show_peers.execute().await,
            ShowQueryCache(show_query_cache) => show_query_cache.execute().await,
            ResetPrepared(cmd) => cmd.execute().await,
            ResetQueryCache(reset_query_cache) => reset_query_cache.execute().await,
            ResetStats(cmd) => cmd.execute().await,
            ShowStats(show_stats) => show_stats.execute().await,
            ShowTransactions(show_transactions) => show_transactions.execute().await,
            ShowMirrors(show_mirrors) => show_mirrors.execute().await,
            ShowVersion(show_version) => show_version.execute().await,
            ShowInstanceId(show_instance_id) => show_instance_id.execute().await,
            SetupSchema(setup_schema) => setup_schema.execute().await,
            Shutdown(shutdown) => shutdown.execute().await,
            ShowLists(show_lists) => show_lists.execute().await,
            ShowListeners(show_listeners) => show_listeners.execute().await,
            ShowPrepared(cmd) => cmd.execute().await,
            ShowReplication(show_replication) => show_replication.execute().await,
            ShowServerMemory(show_server_memory) => show_server_memory.execute().await,
            ShowClientMemory(show_client_memory) => show_client_memory.execute().await,
            ShowTableCopies(show_table_copies) => show_table_copies.execute().await,
            ShowReplicationSlots(cmd) => cmd.execute().await,
            ShowSchemaSync(cmd) => cmd.execute().await,
            Set(set) => set.execute().await,
            Ban(ban) => ban.execute().await,
            Probe(probe) => probe.execute().await,
            MaintenanceMode(maintenance_mode) => maintenance_mode.execute().await,
            Healthcheck(healthcheck) => healthcheck.execute().await,
            Reshard(reshard) => reshard.execute().await,
            SchemaSync(cmd) => cmd.execute().await,
            CopyData(cmd) => cmd.execute().await,
            Replicate(cmd) => cmd.execute().await,
            ShowTasks(cmd) => cmd.execute().await,
            StopTask(cmd) => cmd.execute().await,
            Cutover(cmd) => cmd.execute().await,
            Guc(cmd) => cmd.execute().await,
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
            ShowBans(show_bans) => show_bans.name(),
            ShowConfig(show_config) => show_config.name(),
            ShowServers(show_servers) => show_servers.name(),
            ShowPeers(show_peers) => show_peers.name(),
            ShowQueryCache(show_query_cache) => show_query_cache.name(),
            ResetPrepared(cmd) => cmd.name(),
            ResetQueryCache(reset_query_cache) => reset_query_cache.name(),
            ResetStats(cmd) => cmd.name(),
            ShowStats(show_stats) => show_stats.name(),
            ShowTransactions(show_transactions) => show_transactions.name(),
            ShowMirrors(show_mirrors) => show_mirrors.name(),
            ShowVersion(show_version) => show_version.name(),
            ShowInstanceId(show_instance_id) => show_instance_id.name(),
            SetupSchema(setup_schema) => setup_schema.name(),
            Shutdown(shutdown) => shutdown.name(),
            ShowLists(show_lists) => show_lists.name(),
            ShowListeners(show_listeners) => show_listeners.name(),
            ShowPrepared(show) => show.name(),
            ShowReplication(show_replication) => show_replication.name(),
            ShowServerMemory(show_server_memory) => show_server_memory.name(),
            ShowClientMemory(show_client_memory) => show_client_memory.name(),
            ShowTableCopies(show_table_copies) => show_table_copies.name(),
            ShowReplicationSlots(cmd) => cmd.name(),
            ShowSchemaSync(cmd) => cmd.name(),
            Set(set) => set.name(),
            Ban(ban) => ban.name(),
            Probe(probe) => probe.name(),
            MaintenanceMode(maintenance_mode) => maintenance_mode.name(),
            Healthcheck(healthcheck) => healthcheck.name(),
            Reshard(reshard) => reshard.name(),
            SchemaSync(cmd) => cmd.name(),
            CopyData(cmd) => cmd.name(),
            Replicate(cmd) => cmd.name(),
            ShowTasks(cmd) => cmd.name(),
            StopTask(cmd) => cmd.name(),
            Cutover(cmd) => cmd.name(),
            Guc(cmd) => cmd.name(),
        }
    }
}

/// Admin command parser.
pub struct Parser;

impl Parser {
    /// Parse the query and return a command we can execute.
    pub fn parse(sql: &str) -> Result<ParseResult, Error> {
        // Handle SET separately because
        // we're about to clobber valid SQL syntax below.
        if is_set_statement(sql) {
            return Ok(ParseResult::Set(Set::parse(sql)?));
        }

        if let Ok(show) = get_show_variable(sql) {
            return Ok(match show.as_str() {
                "clients" => ParseResult::ShowClients(ShowClients::parse(sql)?),
                "pools" => ParseResult::ShowPools(ShowPools::parse(sql)?),
                "bans" => ParseResult::ShowBans(ShowBans::parse(sql)?),
                "config" => ParseResult::ShowConfig(ShowConfig::parse(sql)?),
                "servers" => ParseResult::ShowServers(ShowServers::parse(sql)?),
                "peers" => ParseResult::ShowPeers(ShowPeers::parse(sql)?),
                "query_cache" => ParseResult::ShowQueryCache(ShowQueryCache::parse(sql)?),
                "stats" => ParseResult::ShowStats(ShowStats::parse(sql)?),
                "transactions" => ParseResult::ShowTransactions(ShowTransactions::parse(sql)?),
                "mirrors" => ParseResult::ShowMirrors(ShowMirrors::parse(sql)?),
                "version" => ParseResult::ShowVersion(ShowVersion::parse(sql)?),
                "instance_id" => ParseResult::ShowInstanceId(ShowInstanceId::parse(sql)?),
                "lists" => ParseResult::ShowLists(ShowLists::parse(sql)?),
                "listeners" => ParseResult::ShowListeners(ShowListeners::parse(sql)?),
                "prepared" => ParseResult::ShowPrepared(ShowPreparedStatements::parse(sql)?),
                "replication" => ParseResult::ShowReplication(ShowReplication::parse(sql)?),
                "replication_slots" => {
                    ParseResult::ShowReplicationSlots(ShowReplicationSlots::parse(sql)?)
                }
                "schema_sync" => ParseResult::ShowSchemaSync(ShowSchemaSync::parse(sql)?),
                "table_copies" => ParseResult::ShowTableCopies(ShowTableCopies::parse(sql)?),
                "tasks" => ParseResult::ShowTasks(ShowTasks::parse(sql)?),
                variable => ParseResult::Guc(ShowGuc {
                    variable: variable.to_string(),
                }),
            });
        }

        let sql = sql.trim().replace(";", "").to_lowercase();
        let mut iter = sql.split(" ");

        Ok(match iter.next().ok_or(Error::Syntax)?.trim() {
            "pause" | "resume" => ParseResult::Pause(Pause::parse(&sql)?),
            "shutdown" => ParseResult::Shutdown(Shutdown::parse(&sql)?),
            "reconnect" => ParseResult::Reconnect(Reconnect::parse(&sql)?),
            "reload" => ParseResult::Reload(Reload::parse(&sql)?),
            "ban" | "unban" => ParseResult::Ban(Ban::parse(&sql)?),
            "healthcheck" => ParseResult::Healthcheck(Healthcheck::parse(&sql)?),
            // These are not covered by the show handler above
            // because they are not valid SQL syntax.
            "show" => match iter.next().ok_or(Error::Syntax)?.trim() {
                // These two are duplicated because they support selecting columns from their output.
                "clients" => ParseResult::ShowClients(ShowClients::parse(&sql)?),
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

                command => {
                    debug!("unknown admin show command: '{}'", command);
                    return Err(Error::Syntax);
                }
            },
            "reset" => match iter.next().ok_or(Error::Syntax)?.trim() {
                "prepared" => ParseResult::ResetPrepared(ResetPrepared::parse(&sql)?),
                "query_cache" => ParseResult::ResetQueryCache(ResetQueryCache::parse(&sql)?),
                "stats" => ParseResult::ResetStats(ResetStats::parse(&sql)?),
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
            "schema_sync" => ParseResult::SchemaSync(SchemaSync::parse(&sql)?),
            "copy_data" => ParseResult::CopyData(CopyData::parse(&sql)?),
            "replicate" => ParseResult::Replicate(Replicate::parse(&sql)?),
            "stop_task" => ParseResult::StopTask(StopTask::parse(&sql)?),
            "cutover" => ParseResult::Cutover(Cutover::parse(&sql)?),
            "probe" => ParseResult::Probe(Probe::parse(&sql)?),
            "maintenance" => ParseResult::MaintenanceMode(MaintenanceMode::parse(&sql)?),
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

    macro_rules! assert_parses {
        ($sql:literal, $variant:pat) => {
            assert!(
                matches!(Parser::parse($sql), Ok($variant)),
                "failed to parse `{}`",
                $sql
            );
        };
    }

    #[test]
    fn parses_pool_control_commands() {
        assert_parses!("PAUSE", ParseResult::Pause(_));
        assert_parses!("RESUME", ParseResult::Pause(_));
        assert_parses!("RECONNECT", ParseResult::Reconnect(_));
        assert_parses!("RELOAD", ParseResult::Reload(_));
        assert_parses!("SHUTDOWN", ParseResult::Shutdown(_));
        assert_parses!("BAN", ParseResult::Ban(_));
        assert_parses!("UNBAN", ParseResult::Ban(_));
        assert_parses!("HEALTHCHECK", ParseResult::Healthcheck(_));
        assert_parses!(
            "PROBE postgres://postgres@localhost/postgres",
            ParseResult::Probe(_)
        );
        assert_parses!("MAINTENANCE ON", ParseResult::MaintenanceMode(_));
        assert_parses!("SET query_timeout TO '1000'", ParseResult::Set(_));
    }

    #[test]
    fn parses_show_commands() {
        assert_parses!("SHOW CLIENTS", ParseResult::ShowClients(_));
        assert_parses!("SHOW POOLS", ParseResult::ShowPools(_));
        assert_parses!("SHOW BANS", ParseResult::ShowBans(_));
        assert_parses!("SHOW CONFIG", ParseResult::ShowConfig(_));
        assert_parses!("SHOW SERVERS", ParseResult::ShowServers(_));
        assert_parses!("SHOW PEERS", ParseResult::ShowPeers(_));
        assert_parses!("SHOW QUERY_CACHE", ParseResult::ShowQueryCache(_));
        assert_parses!("SHOW STATS", ParseResult::ShowStats(_));
        assert_parses!("SHOW TRANSACTIONS", ParseResult::ShowTransactions(_));
        assert_parses!("SHOW MIRRORS", ParseResult::ShowMirrors(_));
        assert_parses!("SHOW VERSION", ParseResult::ShowVersion(_));
        assert_parses!("SHOW INSTANCE_ID", ParseResult::ShowInstanceId(_));
        assert_parses!("SHOW LISTS", ParseResult::ShowLists(_));
        assert_parses!("SHOW LISTENERS", ParseResult::ShowListeners(_));
        assert_parses!("SHOW PREPARED", ParseResult::ShowPrepared(_));
        assert_parses!("SHOW REPLICATION", ParseResult::ShowReplication(_));
        assert_parses!(
            "SHOW REPLICATION_SLOTS",
            ParseResult::ShowReplicationSlots(_)
        );
        assert_parses!("SHOW SCHEMA_SYNC", ParseResult::ShowSchemaSync(_));
        assert_parses!("SHOW TABLE_COPIES", ParseResult::ShowTableCopies(_));
        assert_parses!("SHOW TASKS", ParseResult::ShowTasks(_));
        assert_parses!("SHOW SERVER MEMORY", ParseResult::ShowServerMemory(_));
        assert_parses!("SHOW CLIENT MEMORY", ParseResult::ShowClientMemory(_));
        assert_parses!("SHOW server_version", ParseResult::Guc(_));
    }

    #[test]
    fn parses_show_commands_with_selected_columns() {
        assert_parses!(
            "SHOW CLIENTS prepared_statements, application_name",
            ParseResult::ShowClients(_)
        );
        assert_parses!(
            "SHOW SERVERS remote_pid, application_name",
            ParseResult::ShowServers(_)
        );
    }

    #[test]
    fn parses_schema_and_replication_commands() {
        assert_parses!("SETUP SCHEMA", ParseResult::SetupSchema(_));
        assert_parses!("RESHARD source target publication", ParseResult::Reshard(_));
        assert_parses!(
            "SCHEMA_SYNC pre source target publication",
            ParseResult::SchemaSync(_)
        );
        assert_parses!(
            "COPY_DATA source target publication",
            ParseResult::CopyData(_)
        );
        assert_parses!(
            "REPLICATE source target publication",
            ParseResult::Replicate(_)
        );
        assert_parses!("STOP_TASK 1", ParseResult::StopTask(_));
        assert_parses!("CUTOVER", ParseResult::Cutover(_));
    }

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
    fn parses_reset_prepared_command() {
        let result = Parser::parse("RESET PREPARED");
        assert!(matches!(result, Ok(ParseResult::ResetPrepared(_))));
    }

    #[test]
    fn parses_reset_stats_command() {
        let result = Parser::parse("RESET STATS");
        assert!(matches!(result, Ok(ParseResult::ResetStats(_))));
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

    #[test]
    fn parses_show_listeners_command() {
        let result = Parser::parse("SHOW LISTENERS;");
        assert!(matches!(result, Ok(ParseResult::ShowListeners(_))));
    }

    #[test]
    fn parses_show_bans_command() {
        let result = Parser::parse("SHOW BANS;");
        assert!(matches!(result, Ok(ParseResult::ShowBans(_))));
    }

    #[test]
    fn parses_cutover_command() {
        assert!(matches!(
            Parser::parse("CUTOVER"),
            Ok(ParseResult::Cutover(_))
        ));
        assert!(matches!(
            Parser::parse("CUTOVER 1"),
            Ok(ParseResult::Cutover(_))
        ));
    }
}
