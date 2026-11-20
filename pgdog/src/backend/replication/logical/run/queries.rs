use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};

use pgdog_stats::ReshardingId;

use super::{ReshardingKey, ReshardingRecord, Stage, TaskOwner};
use crate::backend::server::ServerRequest;
use crate::config::config;
use crate::net::bind::Parameter;
use crate::util::instance_id;

pub(super) const APPLICATION_NAME_PREFIX: &str = "pgdog_resharding:";

pub(super) const CREATE_TABLE: &str = include_str!("resharding.sql");

pub(super) trait Query {
    const SQL: &'static str;

    type Params<'a>;

    fn params(params: Self::Params<'_>) -> Vec<Parameter>;

    fn request(params: Self::Params<'_>) -> ServerRequest {
        ServerRequest::parameterized(Self::SQL, &Self::params(params))
    }
}

pub(super) struct SetupSession;

impl Query for SetupSession {
    const SQL: &'static str = "
        SELECT set_config('application_name', $1, false),
               set_config('lock_timeout', $2, false),
               set_config('DateStyle', 'ISO', false),
               set_config('tcp_keepalives_idle', $3, false),
               set_config('tcp_keepalives_interval', $4, false),
               set_config('tcp_keepalives_count', $5, false),
               set_config('tcp_user_timeout', $6, false)";

    type Params<'a> = ();

    fn params(_: ()) -> Vec<Parameter> {
        let config = config();
        let resharding = &config.config.resharding;
        let tcp = resharding.tcp();
        let secs = |value: Option<Duration>| value.map_or(0, |value| value.as_secs());
        vec![
            text(&format!("{APPLICATION_NAME_PREFIX}{}", instance_id())),
            text(&resharding.state_heartbeat_interval.to_string()),
            text(&secs(tcp.time()).to_string()),
            text(&secs(tcp.interval()).to_string()),
            text(&tcp.retries().unwrap_or(0).to_string()),
            text(
                &tcp.user_timeout()
                    .map_or(0, |value| value.as_millis())
                    .to_string(),
            ),
        ]
    }
}

pub(super) struct TableExists;

impl Query for TableExists {
    const SQL: &'static str = "SELECT (to_regclass('__pgdog.resharding') IS NOT NULL)::int";

    type Params<'a> = ();

    fn params(_: ()) -> Vec<Parameter> {
        vec![]
    }
}

pub(super) struct Lock;

impl Query for Lock {
    const SQL: &'static str =
        "SELECT pg_advisory_lock(hashtext('__pgdog.resharding'), hashtext($1))";

    type Params<'a> = &'a ReshardingKey;

    fn params(key: &ReshardingKey) -> Vec<Parameter> {
        vec![lock_name(key)]
    }
}

pub(super) struct Unlock;

impl Query for Unlock {
    const SQL: &'static str =
        "SELECT pg_advisory_unlock(hashtext('__pgdog.resharding'), hashtext($1))";

    type Params<'a> = &'a ReshardingKey;

    fn params(key: &ReshardingKey) -> Vec<Parameter> {
        vec![lock_name(key)]
    }
}

pub(super) struct LockHolder;

impl Query for LockHolder {
    const SQL: &'static str = "
        SELECT a.application_name
        FROM pg_locks l
        JOIN pg_stat_activity a ON a.pid = l.pid
        WHERE l.locktype = 'advisory'
          AND l.granted
          AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
          AND l.classid = (hashtext('__pgdog.resharding')::bigint & 4294967295)::oid
          AND l.objid = (hashtext($1)::bigint & 4294967295)::oid
          AND l.objsubid = 2";

    type Params<'a> = &'a ReshardingKey;

    fn params(key: &ReshardingKey) -> Vec<Parameter> {
        vec![lock_name(key)]
    }
}

/// For the same set of columns construct:
/// - `sql_select` - columns select list used in sql query
/// - `index_map` - IndexMap `column_name -> index`
macro_rules! columns {
    (sql_select { $first:ident $(, $name:ident)* $(,)? }) => {
        concat!(" ", stringify!($first), $(", ", stringify!($name),)* " ")
    };
    (index_map { $($name:ident),* $(,)? }) => {
        [$(stringify!($name)),*]
            .into_iter()
            .enumerate()
            .map(|(index, name)| (name, index))
            .collect::<indexmap::IndexMap<&'static str, usize>>()
    };
}

/// The result columns of a `__pgdog.resharding` record, in order.
macro_rules! record_columns {
    ($mode:ident) => {
        columns!($mode {
            id,
            source_db,
            destination_db,
            publication,
            command,
            slot_name,
            stage,
            config,
            instance_id,
            run_id,
            task_id,
            format_version,
            started_at,
            resumed_at,
        })
    };
}

pub(super) use {columns, record_columns};

pub(super) struct SelectRow;

impl Query for SelectRow {
    const SQL: &'static str = concat!(
        "SELECT",
        record_columns!(sql_select),
        "
        FROM __pgdog.resharding
        WHERE source_db = $1
          AND destination_db = $2
          AND publication = $3"
    );

    type Params<'a> = &'a ReshardingKey;

    fn params(key: &ReshardingKey) -> Vec<Parameter> {
        key_params(key).into()
    }
}

pub(super) struct ListRows;

impl Query for ListRows {
    const SQL: &'static str = concat!(
        "SELECT",
        record_columns!(sql_select),
        "
        FROM __pgdog.resharding
        ORDER BY source_db, destination_db, publication"
    );

    type Params<'a> = ();

    fn params(_: ()) -> Vec<Parameter> {
        vec![]
    }
}

pub(super) struct InsertRow;

impl Query for InsertRow {
    const SQL: &'static str = "
        INSERT INTO __pgdog.resharding (
            source_db, destination_db, publication, command, slot_name,
            stage, config, instance_id, run_id, task_id, format_version, id,
            started_at
        )
        VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7::jsonb, $8, $9::uuid,
                $10::bigint, $11::integer, $12::uuid, $13::timestamp)";

    type Params<'a> = &'a ReshardingRecord;

    fn params(row: &ReshardingRecord) -> Vec<Parameter> {
        let [source, destination, publication] = key_params(&row.key);
        vec![
            source,
            destination,
            publication,
            text(&serde_json::to_string(&row.command).expect("command is valid JSON")),
            text(&row.slot_name),
            text(&row.stage.to_string()),
            text(&serde_json::to_string(&row.config).expect("config snapshot is valid JSON")),
            text(row.owner.instance_id.as_str()),
            text(&row.owner.run_id.to_string()),
            text(&row.owner.task_id.to_string()),
            text(&row.format_version.to_string()),
            text(&row.id.to_string()),
            text(&utc(row.started_at)),
        ]
    }
}

pub(super) struct UpdateStage;

pub(super) struct StageUpdate {
    pub(super) id: ReshardingId,
    pub(super) stage: Stage,
}

impl Query for UpdateStage {
    const SQL: &'static str = "
        UPDATE __pgdog.resharding
        SET stage = $2
        WHERE id = $1::uuid";

    type Params<'a> = StageUpdate;

    fn params(update: StageUpdate) -> Vec<Parameter> {
        vec![
            text(&update.id.to_string()),
            text(&update.stage.to_string()),
        ]
    }
}

pub(super) struct ResumeRow;

pub(super) struct ResumeUpdate<'a> {
    pub(super) id: ReshardingId,
    pub(super) owner: &'a TaskOwner,
    pub(super) resumed_at: SystemTime,
}

impl Query for ResumeRow {
    const SQL: &'static str = "
        UPDATE __pgdog.resharding
        SET instance_id = $2, run_id = $3::uuid, task_id = $4::bigint,
            resumed_at = $5::timestamp
        WHERE id = $1::uuid";

    type Params<'a> = ResumeUpdate<'a>;

    fn params(update: ResumeUpdate<'_>) -> Vec<Parameter> {
        vec![
            text(&update.id.to_string()),
            text(update.owner.instance_id.as_str()),
            text(&update.owner.run_id.to_string()),
            text(&update.owner.task_id.to_string()),
            text(&utc(update.resumed_at)),
        ]
    }
}

pub(super) struct Heartbeat;

impl Query for Heartbeat {
    const SQL: &'static str = "SELECT 1";

    type Params<'a> = ();

    fn params(_: ()) -> Vec<Parameter> {
        vec![]
    }
}

pub(super) struct DeleteRow;

impl Query for DeleteRow {
    const SQL: &'static str = "DELETE FROM __pgdog.resharding WHERE id = $1::uuid";

    type Params<'a> = ReshardingId;

    fn params(id: ReshardingId) -> Vec<Parameter> {
        vec![text(&id.to_string())]
    }
}

#[cfg(test)]
pub(super) struct DeleteKey;

#[cfg(test)]
impl Query for DeleteKey {
    const SQL: &'static str = "
        DELETE FROM __pgdog.resharding
        WHERE source_db = $1 AND destination_db = $2 AND publication = $3";

    type Params<'a> = &'a ReshardingKey;

    fn params(key: &ReshardingKey) -> Vec<Parameter> {
        key_params(key).into()
    }
}

fn text(value: &str) -> Parameter {
    Parameter::new(value.as_bytes())
}

fn key_params(key: &ReshardingKey) -> [Parameter; 3] {
    [
        text(&key.databases.source),
        text(&key.databases.destination),
        text(&key.publication),
    ]
}

fn lock_name(key: &ReshardingKey) -> Parameter {
    text(&format!(
        "{}/{}/{}",
        key.databases.source, key.databases.destination, key.publication
    ))
}

/// UTC time as text for a `timestamp` column.
fn utc(time: SystemTime) -> String {
    DateTime::<Utc>::from(time)
        .naive_utc()
        .format("%Y-%m-%d %H:%M:%S%.6f")
        .to_string()
}
