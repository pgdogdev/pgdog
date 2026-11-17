use std::time::{SystemTime, UNIX_EPOCH};

use pgdog_stats::ReshardingId;

use super::{ReshardingKey, ReshardingRecord, Stage, TaskOwner};
use crate::backend::server::ServerRequest;
use crate::net::bind::Parameter;

pub(crate) const CREATE_TABLE: &str = include_str!("resharding.sql");

pub(crate) trait Query {
    const SQL: &'static str;

    type Params<'a>;

    fn params(params: Self::Params<'_>) -> Vec<Parameter>;

    fn request(params: Self::Params<'_>) -> ServerRequest {
        ServerRequest::parameterized(Self::SQL, &Self::params(params))
    }
}

pub(crate) struct SetupSession;

impl Query for SetupSession {
    // change tcp keepalive settings to react faster on the
    // client socket abruption
    const SQL: &'static str = "
        SELECT set_config('application_name', $1, false),
               set_config('tcp_keepalives_idle', '60', false),
               set_config('tcp_keepalives_interval', '15', false),
               set_config('tcp_keepalives_count', '4', false),
               set_config('tcp_user_timeout', '120000', false)";

    type Params<'a> = &'a str;

    fn params(application_name: &str) -> Vec<Parameter> {
        vec![text(application_name)]
    }
}

pub(crate) struct TableExists;

impl Query for TableExists {
    const SQL: &'static str = "SELECT (to_regclass('pgdog.resharding') IS NOT NULL)::int";

    type Params<'a> = ();

    fn params(_: ()) -> Vec<Parameter> {
        vec![]
    }
}

pub(crate) struct TryLock;

impl Query for TryLock {
    const SQL: &'static str =
        "SELECT pg_try_advisory_lock(hashtext('pgdog.resharding'), hashtext($1))::int";

    type Params<'a> = &'a ReshardingKey;

    fn params(key: &ReshardingKey) -> Vec<Parameter> {
        vec![lock_name(key)]
    }
}

pub(crate) struct Unlock;

impl Query for Unlock {
    const SQL: &'static str =
        "SELECT pg_advisory_unlock(hashtext('pgdog.resharding'), hashtext($1))";

    type Params<'a> = &'a ReshardingKey;

    fn params(key: &ReshardingKey) -> Vec<Parameter> {
        vec![lock_name(key)]
    }
}

pub(crate) struct LockHolder;

impl Query for LockHolder {
    const SQL: &'static str = "
        SELECT a.application_name
        FROM pg_locks l
        JOIN pg_stat_activity a ON a.pid = l.pid
        WHERE l.locktype = 'advisory'
          AND l.granted
          AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
          AND l.classid = (hashtext('pgdog.resharding')::bigint & 4294967295)::oid
          AND l.objid = (hashtext($1)::bigint & 4294967295)::oid
          AND l.objsubid = 2";

    type Params<'a> = &'a ReshardingKey;

    fn params(key: &ReshardingKey) -> Vec<Parameter> {
        vec![lock_name(key)]
    }
}

pub(crate) struct SelectRow;

impl Query for SelectRow {
    const SQL: &'static str = "
        SELECT source_db, destination_db, publication, command::text, slot_name,
               stage, config::text, instance_id, run_id::text,
               task_id::text, format_version::text, id::text,
               (extract(epoch FROM started_at) * 1000)::bigint::text,
               coalesce((extract(epoch FROM resumed_at) * 1000)::bigint::text, '')
        FROM pgdog.resharding
        WHERE source_db = $1 AND destination_db = $2 AND publication = $3";

    type Params<'a> = &'a ReshardingKey;

    fn params(key: &ReshardingKey) -> Vec<Parameter> {
        key_params(key).into()
    }
}

pub(crate) struct ListRows;

impl Query for ListRows {
    const SQL: &'static str = "
        SELECT source_db, destination_db, publication, command::text, slot_name,
               stage, config::text, instance_id, run_id::text,
               task_id::text, format_version::text, id::text,
               (extract(epoch FROM started_at) * 1000)::bigint::text,
               coalesce((extract(epoch FROM resumed_at) * 1000)::bigint::text, '')
        FROM pgdog.resharding
        ORDER BY source_db, destination_db, publication";

    type Params<'a> = ();

    fn params(_: ()) -> Vec<Parameter> {
        vec![]
    }
}

pub(crate) struct InsertRow;

impl Query for InsertRow {
    const SQL: &'static str = "
        INSERT INTO pgdog.resharding (
            source_db, destination_db, publication, command, slot_name,
            stage, config, instance_id, run_id, task_id, format_version, id,
            started_at
        )
        VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7::jsonb, $8, $9::uuid,
                $10::bigint, $11::integer, $12::uuid, to_timestamp($13::bigint / 1000.0))";

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
            text(&unix_millis(row.started_at).to_string()),
        ]
    }
}

pub(crate) struct UpdateStage;

pub(crate) struct StageUpdate {
    pub(crate) id: ReshardingId,
    pub(crate) stage: Stage,
}

impl Query for UpdateStage {
    const SQL: &'static str = "
        UPDATE pgdog.resharding
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

pub(crate) struct ResumeRow;

pub(crate) struct ResumeUpdate<'a> {
    pub(crate) id: ReshardingId,
    pub(crate) owner: &'a TaskOwner,
    pub(crate) resumed_at: SystemTime,
}

impl Query for ResumeRow {
    const SQL: &'static str = "
        UPDATE pgdog.resharding
        SET instance_id = $2, run_id = $3::uuid, task_id = $4::bigint,
            resumed_at = to_timestamp($5::bigint / 1000.0)
        WHERE id = $1::uuid";

    type Params<'a> = ResumeUpdate<'a>;

    fn params(update: ResumeUpdate<'_>) -> Vec<Parameter> {
        vec![
            text(&update.id.to_string()),
            text(update.owner.instance_id.as_str()),
            text(&update.owner.run_id.to_string()),
            text(&update.owner.task_id.to_string()),
            text(&unix_millis(update.resumed_at).to_string()),
        ]
    }
}

pub(crate) struct Heartbeat;

impl Query for Heartbeat {
    const SQL: &'static str = "SELECT 1";

    type Params<'a> = ();

    fn params(_: ()) -> Vec<Parameter> {
        vec![]
    }
}

pub(crate) struct DeleteRow;

impl Query for DeleteRow {
    const SQL: &'static str = "DELETE FROM pgdog.resharding WHERE id = $1::uuid";

    type Params<'a> = ReshardingId;

    fn params(id: ReshardingId) -> Vec<Parameter> {
        vec![text(&id.to_string())]
    }
}

#[cfg(test)]
pub(crate) struct DeleteKey;

#[cfg(test)]
impl Query for DeleteKey {
    const SQL: &'static str = "
        DELETE FROM pgdog.resharding
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

fn unix_millis(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as i64)
}
