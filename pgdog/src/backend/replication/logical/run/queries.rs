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

pub(crate) struct SetApplicationName;

impl Query for SetApplicationName {
    const SQL: &'static str = "SELECT set_config('application_name', $1, false)";

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
               slot_existed::text, stage, config::text, instance_id, run_id::text,
               task_id::text, format_version::text, id::text
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
               slot_existed::text, stage, config::text, instance_id, run_id::text,
               task_id::text, format_version::text, id::text
        FROM pgdog.resharding
        ORDER BY source_db, destination_db, publication";

    type Params<'a> = ();

    fn params(_: ()) -> Vec<Parameter> {
        vec![]
    }
}

pub(crate) struct CountSlots;

impl Query for CountSlots {
    const SQL: &'static str = "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1";

    type Params<'a> = &'a str;

    fn params(slot_name: &str) -> Vec<Parameter> {
        vec![text(slot_name)]
    }
}

pub(crate) struct CountOrigins;

impl Query for CountOrigins {
    const SQL: &'static str =
        "SELECT count(*) FROM pg_replication_origin WHERE roname = ANY(string_to_array($1, ','))";

    type Params<'a> = &'a [String];

    fn params(names: &[String]) -> Vec<Parameter> {
        vec![text(&names.join(","))]
    }
}

pub(crate) struct InsertRow;

impl Query for InsertRow {
    const SQL: &'static str = "
        INSERT INTO pgdog.resharding (
            source_db, destination_db, publication, command, slot_name,
            slot_existed, stage, config, instance_id, run_id, task_id, format_version, id
        )
        VALUES ($1, $2, $3, $4::jsonb, $5, $6::boolean, $7, $8::jsonb, $9, $10::uuid,
                $11::bigint, $12::integer, $13::uuid)";

    type Params<'a> = &'a ReshardingRecord;

    fn params(row: &ReshardingRecord) -> Vec<Parameter> {
        let [source, destination, publication] = key_params(&row.key);
        vec![
            source,
            destination,
            publication,
            text(&serde_json::to_string(&row.command).expect("command is valid JSON")),
            text(&row.slot_name),
            text(&row.slot_existed.to_string()),
            text(&row.stage.to_string()),
            text(&serde_json::to_string(&row.config).expect("config snapshot is valid JSON")),
            text(row.owner.instance_id.as_str()),
            text(&row.owner.run_id.to_string()),
            text(&row.owner.task_id.to_string()),
            text(&row.format_version.to_string()),
            text(&row.id.to_string()),
        ]
    }
}

pub(crate) struct UpdateStage;

pub(crate) struct StageUpdate<'a> {
    pub(crate) key: &'a ReshardingKey,
    pub(crate) stage: Stage,
}

impl Query for UpdateStage {
    const SQL: &'static str = "
        UPDATE pgdog.resharding
        SET stage = $4
        WHERE source_db = $1 AND destination_db = $2 AND publication = $3";

    type Params<'a> = StageUpdate<'a>;

    fn params(update: StageUpdate<'_>) -> Vec<Parameter> {
        let [source, destination, publication] = key_params(update.key);
        vec![
            source,
            destination,
            publication,
            text(&update.stage.to_string()),
        ]
    }
}

pub(crate) struct UpdateOwner;

pub(crate) struct OwnerUpdate<'a> {
    pub(crate) key: &'a ReshardingKey,
    pub(crate) owner: &'a TaskOwner,
}

impl Query for UpdateOwner {
    const SQL: &'static str = "
        UPDATE pgdog.resharding
        SET instance_id = $4, run_id = $5::uuid, task_id = $6::bigint
        WHERE source_db = $1 AND destination_db = $2 AND publication = $3";

    type Params<'a> = OwnerUpdate<'a>;

    fn params(update: OwnerUpdate<'_>) -> Vec<Parameter> {
        let [source, destination, publication] = key_params(update.key);
        vec![
            source,
            destination,
            publication,
            text(update.owner.instance_id.as_str()),
            text(&update.owner.run_id.to_string()),
            text(&update.owner.task_id.to_string()),
        ]
    }
}

pub(crate) struct DeleteRow;

impl Query for DeleteRow {
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
