use indexmap::IndexMap;
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::{Duration, SystemTime};

use pgdog_postgres_types::Timestamp;

use pgdog_stats::{Databases, ReshardingId, TaskId};
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;
use tokio_util::task::AbortOnDropHandle;
use tracing::warn;
use uuid::Uuid;

use super::super::{Error, subscriber::connect_address};
use super::queries::{
    APPLICATION_NAME_PREFIX, CREATE_TABLE, DeleteRow, Heartbeat, InsertRow, ListRows, Lock,
    LockHolder, Query, ResumeRow, ResumeUpdate, SelectRow, SetupSession, StageUpdate, TableExists,
    Unlock, UpdateStage, columns, record_columns,
};
use super::{FORMAT_VERSION, ReshardingKey, ReshardingRecord, Stage, TaskOwner};
use crate::api::tasks_storage;
use crate::backend::server::ServerRequest;
use crate::backend::{Error as BackendError, Server, pool::Address};
use crate::config::config;
use crate::net::messages::data_types::FromDataType;
use crate::net::{DataRow, Format};
use crate::tasks;
use crate::util::retry::{Retry, RetryConfig};

const LOCK_NOT_AVAILABLE: &str = "55P03";

#[derive(Debug)]
pub(crate) struct ReshardingStore {
    server: Mutex<Option<Server>>,
    addr: Address,
    key: ReshardingKey,
    heartbeat: OnceLock<AbortOnDropHandle<()>>,
}

impl ReshardingStore {
    pub(in super::super) async fn open(addr: &Address, key: ReshardingKey) -> Result<Self, Error> {
        let server = locked_session(addr, &key).await?;

        Ok(Self {
            server: Mutex::new(Some(server)),
            addr: addr.clone(),
            key,
            heartbeat: OnceLock::new(),
        })
    }

    /// Run a query on the state connection every `state_heartbeat_interval`, so a
    /// lost lock is taken back even when the run writes nothing. Stop `owner`
    /// only when another session holds the lock. Ends when the store drops.
    pub(in super::super) fn start_heartbeat(self: &Arc<Self>, owner: TaskId) {
        let store = Arc::downgrade(self);
        let task = tasks::spawn("resharding lock heartbeat", async move {
            loop {
                let interval = lock_timeout();
                tokio::time::sleep(interval).await;
                let Some(store) = store.upgrade() else {
                    return;
                };
                match store.heartbeat().await {
                    Ok(()) => (),
                    Err(err @ Error::ReshardingLocked { .. }) => {
                        warn!(
                            "resharding {}: lock lost ({err}), stopping task {owner}",
                            store.key
                        );
                        tasks_storage().cancel_task(owner);
                        return;
                    }
                    Err(err) => warn!(
                        "resharding {}: state connection is down ({err}), retrying",
                        store.key
                    ),
                }
            }
        });
        let _ = self.heartbeat.set(AbortOnDropHandle::new(task));
    }

    /// Drop the state connection and take the lock again on a new one.
    #[cfg(test)]
    async fn reconnect(&self) -> Result<(), Error> {
        *self.server.lock().await = None;
        self.heartbeat().await
    }

    pub(in super::super) fn key(&self) -> &ReshardingKey {
        &self.key
    }

    pub(in super::super) fn address(&self) -> &Address {
        &self.addr
    }

    pub(in super::super) async fn load(&self) -> Result<Option<ReshardingRecord>, Error> {
        self.execute(SelectRow::request(&self.key))
            .await?
            .first()
            .map(|row| parse_record(row, self.key.clone()))
            .transpose()
    }

    pub(in super::super) async fn insert(&self, row: &ReshardingRecord) -> Result<(), Error> {
        self.execute(InsertRow::request(row)).await?;
        Ok(())
    }

    pub(in super::super) async fn set_stage(
        &self,
        id: ReshardingId,
        stage: Stage,
    ) -> Result<(), Error> {
        self.execute(UpdateStage::request(StageUpdate { id, stage }))
            .await?;
        Ok(())
    }

    pub(in super::super) async fn resume(
        &self,
        id: ReshardingId,
        owner: &TaskOwner,
        resumed_at: SystemTime,
    ) -> Result<(), Error> {
        self.execute(ResumeRow::request(ResumeUpdate {
            id,
            owner,
            resumed_at,
        }))
        .await?;
        Ok(())
    }

    pub(in super::super) async fn delete(&self, id: ReshardingId) -> Result<(), Error> {
        self.execute(DeleteRow::request(id)).await?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) async fn clear(&self) -> Result<(), Error> {
        self.execute(super::queries::DeleteKey::request(&self.key))
            .await?;
        Ok(())
    }

    async fn heartbeat(&self) -> Result<(), Error> {
        self.execute(Heartbeat::request(())).await?;
        Ok(())
    }

    /// Never reconnects: a lost connection has already released the lock.
    pub(in super::super) async fn unlock(&self) -> Result<(), Error> {
        self.server
            .lock()
            .await
            .as_mut()
            .ok_or_else(|| Error::Backend(BackendError::NotConnected))?
            .execute_checked(Unlock::request(&self.key))
            .await?;
        Ok(())
    }

    /// Run `request` on the state connection. When the connection is lost,
    /// open a new one, take the lock again and run `request` again, until
    /// `[resharding] retry_max_attempts` runs out. A SQL error on a
    /// usable connection and a lock held by another session are not retried.
    async fn execute(&self, request: impl Into<ServerRequest>) -> Result<Vec<DataRow>, Error> {
        let request = request.into();
        let mut retry = state_retry(&self.key);
        loop {
            let mut server = self.server.lock().await;
            let err: Error = match server.as_mut() {
                Some(session) => match session.fetch_all(request.clone()).await {
                    Ok(rows) => return Ok(rows),
                    Err(err) if session.in_sync() => return Err(err.into()),
                    Err(err) => {
                        *server = None;
                        err.into()
                    }
                },
                None => match locked_session(&self.addr, &self.key).await {
                    Ok(session) => {
                        *server = Some(session);
                        continue;
                    }
                    Err(err @ Error::ReshardingLocked { .. }) => return Err(err),
                    Err(err) => err,
                },
            };
            drop(server);
            if !retry.delay_retry(&err).await {
                return Err(err);
            }
        }
    }

    pub(in super::super) async fn connect(addr: &Address) -> Result<Server, Error> {
        let mut server = connect_address(addr).await?;
        server.execute_checked(SetupSession::request(())).await?;
        Ok(server)
    }

    pub(in super::super) async fn read(
        server: &mut Server,
        key: &ReshardingKey,
    ) -> Result<Option<ReshardingRecord>, Error> {
        let exists = server.fetch_all::<i32>(TableExists::request(())).await?;
        if exists.first() != Some(&1) {
            return Ok(None);
        }

        let rows = server.fetch_all::<DataRow>(SelectRow::request(key)).await?;
        rows.first()
            .map(|row| parse_record(row, key.clone()))
            .transpose()
    }

    pub(super) async fn list(
        server: &mut Server,
    ) -> Result<Vec<Result<ReshardingRecord, UnreadableRecord>>, Error> {
        let exists = server.fetch_all::<i32>(TableExists::request(())).await?;
        if exists.first() != Some(&1) {
            return Ok(vec![]);
        }

        server
            .fetch_all::<DataRow>(ListRows::request(()))
            .await?
            .iter()
            .map(|row| {
                let key = parse_key(row)?;
                Ok(parse_record(row, key.clone()).map_err(|error| UnreadableRecord { key, error }))
            })
            .collect()
    }

    pub(in super::super) async fn lock_owner(
        server: &mut Server,
        key: &ReshardingKey,
    ) -> Result<Option<String>, Error> {
        let owner = server
            .fetch_all::<String>(LockHolder::request(key))
            .await?
            .into_iter()
            .next()
            .map(|name| match name.strip_prefix(APPLICATION_NAME_PREFIX) {
                Some(instance) => instance.to_owned(),
                None if name.is_empty() => "unknown".into(),
                None => name,
            });
        Ok(owner)
    }
}

fn lock_timeout() -> Duration {
    Duration::from_millis(config().config.resharding.state_heartbeat_interval)
}

fn state_retry(key: &ReshardingKey) -> Retry {
    let config = config();
    let resharding = &config.config.resharding;
    Retry::new(
        RetryConfig::builder()
            .name(format!("resharding {key} state"))
            .max_attempts(resharding.retry_max_attempts)
            .delay(Duration::from_millis(resharding.retry_delay))
            .build(),
    )
}

async fn locked_session(addr: &Address, key: &ReshardingKey) -> Result<Server, Error> {
    let mut server = ReshardingStore::connect(addr).await?;
    server.execute_checked(CREATE_TABLE).await?;
    match server.execute_checked(Lock::request(key)).await {
        Ok(_) => Ok(server),
        Err(BackendError::ExecutionError(err)) if err.code == LOCK_NOT_AVAILABLE => {
            Err(locked_error(&mut server, key).await)
        }
        Err(err) => Err(err.into()),
    }
}

async fn locked_error(server: &mut Server, key: &ReshardingKey) -> Error {
    match ReshardingStore::lock_owner(server, key).await {
        Ok(owner) => Error::ReshardingLocked {
            key: key.to_string(),
            owner: owner.unwrap_or_else(|| "unknown".into()),
        },
        Err(err) => err,
    }
}

#[derive(Debug)]
pub(super) struct UnreadableRecord {
    pub(super) key: ReshardingKey,
    pub(super) error: Error,
}

static RECORD_COLUMNS: LazyLock<IndexMap<&'static str, usize>> =
    LazyLock::new(|| record_columns!(index_map));

fn column<T: FromDataType>(row: &DataRow, name: &str) -> Result<T, Error> {
    RECORD_COLUMNS
        .get(name)
        .and_then(|&index| row.get(index, Format::Text))
        .ok_or_else(|| Error::ReshardingFormat(format!("{name}: missing or invalid")))
}

fn nullable<T: FromDataType>(row: &DataRow, name: &str) -> Result<Option<T>, Error> {
    let null = RECORD_COLUMNS
        .get(name)
        .and_then(|&index| row.get_raw(index))
        .is_some_and(|data| data.is_null);
    if null {
        Ok(None)
    } else {
        column(row, name).map(Some)
    }
}

fn json<T: DeserializeOwned>(row: &DataRow, name: &str) -> Result<T, Error> {
    serde_json::from_str(&column::<String>(row, name)?)
        .map_err(|err| Error::ReshardingFormat(format!("{name}: {err}")))
}

fn parse_key(row: &DataRow) -> Result<ReshardingKey, Error> {
    Ok(ReshardingKey {
        databases: Databases {
            source: column(row, "source_db")?,
            destination: column(row, "destination_db")?,
        },
        publication: column(row, "publication")?,
    })
}

fn parse_record(row: &DataRow, key: ReshardingKey) -> Result<ReshardingRecord, Error> {
    let format_version: i32 = column(row, "format_version")?;
    if format_version > FORMAT_VERSION {
        return Err(Error::ReshardingFormat(format!(
            "format version {format_version} is newer than {FORMAT_VERSION}"
        )));
    }

    Ok(ReshardingRecord {
        id: column::<Uuid>(row, "id")?.into(),
        key,
        command: json(row, "command")?,
        slot_name: column(row, "slot_name")?,
        stage: column::<String>(row, "stage")?.parse()?,
        config: json(row, "config")?,
        owner: TaskOwner {
            instance_id: column::<String>(row, "instance_id")?.into(),
            run_id: column::<Uuid>(row, "run_id")?.into(),
            task_id: TaskId::new(column(row, "task_id")?),
        },
        format_version,
        started_at: utc(column(row, "started_at")?),
        resumed_at: nullable(row, "resumed_at")?.map(utc),
    })
}

/// A `timestamp` column holds UTC time.
fn utc(timestamp: Timestamp) -> SystemTime {
    timestamp.to_naive_datetime().and_utc().into()
}

#[cfg(test)]
pub(super) mod test {
    use super::super::snapshot::ClusterSnapshot;
    use super::super::{ConfigSnapshot, ReshardingCommand};
    use super::*;
    use crate::backend::server::test::test_server;
    use crate::util::instance_id;
    use std::time::UNIX_EPOCH;

    async fn backend_pid(store: &ReshardingStore) -> i32 {
        store
            .server
            .lock()
            .await
            .as_mut()
            .unwrap()
            .fetch_all::<i32>("SELECT pg_backend_pid()")
            .await
            .unwrap()[0]
    }

    pub(in super::super) fn key(name: &str) -> ReshardingKey {
        ReshardingKey {
            databases: Databases {
                source: format!("{name}_src"),
                destination: format!("{name}_dst"),
            },
            publication: "pub".into(),
        }
    }

    async fn listed_for(key: &ReshardingKey) -> Vec<Result<ReshardingRecord, UnreadableRecord>> {
        let mut server = test_server().await;
        ReshardingStore::list(&mut server)
            .await
            .unwrap()
            .into_iter()
            .filter(|row| match row {
                Ok(record) => record.key.databases == key.databases,
                Err(unreadable) => unreadable.key.databases == key.databases,
            })
            .collect()
    }

    fn now_millis() -> SystemTime {
        UNIX_EPOCH
            + Duration::from_millis(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            )
    }

    pub(in super::super) fn row(key: ReshardingKey) -> ReshardingRecord {
        let config =
            ConfigSnapshot::new_test(ClusterSnapshot::new_test(1), ClusterSnapshot::new_test(2));
        ReshardingRecord {
            id: ReshardingId::generate(),
            key,
            command: ReshardingCommand::Reshard {
                skip_schema_sync: false,
                replicate_only: false,
                sync_only: false,
                auto_cutover: true,
                tables_only: false,
            },
            slot_name: "slot".into(),
            stage: Stage::CopyTables,
            config,
            owner: TaskOwner::current(pgdog_stats::TaskId::new(7)),
            format_version: FORMAT_VERSION,
            started_at: now_millis() - Duration::from_secs(3600),
            resumed_at: None,
        }
    }

    async fn open_after_drop(addr: &Address, key: ReshardingKey) -> ReshardingStore {
        for _ in 0..50 {
            match ReshardingStore::open(addr, key.clone()).await {
                Ok(store) => return store,
                Err(Error::ReshardingLocked { .. }) => {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await
                }
                Err(err) => panic!("{err}"),
            }
        }
        panic!("lock of {key} was not released");
    }

    #[tokio::test]
    async fn publications_have_separate_records() {
        let addr = Address::new_test();
        let first_key = key("store_pubs");
        let second_key = ReshardingKey {
            publication: "other_pub".into(),
            ..first_key.clone()
        };
        let first = ReshardingStore::open(&addr, first_key.clone())
            .await
            .unwrap();
        let second = ReshardingStore::open(&addr, second_key.clone())
            .await
            .unwrap();
        for store in [&first, &second] {
            store.clear().await.unwrap();
        }
        let first_row = row(first_key.clone());
        let second_row = row(second_key.clone());
        first.insert(&first_row).await.unwrap();
        second.insert(&second_row).await.unwrap();

        let listed = listed_for(&first_key)
            .await
            .into_iter()
            .map(|row| row.unwrap().key)
            .collect::<Vec<_>>();
        assert_eq!(listed, [second_key, first_key]);

        first.delete(first_row.id).await.unwrap();
        assert!(second.load().await.unwrap().is_some());
        second.delete(second_row.id).await.unwrap();
    }

    #[tokio::test]
    async fn newer_rows_are_reported_alone() {
        let addr = Address::new_test();
        let old_key = key("store_newer");
        let new_key = ReshardingKey {
            publication: "newer_pub".into(),
            ..old_key.clone()
        };
        let old = ReshardingStore::open(&addr, old_key.clone()).await.unwrap();
        let new = ReshardingStore::open(&addr, new_key.clone()).await.unwrap();
        for store in [&old, &new] {
            store.clear().await.unwrap();
        }
        old.insert(&row(old_key.clone())).await.unwrap();
        new.insert(&ReshardingRecord {
            format_version: FORMAT_VERSION + 1,
            ..row(new_key.clone())
        })
        .await
        .unwrap();

        assert!(matches!(new.load().await, Err(Error::ReshardingFormat(_))));

        let listed = listed_for(&old_key).await;
        assert_eq!(listed.len(), 2);
        assert!(
            matches!(&listed[0], Err(UnreadableRecord { key, error: Error::ReshardingFormat(_) }) if *key == new_key)
        );
        assert_eq!(listed[1].as_ref().unwrap().key, old_key);

        old.clear().await.unwrap();
        new.clear().await.unwrap();
    }

    #[tokio::test]
    async fn second_owner_is_refused() {
        set_lock_timeout(100);
        let addr = Address::new_test();
        let first = ReshardingStore::open(&addr, key("store_owner"))
            .await
            .unwrap();

        let err = ReshardingStore::open(&addr, key("store_owner"))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::ReshardingLocked { owner, .. } if owner == instance_id()),
            "{err}"
        );

        drop(first);
        drop(open_after_drop(&addr, key("store_owner")).await);
    }

    #[tokio::test]
    async fn row_round_trips_and_lists() {
        let addr = Address::new_test();
        let key = key("store_rows");
        let store = ReshardingStore::open(&addr, key.clone()).await.unwrap();
        let row = row(key.clone());
        store.clear().await.unwrap();
        store.insert(&row).await.unwrap();
        assert!(store.insert(&row).await.is_err());

        store.set_stage(row.id, Stage::Replication).await.unwrap();
        let loaded = store.load().await.unwrap().unwrap();
        assert_eq!(loaded.stage, Stage::Replication);
        assert_eq!(
            ReshardingRecord {
                stage: Stage::CopyTables,
                ..loaded
            },
            row
        );

        let mut server = test_server().await;
        let listed = listed_for(&key).await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].as_ref().unwrap().key, key);
        assert_eq!(
            ReshardingStore::lock_owner(&mut server, &key)
                .await
                .unwrap(),
            Some(instance_id().to_owned())
        );

        store.delete(row.id).await.unwrap();
        assert_eq!(store.load().await.unwrap(), None);
        drop(store);
        drop(open_after_drop(&addr, key).await);
    }

    #[tokio::test]
    async fn writes_with_an_old_id_keep_the_new_row() {
        let addr = Address::new_test();
        let key = key("store_old_id");
        let store = ReshardingStore::open(&addr, key.clone()).await.unwrap();
        store.clear().await.unwrap();
        let old = row(key.clone());
        store.insert(&old).await.unwrap();
        store.delete(old.id).await.unwrap();
        let new = row(key.clone());
        store.insert(&new).await.unwrap();

        store.set_stage(old.id, Stage::Replication).await.unwrap();
        store
            .resume(
                old.id,
                &TaskOwner::current(pgdog_stats::TaskId::new(9)),
                now_millis(),
            )
            .await
            .unwrap();
        store.delete(old.id).await.unwrap();
        assert_eq!(store.load().await.unwrap(), Some(new.clone()));

        store.delete(new.id).await.unwrap();
        assert_eq!(store.load().await.unwrap(), None);
    }

    #[tokio::test]
    async fn writes_reconnect_after_connection_loss() {
        let addr = Address::new_test();
        let key = key("store_loss");
        let store = ReshardingStore::open(&addr, key.clone()).await.unwrap();
        store.clear().await.unwrap();
        let row = row(key.clone());
        store.insert(&row).await.unwrap();

        let old = backend_pid(&store).await;
        terminate_session(&store).await;

        store.set_stage(row.id, Stage::Replication).await.unwrap();
        assert_ne!(backend_pid(&store).await, old);
        assert_eq!(
            store.load().await.unwrap().unwrap().stage,
            Stage::Replication
        );
        assert!(matches!(
            ReshardingStore::lock_owner(&mut test_server().await, &key).await,
            Ok(Some(_))
        ));
        store.delete(row.id).await.unwrap();
    }

    fn set_lock_timeout(timeout: u64) {
        let mut updated = (*config()).clone();
        updated.config.resharding.state_heartbeat_interval = timeout;
        crate::config::set(updated).unwrap();
    }

    async fn terminate_session(store: &ReshardingStore) {
        let pid = backend_pid(store).await;
        test_server()
            .await
            .execute_checked(format!("SELECT pg_terminate_backend({pid})"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn failed_relock_keeps_writes_failing() {
        set_lock_timeout(100);
        let addr = Address::new_test();
        let key = key("store_relock");
        let store = ReshardingStore::open(&addr, key.clone()).await.unwrap();
        store.clear().await.unwrap();
        let row = row(key.clone());
        store.insert(&row).await.unwrap();

        terminate_session(&store).await;
        let other = open_after_drop(&addr, key.clone()).await;

        assert!(matches!(
            store.reconnect().await,
            Err(Error::ReshardingLocked { .. })
        ));
        assert!(store.delete(row.id).await.is_err());
        assert!(store.heartbeat().await.is_err());
        assert_eq!(other.load().await.unwrap(), Some(row.clone()));

        other.delete(row.id).await.unwrap();
    }

    #[tokio::test]
    async fn relock_waits_for_the_lock_holder() {
        set_lock_timeout(10_000);
        let addr = Address::new_test();
        let key = key("store_relock_wait");
        let store = ReshardingStore::open(&addr, key.clone()).await.unwrap();
        store.clear().await.unwrap();
        let row = row(key.clone());
        store.insert(&row).await.unwrap();

        terminate_session(&store).await;
        let other = open_after_drop(&addr, key.clone()).await;
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            other.unlock().await.unwrap();
            other
        });

        store.reconnect().await.unwrap();
        drop(release.await.unwrap());
        assert_eq!(store.load().await.unwrap(), Some(row.clone()));
        set_lock_timeout(100);
        assert!(matches!(
            ReshardingStore::open(&addr, key.clone()).await,
            Err(Error::ReshardingLocked { .. })
        ));

        store.delete(row.id).await.unwrap();
    }

    #[tokio::test]
    async fn resume_records_the_new_owner_and_time() {
        let addr = Address::new_test();
        let key = key("store_resume");
        let store = ReshardingStore::open(&addr, key.clone()).await.unwrap();
        store.clear().await.unwrap();
        let row = row(key.clone());
        store.insert(&row).await.unwrap();

        let owner = TaskOwner::current(pgdog_stats::TaskId::new(8));
        let resumed_at = now_millis();
        store.resume(row.id, &owner, resumed_at).await.unwrap();
        assert_eq!(
            store.load().await.unwrap().unwrap(),
            ReshardingRecord {
                owner,
                resumed_at: Some(resumed_at),
                ..row
            }
        );

        store.delete(row.id).await.unwrap();
    }

    fn time(utc: &str) -> SystemTime {
        utc.parse::<chrono::DateTime<chrono::Utc>>().unwrap().into()
    }

    /// A record with every column set to a value other than the defaults of [`row`].
    fn full_record(key: ReshardingKey) -> ReshardingRecord {
        let destination = ClusterSnapshot::new_test(4)
            .with_rule(serde_json::json!({"name": "users", "column": "tenant_id"}));
        ReshardingRecord {
            id: ReshardingId::generate(),
            key,
            command: ReshardingCommand::Reshard {
                skip_schema_sync: true,
                replicate_only: false,
                sync_only: true,
                auto_cutover: false,
                tables_only: true,
            },
            slot_name: "pgdog_full_slot".into(),
            stage: Stage::Replication,
            config: ConfigSnapshot::new_test(ClusterSnapshot::new_test(2), destination),
            owner: TaskOwner {
                instance_id: "pgdog-full-1".to_owned().into(),
                run_id: pgdog_stats::RunId::generate(),
                task_id: TaskId::new(4242),
            },
            format_version: FORMAT_VERSION,
            started_at: time("2026-11-18T11:06:40.123456Z"),
            resumed_at: Some(time("2026-11-18T11:07:40Z")),
        }
    }

    #[tokio::test]
    async fn record_is_saved_and_read_back() {
        let addr = Address::new_test();
        let key = key("store_full");
        let store = ReshardingStore::open(&addr, key.clone()).await.unwrap();
        store.clear().await.unwrap();
        let record = full_record(key.clone());
        store
            .insert(&ReshardingRecord {
                resumed_at: None,
                ..record.clone()
            })
            .await
            .unwrap();
        store
            .resume(record.id, &record.owner, record.resumed_at.unwrap())
            .await
            .unwrap();

        let loaded = store.load().await.unwrap().unwrap();
        assert_eq!(loaded.started_at, time("2026-11-18T11:06:40.123456Z"));
        assert_eq!(loaded.resumed_at, Some(time("2026-11-18T11:07:40Z")));
        assert_eq!(loaded, record);

        store.delete(record.id).await.unwrap();
    }
}
