use std::fmt::Display;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pgdog_stats::{Databases, InstanceId};
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

use super::super::{Error, subscriber::connect_address};
use super::queries::{
    CREATE_TABLE, DeleteRow, Heartbeat, InsertRow, ListRows, LockHolder, Query, ResumeRow,
    ResumeUpdate, SelectRow, SetupSession, StageUpdate, TableExists, TryLock, Unlock, UpdateStage,
};
use super::{FORMAT_VERSION, ReshardingKey, ReshardingRecord, Stage, TaskOwner};
use crate::backend::server::ServerRequest;
use crate::backend::{Server, pool::Address};
use crate::net::{DataRow, Format};
use crate::util::instance_id;

const APPLICATION_NAME_PREFIX: &str = "pgdog_resharding:";
const RELOCK_ATTEMPTS: usize = 20;
const RELOCK_DELAY: Duration = Duration::from_millis(250);

#[derive(Debug)]
pub(crate) struct ReshardingStore {
    server: Mutex<Server>,
    addr: Address,
    key: ReshardingKey,
}

impl ReshardingStore {
    pub(crate) async fn open(addr: &Address, key: ReshardingKey) -> Result<Self, Error> {
        let mut server = connect_session(addr).await?;
        if !try_lock(&mut server, &key).await? {
            return Err(locked(&mut server, &key).await?);
        }

        Ok(Self {
            server: Mutex::new(server),
            addr: addr.clone(),
            key,
        })
    }

    pub(crate) async fn reconnect(&self) -> Result<(), Error> {
        let mut server = self.server.lock().await;
        *server = connect_session(&self.addr).await?;
        for _ in 0..RELOCK_ATTEMPTS {
            if try_lock(&mut server, &self.key).await? {
                return Ok(());
            }
            tokio::time::sleep(RELOCK_DELAY).await;
        }
        Err(locked(&mut server, &self.key).await?)
    }

    pub(crate) fn key(&self) -> &ReshardingKey {
        &self.key
    }

    pub(crate) async fn load(&self) -> Result<Option<ReshardingRecord>, Error> {
        let rows = self
            .server
            .lock()
            .await
            .fetch_all::<DataRow>(SelectRow::request(&self.key))
            .await?;
        rows.first()
            .map(|row| parse_record(row, self.key.clone()))
            .transpose()
    }

    pub(crate) async fn insert(&self, row: &ReshardingRecord) -> Result<(), Error> {
        self.execute(InsertRow::request(row)).await
    }

    pub(crate) async fn set_stage(&self, stage: Stage) -> Result<(), Error> {
        self.execute(UpdateStage::request(StageUpdate {
            key: &self.key,
            stage,
        }))
        .await
    }

    pub(crate) async fn resume(
        &self,
        owner: &TaskOwner,
        resumed_at: SystemTime,
    ) -> Result<(), Error> {
        self.execute(ResumeRow::request(ResumeUpdate {
            key: &self.key,
            owner,
            resumed_at,
        }))
        .await
    }

    pub(crate) async fn delete(&self) -> Result<(), Error> {
        self.execute(DeleteRow::request(&self.key)).await
    }

    pub(crate) async fn heartbeat(&self) -> Result<(), Error> {
        self.execute(Heartbeat::request(())).await
    }

    pub(crate) async fn unlock(&self) -> Result<(), Error> {
        self.execute(Unlock::request(&self.key)).await
    }

    async fn execute(&self, request: impl Into<ServerRequest>) -> Result<(), Error> {
        self.server.lock().await.execute_checked(request).await?;
        Ok(())
    }

    pub(crate) async fn read(
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

    pub(crate) async fn list(
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

    pub(crate) async fn lock_owner(
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

async fn try_lock(server: &mut Server, key: &ReshardingKey) -> Result<bool, Error> {
    let locked = server.fetch_all::<i32>(TryLock::request(key)).await?;
    Ok(locked.first() == Some(&1))
}

async fn connect_session(addr: &Address) -> Result<Server, Error> {
    let mut server = connect_address(addr).await?;
    let application_name = format!("{APPLICATION_NAME_PREFIX}{}", instance_id());
    server
        .execute_checked(SetupSession::request(&application_name))
        .await?;
    server.execute_checked(CREATE_TABLE).await?;
    Ok(server)
}

async fn locked(server: &mut Server, key: &ReshardingKey) -> Result<Error, Error> {
    let owner = ReshardingStore::lock_owner(server, key)
        .await?
        .unwrap_or_else(|| "unknown".into());
    Ok(Error::ReshardingLocked {
        key: key.to_string(),
        owner,
    })
}

#[derive(Debug)]
pub(crate) struct UnreadableRecord {
    pub(crate) key: ReshardingKey,
    pub(crate) error: Error,
}

fn text(row: &DataRow, index: usize) -> Result<String, Error> {
    row.get::<String>(index, Format::Text)
        .ok_or_else(|| Error::MissingData)
}

fn parsed<T>(row: &DataRow, index: usize, field: &str) -> Result<T, Error>
where
    T: FromStr,
    T::Err: Display,
{
    text(row, index)?
        .parse()
        .map_err(|err| Error::ReshardingFormat(format!("{field}: {err}")))
}

fn json<T: DeserializeOwned>(row: &DataRow, index: usize, field: &str) -> Result<T, Error> {
    serde_json::from_str(&text(row, index)?)
        .map_err(|err| Error::ReshardingFormat(format!("{field}: {err}")))
}

fn parse_key(row: &DataRow) -> Result<ReshardingKey, Error> {
    Ok(ReshardingKey {
        databases: Databases {
            source: text(row, 0)?,
            destination: text(row, 1)?,
        },
        publication: text(row, 2)?,
    })
}

fn parse_record(row: &DataRow, key: ReshardingKey) -> Result<ReshardingRecord, Error> {
    let format_version: i32 = parsed(row, 11, "format_version")?;
    if format_version > FORMAT_VERSION {
        return Err(Error::ReshardingFormat(format!(
            "format version {format_version} is newer than {FORMAT_VERSION}"
        )));
    }

    Ok(ReshardingRecord {
        id: parsed(row, 12, "id")?,
        key,
        command: json(row, 3, "command")?,
        slot_name: text(row, 4)?,
        slot_existed: parsed(row, 5, "slot_existed")?,
        stage: text(row, 6)?.parse()?,
        config: json(row, 7, "config")?,
        owner: TaskOwner {
            instance_id: InstanceId::from(text(row, 8)?),
            run_id: parsed(row, 9, "run_id")?,
            task_id: parsed(row, 10, "task_id")?,
        },
        format_version,
        started_at: millis(row, 13, "started_at")?,
        resumed_at: match text(row, 14)?.as_str() {
            "" => None,
            _ => Some(millis(row, 14, "resumed_at")?),
        },
    })
}

fn millis(row: &DataRow, index: usize, field: &str) -> Result<SystemTime, Error> {
    Ok(UNIX_EPOCH + Duration::from_millis(parsed::<i64>(row, index, field)?.max(0) as u64))
}

#[cfg(test)]
mod test {
    use super::super::{ConfigSnapshot, ReshardingCommand, ReshardingId};
    use super::*;
    use crate::backend::server::test::test_server;

    async fn backend_pid(store: &ReshardingStore) -> i32 {
        store
            .server
            .lock()
            .await
            .fetch_all::<i32>("SELECT pg_backend_pid()")
            .await
            .unwrap()[0]
    }

    fn key(name: &str) -> ReshardingKey {
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

    fn row(key: ReshardingKey) -> ReshardingRecord {
        let config: ConfigSnapshot = serde_json::from_str(
            r#"{"source":{"shards":1,"sharded_tables":[],"sharded_mappings":[],"omnisharded_tables":[],"sharded_schemas":[]},
            "destination":{"shards":2,"sharded_tables":[],"sharded_mappings":[],"omnisharded_tables":[],"sharded_schemas":[]}}"#,
        )
        .unwrap();
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
            slot_existed: false,
            stage: Stage::Copy,
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
            store.delete().await.unwrap();
        }
        first.insert(&row(first_key.clone())).await.unwrap();
        second.insert(&row(second_key.clone())).await.unwrap();

        let listed = listed_for(&first_key)
            .await
            .into_iter()
            .map(|row| row.unwrap().key)
            .collect::<Vec<_>>();
        assert_eq!(listed, [second_key, first_key]);

        first.delete().await.unwrap();
        assert!(second.load().await.unwrap().is_some());
        second.delete().await.unwrap();
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
            store.delete().await.unwrap();
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

        old.delete().await.unwrap();
        new.delete().await.unwrap();
    }

    #[tokio::test]
    async fn ddl_is_idempotent() {
        let addr = Address::new_test();
        let store = ReshardingStore::open(&addr, key("store_ddl"))
            .await
            .unwrap();
        drop(store);
        let store = open_after_drop(&addr, key("store_ddl")).await;
        assert_eq!(store.load().await.unwrap(), None);
    }

    #[tokio::test]
    async fn second_owner_is_refused() {
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
        store.delete().await.unwrap();
        store.insert(&row).await.unwrap();
        assert!(store.insert(&row).await.is_err());

        store.set_stage(Stage::Replication).await.unwrap();
        let loaded = store.load().await.unwrap().unwrap();
        assert_eq!(loaded.stage, Stage::Replication);
        assert_eq!(
            ReshardingRecord {
                stage: Stage::Copy,
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

        store.delete().await.unwrap();
        assert_eq!(store.load().await.unwrap(), None);
        drop(store);
        drop(open_after_drop(&addr, key).await);
    }

    #[tokio::test]
    async fn writes_fail_after_connection_loss() {
        let addr = Address::new_test();
        let key = key("store_loss");
        let store = ReshardingStore::open(&addr, key.clone()).await.unwrap();
        store.delete().await.unwrap();
        store.insert(&row(key.clone())).await.unwrap();

        let pid = backend_pid(&store).await;
        let mut server = test_server().await;
        server
            .execute_checked(format!("SELECT pg_terminate_backend({pid})"))
            .await
            .unwrap();

        assert!(store.set_stage(Stage::Replication).await.is_err());
        assert!(store.heartbeat().await.is_err());
        drop(store);

        let store = open_after_drop(&addr, key).await;
        assert_eq!(store.load().await.unwrap().unwrap().stage, Stage::Copy);
        store.delete().await.unwrap();
    }

    #[tokio::test]
    async fn resume_records_the_new_owner_and_time() {
        let addr = Address::new_test();
        let key = key("store_resume");
        let store = ReshardingStore::open(&addr, key.clone()).await.unwrap();
        store.delete().await.unwrap();
        let row = row(key.clone());
        store.insert(&row).await.unwrap();

        let owner = TaskOwner::current(pgdog_stats::TaskId::new(8));
        let resumed_at = now_millis();
        store.resume(&owner, resumed_at).await.unwrap();
        assert_eq!(
            store.load().await.unwrap().unwrap(),
            ReshardingRecord {
                owner,
                resumed_at: Some(resumed_at),
                ..row
            }
        );

        store.delete().await.unwrap();
    }
}
