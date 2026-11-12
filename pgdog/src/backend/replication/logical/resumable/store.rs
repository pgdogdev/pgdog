use pgdog_stats::Databases;
use tokio::sync::Mutex;

use super::super::{Error, subscriber::connect_address};
use super::queries::{
    CREATE_TABLE, DeleteRow, InsertRow, ListRows, LockHolder, Query, SelectRow, SetApplicationName,
    StageUpdate, TableExists, TryLock, Unlock, UpdateStage,
};
use super::{FORMAT_VERSION, ReshardingKey, ReshardingRecord, Stage, TaskOwner};
use crate::backend::{Server, pool::Address};
use crate::net::{DataRow, Format};
use crate::util::instance_id;
use pgdog_stats::InstanceId;

const APPLICATION_NAME_PREFIX: &str = "pgdog_resharding:";

#[derive(Debug)]
pub(crate) struct ReshardingStore {
    server: Mutex<Server>,
    key: ReshardingKey,
}

impl ReshardingStore {
    pub(crate) async fn open(addr: &Address, key: ReshardingKey) -> Result<Self, Error> {
        let mut server = connect_address(addr).await?;
        let application_name = format!("{APPLICATION_NAME_PREFIX}{}", instance_id());
        server
            .execute_checked(SetApplicationName::request(&application_name))
            .await?;
        server.execute_checked(CREATE_TABLE).await?;

        if !try_lock(&mut server, &key).await? {
            let owner = Self::lock_owner(&mut server, &key)
                .await?
                .unwrap_or_else(|| "unknown".into());
            return Err(Error::ReshardingLocked {
                key: key.to_string(),
                owner,
            });
        }

        Ok(Self {
            server: Mutex::new(server),
            key,
        })
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
        rows.first().map(parse_row).transpose()
    }

    pub(crate) async fn insert(&self, row: &ReshardingRecord) -> Result<(), Error> {
        self.server
            .lock()
            .await
            .execute_checked(InsertRow::request(row))
            .await?;
        Ok(())
    }

    pub(crate) async fn set_stage(&self, stage: Stage) -> Result<(), Error> {
        let update = StageUpdate {
            key: &self.key,
            stage,
        };
        self.server
            .lock()
            .await
            .execute_checked(UpdateStage::request(update))
            .await?;
        Ok(())
    }

    pub(crate) async fn delete(&self) -> Result<(), Error> {
        self.server
            .lock()
            .await
            .execute_checked(DeleteRow::request(&self.key))
            .await?;
        Ok(())
    }

    pub(crate) async fn ping(&self) -> Result<(), Error> {
        self.server.lock().await.execute_checked("SELECT 1").await?;
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) async fn list(
        server: &mut Server,
        destination_db: &str,
    ) -> Result<Vec<Result<ReshardingRecord, UnreadableRecord>>, Error> {
        let exists = server.fetch_all::<i32>(TableExists::request(())).await?;
        if exists.first() != Some(&1) {
            return Ok(vec![]);
        }

        server
            .fetch_all::<DataRow>(ListRows::request(destination_db))
            .await?
            .iter()
            .map(|row| {
                let key = parse_key(row)?;
                Ok(parse_body(row, key.clone()).map_err(|error| UnreadableRecord { key, error }))
            })
            .collect()
    }

    pub(crate) async fn lock_owner(
        server: &mut Server,
        key: &ReshardingKey,
    ) -> Result<Option<String>, Error> {
        if try_lock(server, key).await? {
            server.execute_checked(Unlock::request(key)).await?;
            return Ok(None);
        }

        let owner = server
            .fetch_all::<String>(LockHolder::request(key))
            .await?
            .into_iter()
            .next()
            .map(|name| match name.strip_prefix(APPLICATION_NAME_PREFIX) {
                Some(instance) => instance.to_owned(),
                None => name,
            })
            .unwrap_or_else(|| "unknown".into());
        Ok(Some(owner))
    }
}

async fn try_lock(server: &mut Server, key: &ReshardingKey) -> Result<bool, Error> {
    let locked = server.fetch_all::<i32>(TryLock::request(key)).await?;
    Ok(locked.first() == Some(&1))
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct UnreadableRecord {
    pub(crate) key: ReshardingKey,
    pub(crate) error: Error,
}

fn text(row: &DataRow, index: usize) -> Result<String, Error> {
    row.get::<String>(index, Format::Text)
        .ok_or_else(|| Error::MissingData)
}

fn parse_row(row: &DataRow) -> Result<ReshardingRecord, Error> {
    parse_body(row, parse_key(row)?)
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

fn parse_body(row: &DataRow, key: ReshardingKey) -> Result<ReshardingRecord, Error> {
    let format_version = text(row, 11)?
        .parse::<i32>()
        .map_err(|err| Error::ResumableFormat(format!("format_version: {err}")))?;
    if format_version > FORMAT_VERSION {
        return Err(Error::ResumableFormat(format!(
            "format version {format_version} is newer than {FORMAT_VERSION}"
        )));
    }

    Ok(ReshardingRecord {
        key,
        command: serde_json::from_str(&text(row, 3)?)
            .map_err(|err| Error::ResumableFormat(format!("command: {err}")))?,
        slot_name: text(row, 4)?,
        slot_existed: text(row, 5)? == "true",
        stage: text(row, 6)?.parse()?,
        config: serde_json::from_str(&text(row, 7)?)
            .map_err(|err| Error::ResumableFormat(format!("config: {err}")))?,
        owner: TaskOwner {
            instance_id: InstanceId::from(text(row, 8)?),
            run_id: text(row, 9)?
                .parse()
                .map_err(|err| Error::ResumableFormat(format!("run_id: {err}")))?,
            task_id: text(row, 10)?
                .parse()
                .map_err(|err| Error::ResumableFormat(format!("task_id: {err}")))?,
        },
        format_version,
    })
}

#[cfg(test)]
mod test {
    use super::super::{ConfigSnapshot, ReshardingCommand};
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

    fn row(key: ReshardingKey) -> ReshardingRecord {
        let config: ConfigSnapshot = serde_json::from_str(
            r#"{"source":{"shards":1,"sharded_tables":[],"sharded_mappings":[],"omnisharded_tables":[],"sharded_schemas":[]},
            "destination":{"shards":2,"sharded_tables":[],"sharded_mappings":[],"omnisharded_tables":[],"sharded_schemas":[]}}"#,
        )
        .unwrap();
        ReshardingRecord {
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

        let mut server = test_server().await;
        let listed = ReshardingStore::list(&mut server, &first_key.databases.destination)
            .await
            .unwrap()
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

        assert!(matches!(new.load().await, Err(Error::ResumableFormat(_))));

        let mut server = test_server().await;
        let listed = ReshardingStore::list(&mut server, &old_key.databases.destination)
            .await
            .unwrap();
        assert_eq!(listed.len(), 2);
        assert!(
            matches!(&listed[0], Err(UnreadableRecord { key, error: Error::ResumableFormat(_) }) if *key == new_key)
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
        let listed = ReshardingStore::list(&mut server, &key.databases.destination)
            .await
            .unwrap();
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
        assert!(store.ping().await.is_err());
        drop(store);

        let store = open_after_drop(&addr, key).await;
        assert_eq!(store.load().await.unwrap().unwrap().stage, Stage::Copy);
        store.delete().await.unwrap();
    }
}
