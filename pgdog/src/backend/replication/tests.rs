use std::time::Duration;

use pgdog_config::{ConfigAndUsers, Database, ShardedTableConfig, User};
use tokio_util::sync::CancellationToken;

use super::logical::publisher::replication_progress::ReplicationProgress;
use super::logical::{Error, data_sync::DataSync, publisher::publisher_impl::Publisher};
use crate::{
    api::{
        replication::ReplicationSlotTask,
        run_task,
        schema_sync::{SchemaSyncPhase, SchemaSyncTask},
        task::TaskError,
    },
    backend::{
        Cluster, ConnectReason, Error as BackendError, Server, ServerOptions, databases,
        pool::{Address, Request},
        schema::sync::SchemaSyncError,
        server::test::test_server,
    },
    config::{config, set},
};

async fn setup_replication_test(
    admin: &mut Server,
    schema: &str,
    destination: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut test_config = ConfigAndUsers::default();
    test_config.config.replication.pg_dump_path = config().config.replication.pg_dump_path.clone();
    for database in [schema, destination] {
        admin
            .execute_checked(format!("CREATE DATABASE {database} TEMPLATE template0"))
            .await?;
        test_config.config.databases.push(Database {
            name: database.into(),
            host: "127.0.0.1".into(),
            port: 5432,
            ..Default::default()
        });
        let mut user = User::new("pgdog", "pgdog", database);
        user.schema_admin = true;
        test_config.users.users.push(user);
        for table in ["parents", "children"] {
            test_config.config.sharded_tables.push(ShardedTableConfig {
                database: database.into(),
                name: Some(table.into()),
                schema: Some(schema.into()),
                column: "tenant_id".into(),
                ..Default::default()
            });
        }
    }
    set(test_config)?;
    databases::init()?;
    Ok(())
}

async fn replicate_until_caught_up(
    publisher: &mut Publisher,
    source: &Cluster,
    destination: &Cluster,
    slot_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut server = source.primary(0, &Request::default()).await?;
    let target: Vec<String> = server
        .fetch_all("SELECT pg_current_wal_lsn()::text")
        .await?;
    let target = target.first().ok_or(Error::MissingData)?;
    let query = format!(
        "SELECT 1::bigint FROM pg_replication_slots \
         WHERE slot_name = '{slot_name}_0' \
         AND confirmed_flush_lsn >= '{target}'::pg_lsn"
    );
    let stop = CancellationToken::new();
    let streams = publisher.prepare_replication(source, &stop).await?;
    let progress = ReplicationProgress::new(source.shards().len());
    let handles: Vec<_> = streams
        .into_iter()
        .map(|stream| {
            let updater = progress.shard(stream.source_shard);
            let task = ReplicationSlotTask::new(stream, source, destination, stop.clone(), updater);
            run_task(task)
        })
        .collect();

    let caught_up = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let rows: Vec<i64> = server.fetch_all(&query).await?;
            if rows == [1] {
                return Ok::<_, Box<dyn std::error::Error>>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    stop.cancel();
    let mut drained = Ok(());
    for handle in handles {
        drained = drained.and(handle.await);
    }
    caught_up??;
    drained?;
    Ok(())
}

async fn cleanup_replication_test(
    publisher: &mut Publisher,
    admin: &mut Server,
    original_config: &ConfigAndUsers,
    test_databases: [&str; 2],
) -> Result<(), Box<dyn std::error::Error>> {
    let cleanup = Box::pin(async {
        publisher.cleanup().await?;
        for database in test_databases {
            let slots: Vec<String> = admin
                .fetch_all(format!(
                    "SELECT slot_name FROM pg_replication_slots WHERE database = '{database}'"
                ))
                .await?;
            if slots.is_empty() {
                continue;
            }
            let mut server = Server::connect(
                &Address {
                    database_name: database.into(),
                    ..Address::new_test()
                },
                ServerOptions::new_replication(),
                ConnectReason::Resharding,
                Default::default(),
            )
            .await?;
            for slot in slots {
                match tokio::time::timeout(
                    Duration::from_secs(10),
                    server.execute_checked(format!("DROP_REPLICATION_SLOT {slot} WAIT")),
                )
                .await?
                {
                    Ok(_) => {}
                    Err(BackendError::ExecutionError(error)) if error.code == "42704" => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .await;
    set(original_config.clone())?;
    databases::reload_from_existing()?;
    for database in test_databases {
        admin
            .execute_checked(format!("DROP DATABASE IF EXISTS {database} WITH (FORCE)"))
            .await?;
    }
    cleanup
}

// Verify the case when the data related to fk update happened during tables
// copy. We can hit the constraint violations when between the related
// tables copies the updates to fk rows happened. Since we copy the table
// with specific snapshot at the moment of copy start and we doesn't start
// copying at the same moment, we can get the snapshots related to different
// moment of times and they could diverge in cross relations.
// Hitting the error
// `insert or update on table \"children\" violates foreign key constraint \"children_parent_fk\"",
// detail: Some("Key (parent_id)=(1) is not present in table \"parents\"`
#[tokio::test]
async fn test_replication_fk_conflicts_after_delete_during_copy()
-> Result<(), Box<dyn std::error::Error>> {
    let schema = "fk_replication_test".to_owned();
    let destination = format!("{schema}_dest");
    let parent = format!("{schema}.parents");
    let child = format!("{schema}.children");
    let original_config = config();
    let mut admin = test_server().await;
    let mut publisher = Publisher::new(&schema, schema.clone());
    let result = async {
        setup_replication_test(&mut admin, &schema, &destination).await?;
        let source = databases::databases().schema_owner(&schema)?;
        let mut source_server = source.primary(0, &Request::default()).await?;

        // create the tables tied by fk and put some data there
        source_server
            .execute_checked(format!(
                "CREATE SCHEMA {schema}; \
                 CREATE TABLE {parent} (id BIGINT PRIMARY KEY, tenant_id BIGINT NOT NULL); \
                 CREATE TABLE {child} (id BIGINT PRIMARY KEY, tenant_id BIGINT NOT NULL, \
                 parent_id BIGINT, CONSTRAINT children_parent_fk \
                 FOREIGN KEY (parent_id) REFERENCES {parent}(id)); \
                 INSERT INTO {parent} VALUES (1, 1), (2, 1), (3, 1); \
                 INSERT INTO {child} VALUES (1, 1, 1), (2, 1, 2), (3, 1, 3); \
                 CREATE PUBLICATION {schema} FOR TABLE {parent}, {child}"
            ))
            .await?;

        // reproduce the flow ran by pgdog - schema sync, data copy, replication.
        let schema_sync = SchemaSyncTask::builder()
            .databases(pgdog_stats::Databases {
                source: schema.clone(),
                destination: destination.clone(),
            })
            .publication(schema.clone());
        // copy initial schema
        run_task(schema_sync.clone().phase(SchemaSyncPhase::Pre).build()).await?;
        let source = databases::databases().schema_owner(&schema)?;
        let dest = databases::databases().schema_owner(&destination)?;
        let cancel = CancellationToken::new();
        publisher.sync_tables(true, &source).await?;
        publisher.create_slots(&source, &cancel).await?;
        let tables = publisher.tables.get(&0).ok_or(Error::MissingData)?;
        let child_table = tables
            .iter()
            .find(|table| table.table.name == "children")
            .ok_or(Error::MissingData)?;
        let parent_table = tables
            .iter()
            .find(|table| table.table.name == "parents")
            .ok_or(Error::MissingData)?;
        let sync = DataSync {
            source: &source,
            dest: &dest,
            format: config().config.general.resharding_copy_format,
        };
        // copy the child table first, so it won't have updates we'll do during copy
        let child_table = sync
            .copy_table(child_table, source_server.addr(), &cancel, |_| {})
            .await?;

        // update the fk related data, so it would be present
        // only in parent table snapshot
        source_server
            .execute_checked(format!(
                "BEGIN; \
                 DELETE FROM {child} WHERE id = 1; \
                 DELETE FROM {parent} WHERE id = 1; \
                 COMMIT"
            ))
            .await?;

        // add another pair, then clear the reference and delete its parent
        source_server
            .execute_checked(format!(
                "BEGIN; \
                 INSERT INTO {parent} VALUES (4, 1); \
                 INSERT INTO {child} VALUES (4, 1, 4); \
                 COMMIT"
            ))
            .await?;
        source_server
            .execute_checked(format!(
                "BEGIN; \
                 UPDATE {child} SET parent_id = NULL WHERE id = 4; \
                 DELETE FROM {parent} WHERE id = 4; \
                 COMMIT"
            ))
            .await?;

        // and now start the copy of parent table.
        // that should have an updated snapshot already with the queries
        // executed above.
        let parent_table = sync
            .copy_table(parent_table, source_server.addr(), &cancel, |_| {})
            .await?;
        publisher.post_data_sync([(0, vec![child_table, parent_table])].into());
        drop(source_server);
        run_task(schema_sync.clone().phase(SchemaSyncPhase::Post).build()).await?;

        // run the replication and wait for all data to be copied
        replicate_until_caught_up(&mut publisher, &source, &dest, &schema).await?;
        run_task(schema_sync.phase(SchemaSyncPhase::Cutover).build()).await?;
        Ok::<_, Box<dyn std::error::Error>>(dest)
    }
    .await;

    let validation = async {
        let dest = result?;
        let mut server = dest.primary(0, &Request::default()).await?;
        let parents: Vec<i64> = server
            .fetch_all(format!("SELECT id FROM {parent} ORDER BY id"))
            .await?;
        let children: Vec<i64> = server
            .fetch_all(format!("SELECT id FROM {child} ORDER BY id"))
            .await?;
        let parent_ids: Vec<String> = server
            .fetch_all(format!(
                "SELECT COALESCE(parent_id::text, 'null') FROM {child} ORDER BY id"
            ))
            .await?;
        Ok::<_, Box<dyn std::error::Error>>((parents, children, parent_ids))
    }
    .await;

    cleanup_replication_test(
        &mut publisher,
        &mut admin,
        &original_config,
        [&schema, &destination],
    )
    .await?;
    let (parents, children, parent_ids) = validation?;
    assert_eq!(parents, [2, 3]);
    assert_eq!(children, [2, 3, 4]);
    assert_eq!(parent_ids, ["2", "3", "null"]);
    Ok(())
}

// Verify the case when the source tables have fk constraints
// and that during the copy this constraints doesn't fire if
// the other table data is not yet present on the destination.
#[tokio::test]
async fn test_replication_fk_constraints_after_copy_child_before_parent()
-> Result<(), Box<dyn std::error::Error>> {
    let schema = "fk_copy_test";
    let destination = "fk_copy_test_dest";
    let original_config = config();
    let mut admin = test_server().await;
    let mut publisher = Publisher::new(schema, schema.into());
    let result = async {
        setup_replication_test(&mut admin, schema, destination).await?;
        let source = databases::databases().schema_owner(schema)?;
        let mut server = source.primary(0, &Request::default()).await?;

        // create the tables tied by fk and put some data there
        server
            .execute_checked(format!(
                "CREATE SCHEMA {schema}; \
                 CREATE TABLE {schema}.parents (id BIGINT PRIMARY KEY, tenant_id BIGINT NOT NULL); \
                 CREATE TABLE {schema}.children (id BIGINT PRIMARY KEY, tenant_id BIGINT NOT NULL, \
                 parent_id BIGINT REFERENCES {schema}.parents(id)); \
                 INSERT INTO {schema}.parents VALUES (1, 1); \
                 INSERT INTO {schema}.children VALUES (1, 1, 1); \
                 CREATE PUBLICATION {schema} FOR TABLE {schema}.parents, {schema}.children"
            ))
            .await?;
        let schema_sync = SchemaSyncTask::builder()
            .databases(pgdog_stats::Databases {
                source: schema.into(),
                destination: destination.into(),
            })
            .publication(schema.to_owned());

        // copy initial schema
        run_task(schema_sync.clone().phase(SchemaSyncPhase::Pre).build()).await?;
        let source = databases::databases().schema_owner(schema)?;
        let dest = databases::databases().schema_owner(destination)?;
        let cancel = CancellationToken::new();
        publisher.sync_tables(true, &source).await?;
        publisher.create_slots(&source, &cancel).await?;
        let tables = publisher.tables.get(&0).ok_or(Error::MissingData)?;
        let child = tables
            .iter()
            .find(|table| table.table.name == "children")
            .ok_or(Error::MissingData)?;
        let parent = tables
            .iter()
            .find(|table| table.table.name == "parents")
            .ok_or(Error::MissingData)?;
        let sync = DataSync {
            source: &source,
            dest: &dest,
            format: config().config.general.resharding_copy_format,
        };

        // copy the child table first, while the parent data is not yet present
        let child = sync
            .copy_table(child, server.addr(), &cancel, |_| {})
            .await?;

        // and now copy the parent table
        let parent = sync
            .copy_table(parent, server.addr(), &cancel, |_| {})
            .await?;
        publisher.post_data_sync([(0, vec![child, parent])].into());

        // add rows after copy so replication must deliver them
        server
            .execute_checked(format!(
                "BEGIN; \
                 INSERT INTO {schema}.parents VALUES (2, 1); \
                 INSERT INTO {schema}.children VALUES (2, 1, 2); \
                 COMMIT"
            ))
            .await?;
        drop(server);

        run_task(schema_sync.clone().phase(SchemaSyncPhase::Post).build()).await?;
        replicate_until_caught_up(&mut publisher, &source, &dest, schema).await?;
        run_task(schema_sync.phase(SchemaSyncPhase::Cutover).build()).await?;
        Ok::<_, Box<dyn std::error::Error>>(dest)
    }
    .await;

    // check that both tables have the copied rows and the fk still points to the parent
    let validation = async {
        let dest = result?;
        let mut server = dest.primary(0, &Request::default()).await?;
        let parents: Vec<String> = server
            .fetch_all(format!(
                "SELECT id || ':' || tenant_id FROM {schema}.parents ORDER BY id"
            ))
            .await?;
        let children: Vec<String> = server
            .fetch_all(format!(
                "SELECT id || ':' || tenant_id || ':' || parent_id \
                 FROM {schema}.children ORDER BY id"
            ))
            .await?;
        Ok::<_, Box<dyn std::error::Error>>((parents, children))
    }
    .await;

    cleanup_replication_test(
        &mut publisher,
        &mut admin,
        &original_config,
        [schema, destination],
    )
    .await?;
    let (parents, children) = validation?;
    assert_eq!(parents, ["1:1", "2:1"]);
    assert_eq!(children, ["1:1:1", "2:1:2"]);
    Ok(())
}

// Almost the same like [`test_replication_fk_conflicts_after_update_during_copy`] test, but
// this one uses the user trigger instead of FK. The user trigger should not fail during
// the replication for the fame reason as FK for the mentioned test.
#[tokio::test]
async fn test_replication_copy_custom_parent_trigger() -> Result<(), Box<dyn std::error::Error>> {
    let schema = "trigger_copy_test";
    let destination = "trigger_copy_test_dest";
    let original_config = config();
    let mut admin = test_server().await;
    let mut publisher = Publisher::new(schema, schema.into());
    let result = async {
        setup_replication_test(&mut admin, schema, destination).await?;
        let source = databases::databases().schema_owner(schema)?;
        let mut server = source.primary(0, &Request::default()).await?;
        // create valid source data with a trigger instead of a foreign key
        server
            .execute_checked(format!(
                "CREATE SCHEMA {schema}; \
                 CREATE TABLE {schema}.parents (id BIGINT PRIMARY KEY, tenant_id BIGINT NOT NULL); \
                 CREATE TABLE {schema}.children (id BIGINT PRIMARY KEY, tenant_id BIGINT NOT NULL, \
                 parent_id BIGINT); \
                 CREATE FUNCTION {schema}.check_parent() RETURNS trigger LANGUAGE plpgsql AS $$ \
                 BEGIN \
                     IF NEW.parent_id IS NOT NULL AND NOT EXISTS \
                         (SELECT 1 FROM {schema}.parents WHERE id = NEW.parent_id) THEN \
                         RAISE EXCEPTION 'parent is missing' USING ERRCODE = '23503'; \
                     END IF; \
                     RETURN NEW; \
                 END; $$; \
                 CREATE TRIGGER children_parent_check BEFORE INSERT OR UPDATE \
                 ON {schema}.children FOR EACH ROW EXECUTE FUNCTION {schema}.check_parent(); \
                 INSERT INTO {schema}.parents VALUES (1, 1); \
                 INSERT INTO {schema}.children VALUES (1, 1, 1); \
                 CREATE PUBLICATION {schema} FOR TABLE {schema}.parents, {schema}.children"
            ))
            .await?;

        // copy the schema and trigger before copying any rows
        let schema_sync = SchemaSyncTask::builder()
            .databases(pgdog_stats::Databases {
                source: schema.into(),
                destination: destination.into(),
            })
            .publication(schema.to_owned());
        run_task(schema_sync.clone().phase(SchemaSyncPhase::Pre).build()).await?;
        let source = databases::databases().schema_owner(schema)?;
        let dest = databases::databases().schema_owner(destination)?;
        let cancel = CancellationToken::new();
        publisher.sync_tables(true, &source).await?;
        publisher.create_slots(&source, &cancel).await?;
        let tables = publisher.tables.get(&0).ok_or(Error::MissingData)?;
        let child = tables
            .iter()
            .find(|table| table.table.name == "children")
            .ok_or(Error::MissingData)?;
        let parent = tables
            .iter()
            .find(|table| table.table.name == "parents")
            .ok_or(Error::MissingData)?;
        let sync = DataSync {
            source: &source,
            dest: &dest,
            format: config().config.general.resharding_copy_format,
        };
        // copy the child first, while its parent is still missing
        let child = sync
            .copy_table(child, server.addr(), &cancel, |_| {})
            .await?;
        let parent = sync
            .copy_table(parent, server.addr(), &cancel, |_| {})
            .await?;
        publisher.post_data_sync([(0, vec![child, parent])].into());

        // add rows after copy so replication must deliver them
        server
            .execute_checked(format!(
                "BEGIN; \
                 INSERT INTO {schema}.parents VALUES (2, 1); \
                 INSERT INTO {schema}.children VALUES (2, 1, 2); \
                 COMMIT"
            ))
            .await?;
        drop(server);
        run_task(schema_sync.clone().phase(SchemaSyncPhase::Post).build()).await?;
        replicate_until_caught_up(&mut publisher, &source, &dest, schema).await?;
        run_task(schema_sync.phase(SchemaSyncPhase::Cutover).build()).await?;
        Ok::<_, Box<dyn std::error::Error>>(dest)
    }
    .await;

    let validation = async {
        let dest = result?;
        // check the copied parent and the child's reference
        let mut server = dest.primary(0, &Request::default()).await?;
        let parents: Vec<i64> = server
            .fetch_all(format!("SELECT id FROM {schema}.parents ORDER BY id"))
            .await?;
        let children: Vec<i64> = server
            .fetch_all(format!(
                "SELECT parent_id FROM {schema}.children ORDER BY id"
            ))
            .await?;
        Ok::<_, Box<dyn std::error::Error>>((parents, children))
    }
    .await;

    cleanup_replication_test(
        &mut publisher,
        &mut admin,
        &original_config,
        [schema, destination],
    )
    .await?;
    let (parents, children) = validation?;
    assert_eq!(parents, [1, 2]);
    assert_eq!(children, [1, 2]);
    Ok(())
}

// Verify that we catch some data inconsistencies after resharding
// in case we created one. It's created artificially during copy,
// since we don't know for cases when we do this wrong for now.
#[ignore = "No validation for now"]
#[tokio::test]
async fn test_replication_fk_inconsistent_check_on_cutover()
-> Result<(), Box<dyn std::error::Error>> {
    let schema = "fk_post_copy_test";
    let destination = "fk_post_copy_test_dest";
    let original_config = config();
    let mut admin = test_server().await;
    let mut publisher = Publisher::new(schema, schema.into());
    let result = async {
        setup_replication_test(&mut admin, schema, destination).await?;
        let source = databases::databases().schema_owner(schema)?;
        let mut server = source.primary(0, &Request::default()).await?;
        server
            .execute_checked(format!(
                "CREATE SCHEMA {schema}; \
                 CREATE TABLE {schema}.parents (id BIGINT PRIMARY KEY, tenant_id BIGINT NOT NULL); \
                 CREATE TABLE {schema}.children (id BIGINT PRIMARY KEY, tenant_id BIGINT NOT NULL, \
                 parent_id BIGINT, CONSTRAINT children_parent_fk \
                 FOREIGN KEY (parent_id) REFERENCES {schema}.parents(id)); \
                 INSERT INTO {schema}.parents VALUES (1, 1); \
                 INSERT INTO {schema}.children VALUES (1, 1, 1); \
                 CREATE PUBLICATION {schema} FOR TABLE {schema}.parents, {schema}.children"
            ))
            .await?;

        // copy the tables without their foreign key
        let schema_sync = SchemaSyncTask::builder()
            .databases(pgdog_stats::Databases {
                source: schema.into(),
                destination: destination.into(),
            })
            .publication(schema.to_owned());
        run_task(schema_sync.clone().phase(SchemaSyncPhase::Pre).build()).await?;
        let source = databases::databases().schema_owner(schema)?;
        let cancel = CancellationToken::new();
        publisher.sync_tables(true, &source).await?;
        publisher.create_slots(&source, &cancel).await?;
        let dest = databases::databases().schema_owner(destination)?;
        let tables = publisher.tables.get(&0).ok_or(Error::MissingData)?;
        let child = tables
            .iter()
            .find(|table| table.table.name == "children")
            .ok_or(Error::MissingData)?;
        let parent = tables
            .iter()
            .find(|table| table.table.name == "parents")
            .ok_or(Error::MissingData)?;
        let sync = DataSync {
            source: &source,
            dest: &dest,
            format: config().config.general.resharding_copy_format,
        };
        let child = sync
            .copy_table(child, server.addr(), &cancel, |_| {})
            .await?;
        let mut destination_server = dest.primary(0, &Request::default()).await?;
        // leave an orphan that replication cannot repair
        destination_server
            .execute_checked(format!("INSERT INTO {schema}.children VALUES (42, 1, 999)"))
            .await?;
        drop(destination_server);
        let parent = sync
            .copy_table(parent, server.addr(), &cancel, |_| {})
            .await?;
        publisher.post_data_sync([(0, vec![child, parent])].into());

        // add valid rows that must arrive through replication
        server
            .execute_checked(format!(
                "BEGIN; \
                 INSERT INTO {schema}.parents VALUES (2, 1); \
                 INSERT INTO {schema}.children VALUES (2, 1, 2); \
                 COMMIT"
            ))
            .await?;
        drop(server);

        run_task(schema_sync.clone().phase(SchemaSyncPhase::Post).build()).await?;

        // wait for the valid source rows to arrive without repairing the orphan
        replicate_until_caught_up(&mut publisher, &source, &dest, schema).await?;

        // cutover should reject the orphan left on the destination
        let cutover = run_task(schema_sync.phase(SchemaSyncPhase::Cutover).build()).await;
        Ok::<_, Box<dyn std::error::Error>>((dest, cutover))
    }
    .await;

    let validation = async {
        let (dest, cutover) = result?;
        let mut server = dest.primary(0, &Request::default()).await?;
        let parents: Vec<i64> = server
            .fetch_all(format!("SELECT id FROM {schema}.parents ORDER BY id"))
            .await?;
        let children: Vec<i64> = server
            .fetch_all(format!(
                "SELECT parent_id FROM {schema}.children ORDER BY id"
            ))
            .await?;
        Ok::<_, Box<dyn std::error::Error>>((parents, children, cutover))
    }
    .await;

    cleanup_replication_test(
        &mut publisher,
        &mut admin,
        &original_config,
        [schema, destination],
    )
    .await?;
    let (parents, children, cutover) = validation?;
    assert_eq!(parents, [1, 2]);
    assert_eq!(children, [1, 2, 999]);
    assert!(
        matches!(
            &cutover,
            Err(TaskError::Failed(SchemaSyncError::Backend(BackendError::ExecutionError(error))))
                if error.code == "23503"
        ),
        "cutover did not reject the orphan with a foreign key error: {cutover:?}"
    );
    Ok(())
}
