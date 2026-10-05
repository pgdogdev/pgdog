use std::panic::AssertUnwindSafe;

use futures::FutureExt;
use pgdog_config::resharding::PostDataValidationStage;
use pgdog_stats::{ReplicationStatus, SyncState, TaskDefinitionKind, TaskProgress};

use super::*;
use crate::api::{replication::ReplicationTask, schema_sync::SchemaSyncBuilder};
use crate::backend::maintenance_mode;

type TestResult = Result<(), Box<dyn std::error::Error>>;

struct FkValidation {
    schema: String,
    destination: String,
    state: ReshardingState,
    schema_sync: SchemaSyncBuilder,
    dest: Cluster,
}

impl FkValidation {
    fn start(&self, auto_cutover: bool) -> TaskWaiter<(), Error> {
        run_task(
            ReplicationTask::builder()
                .state(self.state.clone())
                .schema_sync(self.schema_sync.clone())
                .validate(true)
                .auto_cutover(auto_cutover)
                .build(),
        )
    }
}

async fn stop(
    task: TaskWaiter<(), Error>,
) -> Result<Result<(), TaskError<Error>>, Box<dyn std::error::Error>> {
    tasks_storage().cancel_task(task.id());
    Ok(tokio::time::timeout(Duration::from_secs(60), task).await?)
}

async fn with_fk_validation(
    schema: &str,
    stage: PostDataValidationStage,
    orphan: bool,
    test: impl AsyncFnOnce(&FkValidation) -> TestResult,
) -> TestResult {
    let destination = format!("{schema}_dest");
    let original_config = config();
    let mut admin = test_server().await;
    let result = AssertUnwindSafe(async {
        let fixture = fk_validation_setup(&mut admin, schema, &destination, stage, orphan).await?;
        test(&fixture).await
    })
    .catch_unwind()
    .await;
    cleanup_replication_test(&mut admin, &original_config, [schema, &destination]).await?;
    result.unwrap_or_else(|panic| std::panic::resume_unwind(panic))
}

async fn fk_validation_setup(
    admin: &mut Server,
    schema: &str,
    destination: &str,
    stage: PostDataValidationStage,
    orphan: bool,
) -> Result<FkValidation, Box<dyn std::error::Error>> {
    setup_replication_test(admin, schema, destination).await?;
    let mut test_config = (*config()).clone();
    test_config.config.resharding.post_data_validation = stage;
    set(test_config)?;

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

    let schema_sync = SchemaSyncTask::builder()
        .databases(pgdog_stats::Databases {
            source: schema.into(),
            destination: destination.into(),
        })
        .publication(schema.to_owned());
    run_task(schema_sync.clone().phase(SchemaSyncPhase::Pre).build()).await?;
    let cancel = CancellationToken::new();
    let state = ReshardingState::builder()
        .source(schema)
        .destination(destination)
        .publication(schema)
        .maybe_replication_slot(Some(schema.into()))
        .build()?;
    let dest = databases::databases().schema_owner(destination)?;
    state.sync_tables().await?;
    state.create_slots(&cancel).await?;
    let tables = state.tables();
    let tables = tables.get(&0).ok_or(Error::MissingData)?;
    let find = |name: &str| {
        tables
            .iter()
            .find(|table| table.table.name == name)
            .cloned()
            .ok_or(Error::MissingData)
    };
    let (child, parent) = (find("children")?, find("parents")?);

    let child = copy_table(&source, &dest, &child, server.addr()).await?;
    if orphan {
        let mut destination_server = dest.primary(0, &Request::default()).await?;
        destination_server
            .execute_checked(format!("INSERT INTO {schema}.children VALUES (42, 1, 999)"))
            .await?;
    }
    let parent = copy_table(&source, &dest, &parent, server.addr()).await?;
    state.set_tables([(0, vec![child, parent])].into());

    server
        .execute_checked(format!(
            "BEGIN; \
             INSERT INTO {schema}.parents VALUES (2, 1); \
             INSERT INTO {schema}.children VALUES (2, 1, 2); \
             COMMIT"
        ))
        .await?;
    drop(server);

    replicate_until_caught_up(&state, schema).await?;
    run_task(schema_sync.clone().phase(SchemaSyncPhase::Post).build()).await?;

    Ok(FkValidation {
        schema: schema.into(),
        destination: destination.into(),
        state,
        schema_sync,
        dest,
    })
}

fn replication_status(id: TaskId) -> Option<ReplicationStatus> {
    let mut found = None;
    tasks_storage().try_for_each(|task| {
        if task.id != id {
            return ControlFlow::Continue(());
        }
        if let TaskStatus::Replication(status) = task.state().status {
            found = Some(status);
        }
        ControlFlow::Break(())
    });
    found
}

async fn wait_for_replication_status(
    id: TaskId,
    expected: ReplicationStatus,
) -> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(Duration::from_secs(60), async {
        while replication_status(id) != Some(expected) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}

fn validation_tasks(root: TaskId) -> Vec<TaskProgress> {
    let mut found = Vec::new();
    tasks_storage().for_each(|task| {
        if task.root_id != root || task.id == root {
            return;
        }
        let state = task.state();
        if let TaskDefinitionKind::SchemaSync(definition) = &state.definition.kind
            && definition.sync_state == SyncState::PostDataValidation
        {
            found.push(state.progress);
        }
    });
    found
}

async fn wait_for_validation(
    root: TaskId,
    done: impl Fn(&[TaskProgress]) -> bool,
) -> Result<Vec<TaskProgress>, Box<dyn std::error::Error>> {
    let validations = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let validations = validation_tasks(root);
            if done(&validations) {
                return validations;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(validations)
}

async fn fk_validated(database: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let mut server = Server::connect(
        &Address {
            database_name: database.into(),
            ..Address::new_test()
        },
        ServerOptions::default(),
        ConnectReason::Resharding,
        Default::default(),
    )
    .await?;
    let validated: Vec<String> = server
        .fetch_all(
            "SELECT convalidated::text FROM pg_constraint WHERE conname = 'children_parent_fk'",
        )
        .await?;
    Ok(validated == ["true"])
}

fn is_fk_violation(result: &Result<(), TaskError<Error>>) -> bool {
    matches!(
        result,
        Err(TaskError::Failed(Error::SchemaSync(error)))
            if matches!(
                error.as_ref(),
                SchemaSyncError::Backend(BackendError::ExecutionError(error))
                    if error.code == "23503"
            )
    )
}

// Verify that we catch some data inconsistencies after resharding
// in case we created one. It's created artificially during copy,
// since we don't know for cases when we do this wrong for now.
#[tokio::test]
async fn test_replication_fk_inconsistent_validation() -> TestResult {
    with_fk_validation(
        "fk_post_copy_test",
        PostDataValidationStage::DuringReplication,
        true,
        async |fixture| {
            let outcome = fixture.start(false).await;
            assert!(
                is_fk_violation(&outcome),
                "validation did not reject the orphan with a foreign key error: {outcome:?}"
            );

            let schema = &fixture.schema;
            let mut server = fixture.dest.primary(0, &Request::default()).await?;
            let parents: Vec<i64> = server
                .fetch_all(format!("SELECT id FROM {schema}.parents ORDER BY id"))
                .await?;
            let children: Vec<i64> = server
                .fetch_all(format!(
                    "SELECT parent_id FROM {schema}.children ORDER BY id"
                ))
                .await?;
            assert_eq!(parents, [1, 2]);
            assert_eq!(children, [1, 2, 999]);
            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn post_data_validation_off_does_not_validate() -> TestResult {
    with_fk_validation(
        "fk_validation_off",
        PostDataValidationStage::Off,
        true,
        async |fixture| {
            let task = fixture.start(false);
            let id = task.id();
            wait_for_replication_status(id, ReplicationStatus::Replicating).await?;
            tokio::time::sleep(Duration::from_secs(2)).await;
            assert_eq!(validation_tasks(id), Vec::<TaskProgress>::new());

            let outcome = stop(task).await?;
            assert!(
                matches!(outcome, Err(TaskError::Failed(Error::ReplicationAborted))),
                "replication without validation must run until stopped: {outcome:?}"
            );
            assert!(
                !fk_validated(&fixture.destination).await?,
                "the foreign key must stay NOT VALID"
            );
            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn post_data_validation_before_cutover_stops_migration() -> TestResult {
    with_fk_validation(
        "fk_validation_before",
        PostDataValidationStage::BeforeCutover,
        true,
        async |fixture| {
            let task = fixture.start(true);
            let id = task.id();
            let outcome = tokio::time::timeout(Duration::from_secs(120), task).await?;
            assert!(
                is_fk_violation(&outcome),
                "validation before cutover must stop the migration: {outcome:?}"
            );
            assert_eq!(validation_tasks(id).len(), 1);
            assert!(
                maintenance_mode::waiter(&fixture.schema).is_none(),
                "traffic must resume after a failed validation"
            );
            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn post_data_validation_after_cutover_keeps_replicating() -> TestResult {
    with_fk_validation(
        "fk_validation_after",
        PostDataValidationStage::AfterCutover,
        true,
        async |fixture| {
            let task = fixture.start(true);
            let id = task.id();
            wait_for_replication_status(id, ReplicationStatus::ReverseReplicating).await?;
            let failed = wait_for_validation(id, |validations| {
                validations.iter().any(TaskProgress::is_terminal)
            })
            .await?;
            assert!(
                matches!(failed.as_slice(), [TaskProgress::Error { message }] if message.contains("children_parent_fk")),
                "validation after cutover must fail on the orphan: {failed:?}"
            );
            assert_eq!(
                replication_status(id),
                Some(ReplicationStatus::ReverseReplicating)
            );

            assert!(ReplicationTask::trigger_cutover(Some(id)));
            wait_for_replication_status(id, ReplicationStatus::Replicating).await?;
            tokio::time::sleep(Duration::from_secs(2)).await;
            let validations = validation_tasks(id);
            assert_eq!(
                validations.len(),
                1,
                "a rollback must not validate again: {validations:?}"
            );

            let outcome = stop(task).await?;
            assert!(
                matches!(outcome, Err(TaskError::Failed(Error::ReplicationAborted))),
                "a stop after rollback must abort the forward phase: {outcome:?}"
            );
            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn post_data_validation_during_replication_gates_cutover() -> TestResult {
    with_fk_validation(
        "fk_validation_gate",
        PostDataValidationStage::DuringReplication,
        false,
        async |fixture| {
            let mut lock = fixture.dest.primary(0, &Request::default()).await?;
            lock.execute_checked(format!(
                "BEGIN; LOCK TABLE {}.children IN SHARE UPDATE EXCLUSIVE MODE",
                fixture.schema
            ))
            .await?;

            let task = fixture.start(true);
            let id = task.id();
            wait_for_validation(id, |validations| !validations.is_empty()).await?;
            tokio::time::sleep(Duration::from_secs(3)).await;
            let validations = validation_tasks(id);
            assert!(
                validations.iter().all(|progress| !progress.is_terminal()),
                "validation must still wait for the lock: {validations:?}"
            );
            assert_eq!(replication_status(id), Some(ReplicationStatus::Replicating));

            lock.execute_checked("COMMIT").await?;
            drop(lock);
            wait_for_replication_status(id, ReplicationStatus::ReverseReplicating).await?;
            assert_eq!(validation_tasks(id), [TaskProgress::Finished]);

            let outcome = stop(task).await?;
            assert!(
                outcome.is_ok(),
                "a stop in the rollback window completes the migration: {outcome:?}"
            );
            assert!(
                fk_validated(&fixture.destination).await?,
                "the foreign key must be validated before cutover"
            );
            Ok(())
        },
    )
    .await
}
