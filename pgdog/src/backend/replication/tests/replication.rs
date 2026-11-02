use super::*;
use crate::api::replication::ReplicationTask;
use pgdog_stats::{ReshardingDirection, SyncState};

#[tokio::test]
async fn stopped_replication_reports_every_stream_as_cancelled()
-> Result<(), Box<dyn std::error::Error>> {
    let schema = "replication_task_stop";
    let destination = "replication_task_stop_dest";
    let original_config = config();
    let mut admin = test_server().await;
    let result = async {
        setup_replication_test(&mut admin, schema, destination).await?;
        let (mut server, _dest_server) = create_tables(schema, destination, 10).await?;
        let state = ReshardingState::builder()
            .source(schema)
            .destination(destination)
            .publication(schema)
            .maybe_replication_slot(Some(schema.into()))
            .build()?;
        run_task(
            CopyDataTask::builder()
                .state(state.clone())
                .format(config().config.general.resharding_copy_format)
                .build(),
        )
        .await?;

        let task = run_task(
            ReplicationTask::builder()
                .state(state.clone())
                .schema_sync(
                    SchemaSyncTask::builder()
                        .databases(state.databases())
                        .publication(schema.to_owned()),
                )
                .validate(false)
                .build(),
        );
        let id = task.id();
        wait_for_active_source_slot(&mut server, schema).await?;

        tasks_storage().cancel_task(id);
        let outcome = tokio::time::timeout(Duration::from_secs(90), task).await?;
        let states = subtree_progress(id);
        let leftover = source_slots(&mut server, schema).await?;

        Ok::<_, Box<dyn std::error::Error>>((outcome, states, leftover))
    }
    .await;

    cleanup_replication_test(&mut admin, &original_config, [schema, destination]).await?;

    let (outcome, states, leftover) = result?;
    assert!(
        matches!(outcome, Err(TaskError::Failed(Error::ReplicationAborted))),
        "{outcome:?}"
    );
    assert_eq!(leftover, Vec::<String>::new());
    for name in ["replication", "replication_cluster", "replication_shard"] {
        let progress = progress_of(&states, name);
        assert!(
            !progress.is_empty() && progress.iter().all(|p| *p == TaskProgress::Cancelled),
            "every stopped {name} task must report as cancelled: {states:?}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn cutover_and_rollback_swap_the_direction() -> Result<(), Box<dyn std::error::Error>> {
    let schema = "replication_direction";
    let destination = "replication_direction_dest";
    let original_config = config();
    let mut admin = test_server().await;
    let result = async {
        setup_replication_test(&mut admin, schema, destination).await?;
        let (_server, mut dest_server) = create_tables(schema, destination, 10).await?;
        dest_server
            .execute_checked(format!(
                "CREATE PUBLICATION {schema} FOR TABLE {schema}.main, {schema}.small"
            ))
            .await?;
        let state = ReshardingState::builder()
            .source(schema)
            .destination(destination)
            .publication(schema)
            .maybe_replication_slot(Some(schema.into()))
            .build()?;
        run_task(
            CopyDataTask::builder()
                .state(state.clone())
                .format(config().config.general.resharding_copy_format)
                .build(),
        )
        .await?;

        let task = run_task(
            ReplicationTask::builder()
                .state(state.clone())
                .schema_sync(
                    SchemaSyncTask::builder()
                        .databases(state.databases())
                        .publication(schema.to_owned()),
                )
                .validate(false)
                .auto_cutover(true)
                .build(),
        );
        let id = task.id();

        let rolled_back = async {
            wait_for_replication_status(id, ReplicationStatus::ReverseReplicating).await?;
            if !ReplicationTask::trigger_cutover(Some(id)) {
                return Err("the rollback cutover was not triggered".into());
            }
            wait_for_replication_status(id, ReplicationStatus::Replicating).await?;
            Ok::<_, Box<dyn std::error::Error>>(())
        }
        .await;

        tasks_storage().cancel_task(id);
        let outcome = tokio::time::timeout(Duration::from_secs(90), task).await?;
        rolled_back?;
        Ok::<_, Box<dyn std::error::Error>>((subtree_definitions(id), outcome))
    }
    .await;

    cleanup_replication_test(&mut admin, &original_config, [schema, destination]).await?;

    let (definitions, outcome) = result?;
    let mut replication: Vec<_> = definitions
        .iter()
        .filter_map(|definition| match definition {
            TaskDefinitionKind::ReplicationCluster(cluster) => Some(cluster.direction),
            _ => None,
        })
        .collect();
    replication.dedup();
    let schema_sync: Vec<_> = definitions
        .iter()
        .filter_map(|definition| match definition {
            TaskDefinitionKind::SchemaSync(sync) => Some((sync.sync_state, sync.direction)),
            _ => None,
        })
        .collect();

    assert_eq!(
        replication,
        [
            ReshardingDirection::Forward,
            ReshardingDirection::Reverse,
            ReshardingDirection::Forward,
        ]
    );
    assert_eq!(
        schema_sync,
        [
            (SyncState::Cutover, ReshardingDirection::Forward),
            (SyncState::Cutover, ReshardingDirection::Reverse),
        ]
    );
    assert!(
        matches!(outcome, Err(TaskError::Failed(Error::ReplicationAborted))),
        "{outcome:?}"
    );
    Ok(())
}
