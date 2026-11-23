use super::*;

#[tokio::test]
async fn failed_table_copy_fails_copy_data_and_cancels_the_other_copies()
-> Result<(), Box<dyn std::error::Error>> {
    let schema = "copy_data_table_error";
    let destination = "copy_data_table_error_dest";
    let original_config = config();
    let mut admin = test_server().await;
    let result = async {
        setup_replication_test(&mut admin, schema, destination).await?;
        let mut parallel = (*config()).clone();
        parallel.config.general.resharding_parallel_copies = 2;
        set(parallel)?;
        databases::init()?;

        let (_server, mut dest_server) = create_tables(schema, destination, 200_000).await?;
        dest_server
            .execute_checked(format!("INSERT INTO {schema}.small VALUES (1)"))
            .await?;

        let state = ReshardingState::builder()
            .source(schema)
            .destination(destination)
            .publication(schema)
            .maybe_replication_slot(Some(schema.into()))
            .build()?;
        let task = run_task(
            CopyDataTask::builder()
                .state(state.clone())
                .format(config().config.general.resharding_copy_format)
                .build(),
        );
        let id = task.id();
        let outcome = tokio::time::timeout(Duration::from_secs(90), task).await?;
        tokio::time::timeout(Duration::from_secs(30), async {
            while !subtree_progress(id)
                .iter()
                .all(|(_, progress)| progress.is_terminal())
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        let states = subtree_progress(id);
        state.drop_slots_if_owned().await?;

        Ok::<_, Box<dyn std::error::Error>>((outcome, states))
    }
    .await;

    cleanup_replication_test(&mut admin, &original_config, [schema, destination]).await?;

    let (outcome, states) = result?;
    assert!(matches!(outcome, Err(TaskError::Failed(_))), "{outcome:?}");
    assert!(
        matches!(
            progress_of(&states, "copy_data").as_slice(),
            [TaskProgress::Error { .. }]
        ),
        "copy_data must report the failure of its table copy: {states:?}"
    );
    let copies = progress_of(&states, "table_copy");
    assert_eq!(copies.len(), 2, "{states:?}");
    assert!(
        copies
            .iter()
            .any(|progress| matches!(progress, TaskProgress::Error { .. })),
        "{states:?}"
    );
    assert!(copies.contains(&TaskProgress::Cancelled), "{states:?}");

    Ok(())
}
