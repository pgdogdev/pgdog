use crate::{
    frontend::router::parser::{Limit, OrderBy, Shard, ShardWithPriority},
    net::{DataRow, Field, Format},
};

use super::*;

#[test]
fn test_inconsistent_row_descriptions() {
    let route = Route::default();
    let mut multi_shard = MultiShard::new(vec![0, 1], &route);

    // Create two different row descriptions
    let rd1 = RowDescription::new(&[Field::text("name"), Field::bigint("id")]);
    let rd2 = RowDescription::new(&[Field::text("name")]); // Missing column

    // First row description should be processed successfully
    let result = multi_shard.forward(rd1.message().unwrap()).unwrap();
    assert!(result.is_none()); // Not forwarded until all shards respond

    // Second inconsistent row description should cause an error
    let result = multi_shard.forward(rd2.message().unwrap());
    assert!(result.is_err());

    if let Err(error) = result {
        let error_str = format!("{}", error);
        assert!(error_str.contains("inconsistent row descriptions"));
        assert!(error_str.contains("expected 2 columns, got 1 columns"));
    }
}

#[test]
fn test_inconsistent_data_rows() {
    let route = Route::default();
    let mut multi_shard = MultiShard::new(vec![0, 1], &route);

    // Set up row description first
    let rd = RowDescription::new(&[Field::text("name"), Field::bigint("id")]);
    multi_shard.forward(rd.message().unwrap()).unwrap();

    // Create data rows with different column counts
    let mut dr1 = DataRow::new();
    dr1.add("test").add(123_i64);

    let mut dr2 = DataRow::new();
    dr2.add("only_name"); // Missing id column

    // First data row should be processed successfully
    let result = multi_shard.forward(dr1.message().unwrap()).unwrap();
    assert!(result.is_none()); // Buffered, not forwarded immediately

    // Second inconsistent data row should cause an error
    let result = multi_shard.forward(dr2.message().unwrap());
    assert!(result.is_err());

    if let Err(error) = result {
        let error_str = format!("{}", error);
        assert!(error_str.contains("inconsistent column count in data rows"));
        assert!(error_str.contains("expected 2 columns, got 1 columns"));
    }
}

#[test]
fn test_rd_before_dr() {
    let mut multi_shard = MultiShard::new(
        vec![0, 1, 2],
        &Route::read(ShardWithPriority::new_default_unset(Shard::All)),
    );
    let rd = RowDescription::new(&[Field::bigint("id")]);
    let mut dr = DataRow::new();
    dr.add(1i64);
    for _ in 0..2 {
        let result = multi_shard
            .forward(rd.message().unwrap().backend(BackendPid::for_test(1)))
            .unwrap();
        assert!(result.is_none()); // dropped
        let result = multi_shard
            .forward(dr.message().unwrap().backend(BackendPid::for_test(1)))
            .unwrap();
        assert!(result.is_none()); // buffered.
    }

    let result = multi_shard.forward(rd.message().unwrap()).unwrap();
    assert_eq!(result, Some(rd.message().unwrap()));
    let result = multi_shard.message();
    // Waiting for command complete
    assert!(result.is_none());

    for _ in 0..3 {
        let result = multi_shard
            .forward(
                CommandComplete::from_str("SELECT 1")
                    .message()
                    .unwrap()
                    .backend(BackendPid::for_test(1)),
            )
            .unwrap();
        assert!(result.is_none());
    }

    for _ in 0..2 {
        let result = multi_shard.message();
        let id = BackendPid::for_test(1);
        assert_eq!(
            result.map(|m| m.backend(id)),
            Some(dr.message().unwrap().backend(id))
        );
    }

    let result = multi_shard
        .message()
        .map(|m| m.backend(BackendPid::for_test(1)));
    assert_eq!(
        result,
        Some(
            CommandComplete::from_str("SELECT 3")
                .message()
                .unwrap()
                .backend(BackendPid::for_test(1))
        )
    );

    // Buffer is empty.
    assert!(multi_shard.message().is_none());
}

#[test]
fn test_ready_for_query_error_preservation() {
    let route = Route::default();
    let mut multi_shard = MultiShard::new(vec![0, 1], &route);

    // Create ReadyForQuery messages - one with transaction error, one normal
    let rfq_error = ReadyForQuery::error();
    let rfq_normal = ReadyForQuery::in_transaction(false);

    // Forward first ReadyForQuery message with error state
    let result = multi_shard.forward(rfq_error.message().unwrap()).unwrap();
    assert!(result.is_none()); // Should not be forwarded yet (waiting for second shard)

    // Forward second normal ReadyForQuery message
    let result = multi_shard.forward(rfq_normal.message().unwrap()).unwrap();

    // Should return the error message, not the normal one
    assert!(result.is_some());
    let returned_message = result.unwrap();
    let returned_rfq = ReadyForQuery::from_bytes(returned_message.to_bytes()).unwrap();
    assert!(returned_rfq.is_transaction_aborted());
}

#[test]
fn test_omni_command_complete_not_summed() {
    // For omni-sharded tables, we should NOT sum row counts across shards.
    let route = Route::write(ShardWithPriority::new_table_omni(Shard::All)).with_omnisharded(true);
    let mut multi_shard = MultiShard::new(vec![0, 1, 2], &route);

    let backend1 = BackendPid::for_test(1);
    let backend2 = BackendPid::for_test(2);
    let backend3 = BackendPid::for_test(3);

    // All shards report UPDATE 5
    multi_shard
        .forward(
            CommandComplete::from_str("UPDATE 5")
                .message()
                .unwrap()
                .backend(backend1),
        )
        .unwrap();
    multi_shard
        .forward(
            CommandComplete::from_str("UPDATE 5")
                .message()
                .unwrap()
                .backend(backend2),
        )
        .unwrap();
    multi_shard
        .forward(
            CommandComplete::from_str("UPDATE 5")
                .message()
                .unwrap()
                .backend(backend3),
        )
        .unwrap();

    let result = multi_shard.message();
    let cc = CommandComplete::from_bytes(result.unwrap().to_bytes()).unwrap();
    // Should be 5 (from one shard), not 15 (sum of all shards)
    assert_eq!(cc.rows().unwrap(), Some(5));
}

#[test]
fn test_omni_command_complete_uses_first_shard_row_count() {
    // For omni, we use the first shard's row count for consistency with DataRow behavior.
    let route = Route::write(ShardWithPriority::new_table_omni(Shard::All)).with_omnisharded(true);
    let mut multi_shard = MultiShard::new(vec![0, 1], &route);

    let backend1 = BackendPid::for_test(1);
    let backend2 = BackendPid::for_test(2);

    // First shard reports 7 rows
    multi_shard
        .forward(
            CommandComplete::from_str("UPDATE 7")
                .message()
                .unwrap()
                .backend(backend1),
        )
        .unwrap();

    // Second shard reports 9 rows (different, to distinguish first vs last)
    multi_shard
        .forward(
            CommandComplete::from_str("UPDATE 9")
                .message()
                .unwrap()
                .backend(backend2),
        )
        .unwrap();

    let result = multi_shard.message();
    let cc = CommandComplete::from_bytes(result.unwrap().to_bytes()).unwrap();
    // Should be 7 (from FIRST shard), not 9 (from last)
    assert_eq!(cc.rows().unwrap(), Some(7));
}

#[test]
fn test_omni_data_rows_only_from_first_server() {
    // For omni-sharded tables with RETURNING, only forward DataRows from the first server.
    let route = Route::write(ShardWithPriority::new_table_omni(Shard::All)).with_omnisharded(true);
    let mut multi_shard = MultiShard::new(vec![0, 1], &route);

    let backend1 = BackendPid::for_test(1);
    let backend2 = BackendPid::for_test(2);

    // Setup: send RowDescription from both shards
    let rd = RowDescription::new(&[Field::bigint("id")]);
    multi_shard
        .forward(rd.message().unwrap().backend(backend1))
        .unwrap();
    let rd_result = multi_shard
        .forward(rd.message().unwrap().backend(backend2))
        .unwrap();
    assert!(rd_result.is_some()); // RowDescription forwarded after all shards

    // DataRow from first shard (backend1) - should be forwarded
    let mut dr1 = DataRow::new();
    dr1.add(100_i64);
    let result = multi_shard
        .forward(dr1.message().unwrap().backend(backend1))
        .unwrap();
    assert!(result.is_some()); // Should be forwarded

    // DataRow from second shard (backend2) - should NOT be forwarded
    let mut dr2 = DataRow::new();
    dr2.add(200_i64);
    let result = multi_shard
        .forward(dr2.message().unwrap().backend(backend2))
        .unwrap();
    assert!(result.is_none()); // Should be dropped

    // Another DataRow from first shard - should be forwarded
    let mut dr3 = DataRow::new();
    dr3.add(101_i64);
    let result = multi_shard
        .forward(dr3.message().unwrap().backend(backend1))
        .unwrap();
    assert!(result.is_some()); // Should be forwarded
}

#[test]
fn test_order_by_k_way_merge_streams_before_command_complete() {
    let route = Route::select(
        ShardWithPriority::new_table(Shard::All),
        vec![OrderBy::Asc(1)],
        Default::default(),
        Limit::default(),
        None,
    );
    let mut multi_shard = MultiShard::new(vec![0, 1], &route);

    let rd = RowDescription::new(&[Field::bigint("id")]);
    let backend1 = BackendPid::for_test(1);
    let backend2 = BackendPid::for_test(2);

    let rd_result = multi_shard
        .forward_from(0, rd.message().unwrap().backend(backend1))
        .unwrap();
    assert!(rd_result.is_none());
    let rd_result = multi_shard
        .forward_from(1, rd.message().unwrap().backend(backend2))
        .unwrap();
    assert!(rd_result.is_some());

    let mut shard0_first = DataRow::new();
    shard0_first.add(1_i64);
    multi_shard
        .forward_from(0, shard0_first.message().unwrap().backend(backend1))
        .unwrap();
    assert!(multi_shard.message().is_none());

    let mut shard1_first = DataRow::new();
    shard1_first.add(2_i64);
    multi_shard
        .forward_from(1, shard1_first.message().unwrap().backend(backend2))
        .unwrap();
    let next = DataRow::from_bytes(multi_shard.message().unwrap().to_bytes()).unwrap();
    assert_eq!(next.get::<i64>(0, Format::Text).unwrap(), 1_i64);

    let mut shard0_second = DataRow::new();
    shard0_second.add(3_i64);
    multi_shard
        .forward_from(0, shard0_second.message().unwrap().backend(backend1))
        .unwrap();
    let next = DataRow::from_bytes(multi_shard.message().unwrap().to_bytes()).unwrap();
    assert_eq!(next.get::<i64>(0, Format::Text).unwrap(), 2_i64);

    multi_shard
        .forward_from(
            1,
            CommandComplete::from_str("SELECT 1")
                .message()
                .unwrap()
                .backend(backend2),
        )
        .unwrap();
    let next = DataRow::from_bytes(multi_shard.message().unwrap().to_bytes()).unwrap();
    assert_eq!(next.get::<i64>(0, Format::Text).unwrap(), 3_i64);

    multi_shard
        .forward_from(
            0,
            CommandComplete::from_str("SELECT 2")
                .message()
                .unwrap()
                .backend(backend1),
        )
        .unwrap();

    let cc = CommandComplete::from_bytes(multi_shard.message().unwrap().to_bytes()).unwrap();
    assert_eq!(cc.rows().unwrap(), Some(3));
}
