use crate::net::{DataRow, Field};

use super::*;

#[test]
fn test_inconsistent_row_descriptions() {
    let route = Route::default();
    let mut multi_shard = MultiShard::new(2, &route);

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
        let error_str = format!("{error}");
        assert!(error_str.contains("inconsistent row descriptions"));
        assert!(error_str.contains("expected 2 columns, got 1 columns"));
    }
}

#[test]
fn test_inconsistent_data_rows() {
    let route = Route::default();
    let mut multi_shard = MultiShard::new(2, &route);

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
        let error_str = format!("{error}");
        assert!(error_str.contains("inconsistent column count in data rows"));
        assert!(error_str.contains("expected 2 columns, got 1 columns"));
    }
}

#[test]
fn test_rd_before_dr() {
    let mut multi_shard = MultiShard::new(3, &Route::read(None));
    let rd = RowDescription::new(&[Field::bigint("id")]);
    let mut dr = DataRow::new();
    dr.add(1i64);
    for _ in 0..2 {
        let result = multi_shard
            .forward(rd.message().unwrap().backend())
            .unwrap();
        assert!(result.is_none()); // dropped
        let result = multi_shard
            .forward(dr.message().unwrap().backend())
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
                    .backend(),
            )
            .unwrap();
        assert!(result.is_none());
    }

    for _ in 0..2 {
        let result = multi_shard.message();
        assert_eq!(
            result.map(|m| m.backend()),
            Some(dr.message().unwrap().backend())
        );
    }

    let result = multi_shard.message().map(|m| m.backend());
    assert_eq!(
        result,
        Some(
            CommandComplete::from_str("SELECT 3")
                .message()
                .unwrap()
                .backend()
        )
    );

    // Buffer is empty.
    assert!(multi_shard.message().is_none());
}
