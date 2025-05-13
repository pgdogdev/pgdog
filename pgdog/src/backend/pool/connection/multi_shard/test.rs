use crate::net::{DataRow, Field};

use super::*;

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
