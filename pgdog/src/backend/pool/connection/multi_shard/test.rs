use crate::net::DataRow;

use super::*;

#[test]
fn test_rd_before_dr() {
    let mut multi_shard = MultiShard::new(3, &Route::read(None));
    for _ in 0..2 {
        let result = multi_shard
            .forward(RowDescription::new(&[]).message().unwrap())
            .unwrap();
        assert!(result.is_none()); // dropped
        let result = multi_shard
            .forward(DataRow::new().message().unwrap())
            .unwrap();
        assert!(result.is_none()); // buffered.
    }
}
