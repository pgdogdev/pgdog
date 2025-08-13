use crate::{
    frontend::{
        client::query_engine::{cross_shard_check::CrossShardCheck, test::Stream},
        router::{parser::route::Route, parser::route::Shard},
        Stats,
    },
    net::Protocol,
};

#[tokio::test]
async fn test_cross_shard_check() {
    let test_cases = [
        // (disabled, shard, in_transaction, should_block)
        (true, Shard::All, false, true),
        (true, Shard::All, true, true),
        (true, Shard::Multi(vec![0, 1]), false, true),
        (true, Shard::Multi(vec![0, 1]), true, true),
        (true, Shard::Direct(0), false, false),
        (true, Shard::Direct(0), true, false),
        (false, Shard::All, false, false),
        (false, Shard::All, true, false),
        (false, Shard::Multi(vec![0, 1]), false, false),
        (false, Shard::Multi(vec![0, 1]), true, false),
        (false, Shard::Direct(0), false, false),
        (false, Shard::Direct(0), true, false),
    ];

    for (disabled, shard, in_transaction, should_block) in test_cases {
        for is_read in [true, false] {
            let mut stats = Stats::new();
            let initial_bytes = stats.bytes_sent;

            let route = if is_read {
                Route::read(shard.clone())
            } else {
                Route::write(shard.clone())
            };

            let mut cross_shard_check =
                CrossShardCheck::new(disabled, &route, in_transaction, &mut stats);
            let mut stream = Stream::default();

            let result = cross_shard_check.handle(&mut stream).await.unwrap();

            if should_block {
                assert!(
                    result,
                    "expected blocking for disabled={}, shard={:?}, in_transaction={}, is_read={}",
                    disabled, shard, in_transaction, is_read
                );
                assert_eq!(stream.messages.len(), 2);
                assert_eq!(stream.messages[0].code(), 'E');
                assert_eq!(stream.messages[1].code(), 'Z');
                assert_eq!(stream.messages[1].in_transaction(), in_transaction);
                assert!(stream.flushed);

                let expected_bytes: usize = stream.messages.iter().map(|m| m.len()).sum();
                assert_eq!(stats.bytes_sent, expected_bytes);
                assert!(expected_bytes > 0);
            } else {
                assert!(!result, "expected no blocking for disabled={}, shard={:?}, in_transaction={}, is_read={}", disabled, shard, in_transaction, is_read);
                assert_eq!(stream.messages.len(), 0);
                assert!(!stream.flushed);
                assert_eq!(stats.bytes_sent, initial_bytes);
            }
        }
    }
}
