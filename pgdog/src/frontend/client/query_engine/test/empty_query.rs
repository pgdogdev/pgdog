use crate::{
    frontend::{
        client::query_engine::{empty_query::EmptyQuery, test::Stream},
        Stats,
    },
    net::Protocol,
};

#[tokio::test]
async fn test_empty_query() {
    for in_transaction in [true, false] {
        let mut stats = Stats::new();
        let initial_bytes = stats.bytes_sent;

        let mut empty_query = EmptyQuery::new(in_transaction, &mut stats);
        let mut stream = Stream::default();
        empty_query.handle(&mut stream).await.unwrap();

        assert_eq!(stream.messages.len(), 2);
        assert_eq!(stream.messages[0].code(), 'I');
        assert_eq!(stream.messages[1].code(), 'Z');
        assert_eq!(stream.messages[1].in_transaction(), in_transaction);
        assert!(stream.flushed);

        let expected_bytes: usize = stream.messages.iter().map(|m| m.len()).sum();
        assert_eq!(stats.bytes_sent, initial_bytes + expected_bytes);
        assert!(expected_bytes > 0);
    }
}
