use crate::{
    backend::pool::Connection,
    frontend::{
        client::query_engine::{rollback::Rollback, test::Stream},
        Stats,
    },
    net::Protocol,
};

#[tokio::test]
async fn test_rollback() {
    for in_transaction in [true, false] {
        let mut stats = Stats::new();
        let initial_bytes = stats.bytes_sent;
        let mut backend = Connection::default();

        let mut rollback = Rollback::new(in_transaction, &mut backend, &mut stats);
        let mut stream = Stream::default();
        rollback.handle(&mut stream).await.unwrap();

        if in_transaction {
            assert_eq!(stream.messages.len(), 2);
            assert_eq!(stream.messages[0].code(), 'C');
            assert_eq!(stream.messages[1].code(), 'Z');
            assert_eq!(stream.messages[1].in_transaction(), false);
        } else {
            assert_eq!(stream.messages.len(), 3);
            assert_eq!(stream.messages[0].code(), 'N');
            assert_eq!(stream.messages[1].code(), 'C');
            assert_eq!(stream.messages[2].code(), 'Z');
            assert_eq!(stream.messages[2].in_transaction(), false);
        }

        assert!(stream.flushed);

        let expected_bytes: usize = stream.messages.iter().map(|m| m.len()).sum();
        assert_eq!(stats.bytes_sent, initial_bytes + expected_bytes);
        assert!(expected_bytes > 0);
    }
}
