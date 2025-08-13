use crate::{
    frontend::{
        client::query_engine::{begin::Begin, test::Stream},
        Stats,
    },
    net::Protocol,
};

#[tokio::test]
async fn test_begin() {
    for in_transaction in [true, false] {
        let mut stats = Stats::new();
        let initial_bytes_sent = stats.bytes_sent;

        let mut begin = Begin::new(in_transaction, &mut stats);
        let mut stream = Stream::default();
        begin.handle(&mut stream).await.unwrap();

        assert_eq!(stream.messages.len(), 2);
        assert_eq!(stream.messages[0].code(), 'C');
        assert_eq!(stream.messages[1].code(), 'Z');
        assert_eq!(stream.messages[1].in_transaction(), true);
        assert!(stream.flushed);

        // Verify stats were updated with bytes sent
        let expected_bytes: usize = stream.messages.iter().map(|m| m.len()).sum();
        assert_eq!(stats.bytes_sent, initial_bytes_sent + expected_bytes);
        assert!(expected_bytes > 0, "should have sent some bytes");
    }
}
