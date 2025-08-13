use crate::{
    frontend::client::query_engine::{begin::Begin, test::Stream},
    net::Protocol,
};

#[tokio::test]
async fn test_begin() {
    for in_transaction in [true, false] {
        let mut begin = Begin::new(in_transaction);
        let mut stream = Stream::default();
        begin.handle(&mut stream).await.unwrap();

        assert_eq!(stream.messages.len(), 2);
        assert_eq!(stream.messages[0].code(), 'C');
        assert_eq!(stream.messages[1].code(), 'Z');
        assert_eq!(stream.messages[1].in_transaction(), true);
        assert!(stream.flushed);
    }
}
