use bytes::{BufMut, Bytes, BytesMut};
use sqlx::Executor;
use std::time::Duration;
use tokio::{io::AsyncWriteExt, net::TcpStream, time::timeout};

use crate::{
    setup::admin_sqlx,
    utils::{Message, startup},
};

async fn ready_messages(stream: &mut TcpStream) -> Vec<Message> {
    timeout(Duration::from_secs(10), async {
        let mut messages = Vec::new();
        loop {
            let message = Message::read(stream).await.expect("protocol response");
            let done = message.code == 'Z';
            messages.push(message);
            if done {
                return messages;
            }
        }
    })
    .await
    .expect("ReadyForQuery timeout")
}

async fn simple_query(stream: &mut TcpStream, sql: &str) -> Vec<Message> {
    Message {
        code: 'Q',
        payload: Bytes::from(format!("{sql}\0")),
    }
    .send(stream)
    .await
    .expect("simple query");
    ready_messages(stream).await
}

async fn parse_and_sync(stream: &mut TcpStream, name: &str, sql: &str) -> Vec<Message> {
    Message::new_parse(name, sql)
        .send(stream)
        .await
        .expect("Parse");
    Message {
        code: 'S',
        payload: Bytes::new(),
    }
    .send(stream)
    .await
    .expect("Sync");
    ready_messages(stream).await
}

async fn execute_statement(stream: &mut TcpStream, name: &str) -> Vec<Message> {
    let mut bind = BytesMut::new();
    bind.put_u8(0);
    bind.put_slice(name.as_bytes());
    bind.put_u8(0);
    for _ in 0..3 {
        bind.put_i16(0);
    }
    Message {
        code: 'B',
        payload: bind.freeze(),
    }
    .send(stream)
    .await
    .expect("Bind");
    Message {
        code: 'E',
        payload: Bytes::from_static(&[0, 0, 0, 0, 0]),
    }
    .send(stream)
    .await
    .expect("Execute");
    Message {
        code: 'S',
        payload: Bytes::new(),
    }
    .send(stream)
    .await
    .expect("Sync");
    ready_messages(stream).await
}

#[tokio::test]
async fn cached_parse_rejects_aborted_transaction_in_both_pooling_modes() {
    let admin = admin_sqlx().await;
    admin
        .execute("SET auth_type TO 'trust'")
        .await
        .expect("test authentication");

    for user in ["pgdog", "pgdog_session"] {
        let mut stream = TcpStream::connect("127.0.0.1:6432").await.expect("connect");
        stream
            .write_all(&startup(user, "pgdog"))
            .await
            .expect("startup");
        ready_messages(&mut stream).await;
        simple_query(&mut stream, "BEGIN").await;

        let warm = parse_and_sync(&mut stream, "warm_select", "SELECT 1").await;
        assert!(
            warm.iter().any(|m| m.code == '1'),
            "prepare before abort: {warm:?}"
        );

        let failed = simple_query(&mut stream, "SELECT 1/0").await;
        assert!(failed.iter().any(|m| m.code == 'E'));

        // A new client name with identical SQL resolves to the cached server statement.
        let responses = parse_and_sync(&mut stream, "aborted_select", "SELECT 1").await;
        let codes: Vec<_> = responses.iter().map(|m| m.code).collect();
        assert_eq!(
            codes,
            ['E', 'Z'],
            "cached Parse must fail in an aborted transaction ({user})"
        );
        assert!(
            responses[0]
                .payload
                .split(|byte| *byte == 0)
                .any(|field| field == b"C25P02")
        );
        assert_eq!(responses[1].payload.as_ref(), b"E");

        simple_query(&mut stream, "ROLLBACK").await;
        // The frontend's original statement remains usable after the backend reprepare failed.
        let recovered = execute_statement(&mut stream, "warm_select").await;
        assert!(
            !recovered.iter().any(|m| m.code == 'E'),
            "recovery: {recovered:?}"
        );
        assert!(recovered.iter().any(|m| m.code == 'D'));
        assert_eq!(
            recovered.last().expect("ReadyForQuery").payload.as_ref(),
            b"I"
        );
    }

    admin
        .execute("RELOAD")
        .await
        .expect("restore authentication");
}

#[tokio::test]
async fn cached_parse_allows_transaction_exit_after_abort() {
    let admin = admin_sqlx().await;
    admin
        .execute("SET auth_type TO 'trust'")
        .await
        .expect("test authentication");

    for user in ["pgdog", "pgdog_session"] {
        for sql in ["ROLLBACK", "COMMIT"] {
            let mut stream = TcpStream::connect("127.0.0.1:6432").await.expect("connect");
            stream
                .write_all(&startup(user, "pgdog"))
                .await
                .expect("startup");
            ready_messages(&mut stream).await;
            simple_query(&mut stream, "BEGIN").await;
            let warm = parse_and_sync(&mut stream, "warm_exit", sql).await;
            assert!(warm.iter().any(|message| message.code == '1'));
            assert_eq!(warm.last().expect("ReadyForQuery").payload.as_ref(), b"T");

            let failed = simple_query(&mut stream, "SELECT 1/0").await;
            assert!(failed.iter().any(|message| message.code == 'E'));
            assert_eq!(failed.last().expect("ReadyForQuery").payload.as_ref(), b"E");

            let parsed = parse_and_sync(&mut stream, "aborted_exit", sql).await;
            assert_eq!(
                parsed
                    .iter()
                    .map(|message| message.code)
                    .collect::<Vec<_>>(),
                ['1', 'Z'],
                "cached {sql} Parse remains valid after abort ({user})"
            );
            assert_eq!(parsed.last().expect("ReadyForQuery").payload.as_ref(), b"E");

            let executed = execute_statement(&mut stream, "aborted_exit").await;
            assert!(
                !executed.iter().any(|message| message.code == 'E'),
                "{executed:?}"
            );
            assert!(executed.iter().any(|message| message.code == 'C'));
            assert_eq!(
                executed.last().expect("ReadyForQuery").payload.as_ref(),
                b"I"
            );
        }
    }
    admin
        .execute("RELOAD")
        .await
        .expect("restore authentication");
}
