use bytes::{Buf, Bytes};
use rust::{
    setup::admin_sqlx,
    utils::{Message, startup_with_version},
};
use serial_test::serial;
use sqlx::Executor;
use tokio::{io::AsyncWriteExt, net::TcpStream};

const PROTOCOL_3_2: i32 = 196610;
const PROTOCOL_3_3: i32 = 196611;

async fn startup_exchange(protocol_version: i32, extra_params: &[(&str, &str)]) -> Vec<Message> {
    let mut stream = TcpStream::connect("127.0.0.1:6432").await.unwrap();
    stream
        .write_all(&startup_with_version(
            "pgdog",
            "pgdog",
            protocol_version,
            extra_params,
        ))
        .await
        .unwrap();

    let mut messages = vec![];
    loop {
        let message = Message::read(&mut stream).await.unwrap();
        let done = message.code == 'Z';
        messages.push(message);
        if done {
            break;
        }
    }

    messages
}

fn backend_key_secret_len(messages: &[Message]) -> usize {
    let key = messages
        .iter()
        .find(|message| message.code == 'K')
        .expect("BackendKeyData should be present");
    key.payload.len() - 4
}

fn parse_c_string(payload: &mut Bytes) -> String {
    let nul = payload
        .iter()
        .position(|byte| *byte == 0)
        .expect("NUL-terminated string");
    let value = String::from_utf8(payload.split_to(nul).to_vec()).expect("valid utf-8");
    payload.advance(1);
    value
}

fn parse_negotiation(message: &Message) -> (i32, Vec<String>) {
    assert_eq!(message.code, 'v');

    let mut payload = message.payload.clone();
    let version = payload.get_i32();
    let count = payload.get_i32();
    let mut options = Vec::with_capacity(count as usize);
    for _ in 0..count {
        options.push(parse_c_string(&mut payload));
    }

    (version, options)
}

#[tokio::test]
#[serial]
async fn test_protocol_3_2_uses_extended_backend_key_data() {
    let admin = admin_sqlx().await;
    admin.execute("RELOAD").await.unwrap();
    admin.execute("SET auth_type TO 'trust'").await.unwrap();

    let join = tokio::spawn(async { startup_exchange(PROTOCOL_3_2, &[]).await }).await;

    admin.execute("RELOAD").await.unwrap();

    let messages = join.unwrap();
    assert!(
        !messages.iter().any(|message| message.code == 'v'),
        "3.2 should not need a downgrade negotiation",
    );
    assert!(
        backend_key_secret_len(&messages) > 4,
        "3.2 should return an extended cancel secret",
    );
}

#[tokio::test]
#[serial]
async fn test_protocol_3_3_negotiates_down_to_3_2() {
    let admin = admin_sqlx().await;
    admin.execute("RELOAD").await.unwrap();
    admin.execute("SET auth_type TO 'trust'").await.unwrap();

    let join = tokio::spawn(async { startup_exchange(PROTOCOL_3_3, &[]).await }).await;

    admin.execute("RELOAD").await.unwrap();

    let messages = join.unwrap();
    let negotiation = messages
        .first()
        .expect("startup exchange should return at least one message");
    let (version, options) = parse_negotiation(negotiation);

    assert_eq!(version, PROTOCOL_3_2);
    assert!(options.is_empty());
    assert!(
        backend_key_secret_len(&messages) > 4,
        "after negotiating to 3.2, the cancel secret should still be extended",
    );
}

#[tokio::test]
#[serial]
async fn test_protocol_options_are_reported_via_negotiation() {
    let admin = admin_sqlx().await;
    admin.execute("RELOAD").await.unwrap();
    admin.execute("SET auth_type TO 'trust'").await.unwrap();

    let join =
        tokio::spawn(async { startup_exchange(PROTOCOL_3_2, &[("_pq_.command_tag", "on")]).await })
            .await;

    admin.execute("RELOAD").await.unwrap();

    let messages = join.unwrap();
    let negotiation = messages
        .first()
        .expect("startup exchange should return at least one message");
    let (version, options) = parse_negotiation(negotiation);

    assert_eq!(version, PROTOCOL_3_2);
    assert_eq!(options, vec!["_pq_.command_tag"]);
    assert!(
        backend_key_secret_len(&messages) > 4,
        "3.2 protocol startup should still receive an extended cancel secret",
    );
}
