use std::time::{Duration, Instant};

use pgdog_config::PoolerMode;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

use bytes::{BufMut, BytesMut};

use crate::{
    backend::databases::databases,
    config::{
        config, load_test, load_test_replicas, load_test_sharded, load_test_with_pooler_mode, set,
        PreparedStatements,
    },
    frontend::{
        client::{BufferEvent, QueryEngine},
        prepared_statements, Client,
    },
    net::{
        Bind, Close, CommandComplete, DataRow, Describe, ErrorResponse, Execute, Field, Flush,
        Format, FromBytes, Message, Parameters, Parse, Query, ReadyForQuery, RowDescription, Sync,
        Terminate, ToBytes,
    },
    state::State,
};

use super::Stream;

pub mod target_session_attrs;
pub mod test_client;
pub use test_client::{SpawnedClient, TestClient};

//
// cargo nextest runs these in separate processes.
// That's important otherwise I'm not sure what would happen.
//

pub async fn test_client(replicas: bool) -> (TcpStream, Client) {
    if replicas {
        load_test_replicas();
    } else {
        load_test();
    }

    parallel_test_client().await
}

/// Load test client with 4 databases (2 shards, 1 replica each).
pub async fn test_client_sharded() -> (TcpStream, Client) {
    load_test_sharded();

    parallel_test_client().await
}

pub async fn test_client_with_params(params: Parameters, replicas: bool) -> (TcpStream, Client) {
    if replicas {
        load_test_replicas();
    } else {
        load_test();
    }

    parallel_test_client_with_params(params).await
}

pub async fn parallel_test_client() -> (TcpStream, Client) {
    parallel_test_client_with_params(Parameters::default()).await
}

pub async fn parallel_test_client_with_params(params: Parameters) -> (TcpStream, Client) {
    let addr = "127.0.0.1:0".to_string();
    let conn_addr = addr.clone();
    let stream = TcpListener::bind(&conn_addr).await.unwrap();
    let port = stream.local_addr().unwrap().port();
    let connect_handle = tokio::spawn(async move {
        let (stream, _) = stream.accept().await.unwrap();
        let stream = Stream::plain(stream, 4096);

        Client::new_test(stream, params)
    });

    let conn = TcpStream::connect(&format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    let client = connect_handle.await.unwrap();

    (conn, client)
}

macro_rules! new_client {
    ($replicas:expr) => {{
        crate::logger();
        let (conn, client) = test_client($replicas).await;
        let engine = QueryEngine::from_client(&client).unwrap();

        (conn, client, engine)
    }};
}

pub fn buffer(messages: &[impl ToBytes]) -> BytesMut {
    let mut buf = BytesMut::new();
    for message in messages {
        buf.put(message.to_bytes().unwrap());
    }
    buf
}

/// Read a series of messages from the stream and make sure
/// they arrive in the right order.
pub async fn read_messages(stream: &mut (impl AsyncRead + Unpin), codes: &[char]) -> Vec<Message> {
    let mut result = vec![];
    let mut error = false;

    for code in codes {
        let c = stream.read_u8().await.unwrap();

        if c as char != *code {
            if c as char == 'E' {
                error = true;
            } else {
                panic!("got message code {}, expected {}", c as char, *code);
            }
        }

        let len = stream.read_i32().await.unwrap();
        let mut data = vec![0u8; len as usize - 4];
        stream.read_exact(&mut data).await.unwrap();

        let mut message = BytesMut::new();
        message.put_u8(c);
        message.put_i32(len);
        message.put_slice(&data);

        let message = Message::new(message.freeze());

        if error {
            panic!(
                "{:#}",
                ErrorResponse::from_bytes(message.to_bytes().unwrap()).unwrap()
            );
        } else {
            result.push(message);
        }
    }

    result
}

macro_rules! buffer {
    ( $( $msg:block ),* ) => {{
        let mut buf = BytesMut::new();

        $(
           buf.put($msg.to_bytes().unwrap());
        )*

        buf
    }}
}

macro_rules! read_one {
    ($conn:expr) => {{
        let mut buf = BytesMut::new();
        let code = $conn.read_u8().await.unwrap();
        buf.put_u8(code);
        let len = $conn.read_i32().await.unwrap();
        buf.put_i32(len);
        buf.resize(len as usize + 1, 0);
        $conn.read_exact(&mut buf[5..]).await.unwrap();

        buf
    }};
}

macro_rules! read {
    ($conn:expr, $codes:expr) => {{
        let mut result = vec![];
        for c in $codes {
            let buf = read_one!($conn);
            assert_eq!(buf[0] as char, c);
            result.push(buf);
        }

        result
    }};
}

#[tokio::test]
async fn test_multiple_async() {
    let (mut conn, mut client, _) = new_client!(false);

    let handle = tokio::spawn(async move {
        client.run().await.unwrap();
    });

    let mut buf = vec![];
    for i in 0..50 {
        let q = Query::new(format!("SELECT {}::bigint AS one", i));
        buf.extend(&q.to_bytes().unwrap())
    }

    conn.write_all(&buf).await.unwrap();

    for i in 0..50 {
        let mut codes = vec![];
        for c in ['T', 'D', 'C', 'Z'] {
            // Buffer.
            let mut b = BytesMut::new();
            // Code
            let code = conn.read_u8().await.unwrap();
            assert_eq!(c, code as char);
            b.put_u8(code);
            // Length
            let len = conn.read_i32().await.unwrap();
            b.put_i32(len);
            b.resize(len as usize + 1, 0);
            // The rest.
            conn.read_exact(&mut b[5..]).await.unwrap();
            match c {
                'T' => {
                    let rd = RowDescription::from_bytes(b.freeze()).unwrap();
                    assert_eq!(rd.field(0).unwrap(), &Field::bigint("one"));
                    codes.push(c);
                }

                'D' => {
                    let dr = DataRow::from_bytes(b.freeze()).unwrap();
                    assert_eq!(dr.get::<i64>(0, Format::Text), Some(i));
                    codes.push(c);
                }

                'C' => {
                    let cc = CommandComplete::from_bytes(b.freeze()).unwrap();
                    assert_eq!(cc.command(), "SELECT 1");
                    codes.push(c);
                }

                'Z' => {
                    let rfq = ReadyForQuery::from_bytes(b.freeze()).unwrap();
                    assert_eq!(rfq.status, 'I');
                    codes.push(c);
                }

                _ => panic!("unexpected code"),
            }
        }

        assert_eq!(codes, ['T', 'D', 'C', 'Z']);
    }

    conn.write_all(&Terminate.to_bytes().unwrap())
        .await
        .unwrap();
    handle.await.unwrap();

    let dbs = databases();
    let cluster = dbs.cluster(("pgdog", "pgdog")).unwrap();
    let shard = cluster.shards()[0].pools()[0].state();
    // Each simple query gets its own server assignment.
    assert_eq!(shard.stats.counts.server_assignment_count, 50);
}

#[tokio::test]
async fn test_abrupt_disconnect() {
    let (conn, mut client, _) = new_client!(false);

    drop(conn);

    let event = client.buffer(State::Idle).await.unwrap();
    assert_eq!(event, BufferEvent::DisconnectAbrupt);
    assert!(client.client_request.messages.is_empty());

    // Client disconnects and returns gracefully.
    let (conn, mut client, _) = new_client!(false);
    drop(conn);
    client.run().await.unwrap();
}

#[tokio::test]
async fn test_client_idle_timeout() {
    let (mut conn, mut client, _inner) = new_client!(false);

    let mut config = (*config()).clone();
    config.config.general.client_idle_timeout = 25;
    set(config).unwrap();

    let start = Instant::now();
    let res = client.buffer(State::Idle).await.unwrap();
    assert_eq!(res, BufferEvent::DisconnectAbrupt);

    let err = read_one!(conn);
    assert!(start.elapsed() >= Duration::from_millis(25));
    let err = ErrorResponse::from_bytes(err.freeze()).unwrap();
    assert_eq!(err.code, "57P05");

    assert!(timeout(
        Duration::from_millis(50),
        client.buffer(State::IdleInTransaction)
    )
    .await
    .is_err());
}

#[tokio::test]
async fn test_parse_describe_flush_bind_execute_close_sync() {
    let (mut conn, mut client, _) = new_client!(false);

    let handle = tokio::spawn(async move {
        client.run().await.unwrap();
    });

    let mut buf = BytesMut::new();

    buf.put(Parse::new_anonymous("SELECT 1").to_bytes().unwrap());
    buf.put(Describe::new_statement("").to_bytes().unwrap());
    buf.put(Flush.to_bytes().unwrap());

    conn.write_all(&buf).await.unwrap();

    let _ = read!(conn, ['1', 't', 'T']);

    let mut buf = BytesMut::new();
    buf.put(Bind::new_statement("").to_bytes().unwrap());
    buf.put(Execute::new().to_bytes().unwrap());
    buf.put(Close::named("").to_bytes().unwrap());
    buf.put(Sync.to_bytes().unwrap());

    conn.write_all(&buf).await.unwrap();

    let _ = read!(conn, ['2', 'D', 'C', '3', 'Z']);

    conn.write_all(&Terminate.to_bytes().unwrap())
        .await
        .unwrap();

    handle.await.unwrap();
}

#[tokio::test]
async fn test_client_login_timeout() {
    use crate::config::config;
    use tokio::time::sleep;

    crate::logger();
    load_test();

    let mut config = (*config()).clone();
    config.config.general.client_login_timeout = 100;
    set(config).unwrap();

    let addr = "127.0.0.1:0".to_string();
    let stream = TcpListener::bind(&addr).await.unwrap();
    let port = stream.local_addr().unwrap().port();

    let handle = tokio::spawn(async move {
        let (stream, addr) = stream.accept().await.unwrap();
        let stream = Stream::plain(stream, 4096);

        let mut params = crate::net::parameter::Parameters::default();
        params.insert("user", "pgdog");
        params.insert("database", "pgdog");

        Client::spawn(stream, params, addr, crate::config::config()).await
    });

    let conn = TcpStream::connect(&format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    sleep(Duration::from_millis(150)).await;

    drop(conn);

    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_statement_mode() {
    crate::logger();

    load_test_with_pooler_mode(PoolerMode::Statement);
    let (mut conn, mut client) = parallel_test_client().await;

    tokio::spawn(async move {
        client.run().await.unwrap();
    });

    let req = buffer!({ Query::new("BEGIN") });
    conn.write_all(&req).await.unwrap();

    let msgs = read!(conn, ['E', 'Z']);
    let error = ErrorResponse::from_bytes(msgs[0].clone().freeze()).unwrap();
    assert_eq!(error.code, "58000");
}

#[tokio::test]
async fn test_client_login_timeout_does_not_affect_queries() {
    crate::logger();
    load_test();

    let mut config = (*config()).clone();
    config.config.general.client_login_timeout = 100;
    set(config).unwrap();

    let (mut conn, mut client, _) = new_client!(false);

    let handle = tokio::spawn(async move {
        client.run().await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(150)).await;

    let buf = buffer!({ Query::new("SELECT pg_sleep(0.2)") });
    conn.write_all(&buf).await.unwrap();

    let msgs = read!(conn, ['T', 'D', 'C', 'Z']);
    assert_eq!(msgs[0][0] as char, 'T');
    assert_eq!(msgs[3][0] as char, 'Z');

    conn.write_all(&buffer!({ Terminate })).await.unwrap();
    handle.await.unwrap();
}

#[tokio::test]
async fn test_anon_prepared_statements() {
    crate::logger();
    load_test();

    let (mut conn, mut client, _) = new_client!(false);

    let mut c = (*config()).clone();
    c.config.general.prepared_statements = PreparedStatements::ExtendedAnonymous;
    set(c).unwrap();

    let handle = tokio::spawn(async move {
        client.run().await.unwrap();
    });

    conn.write_all(&buffer!(
        { Parse::new_anonymous("SELECT 1") },
        { Bind::new_params("", &[]) },
        { Execute::new() },
        { Sync }
    ))
    .await
    .unwrap();

    let _ = read!(conn, ['1', '2', 'D', 'C', 'Z']);

    {
        let cache = prepared_statements::PreparedStatements::global();
        let read = cache.read();
        assert!(!read.is_empty());
    }

    conn.write_all(&buffer!({ Terminate })).await.unwrap();
    handle.await.unwrap();
}

#[tokio::test]
async fn test_anon_prepared_statements_extended() {
    crate::logger();
    load_test();

    let (mut conn, mut client, _) = new_client!(false);

    let mut c = (*config()).clone();
    c.config.general.prepared_statements = PreparedStatements::Extended;
    set(c).unwrap();

    let handle = tokio::spawn(async move {
        client.run().await.unwrap();
    });

    conn.write_all(&buffer!(
        { Parse::new_anonymous("SELECT 1") },
        { Bind::new_params("", &[]) },
        { Execute::new() },
        { Sync }
    ))
    .await
    .unwrap();

    let _ = read!(conn, ['1', '2', 'D', 'C', 'Z']);

    {
        let cache = prepared_statements::PreparedStatements::global();
        let read = cache.read();
        assert!(read.is_empty());
    }

    conn.write_all(&buffer!({ Terminate })).await.unwrap();
    handle.await.unwrap();
}

#[tokio::test]
async fn test_query_timeout() {
    crate::logger();
    load_test();

    let (mut conn, mut client, mut engine) = new_client!(false);

    let mut c = (*config()).clone();
    c.config.general.query_timeout = 50;
    set(c).unwrap();

    let buf = buffer!({ Query::new("SELECT pg_sleep(0.2)") });
    conn.write_all(&buf).await.unwrap();

    client.buffer(State::Idle).await.unwrap();
    let result = client.client_messages(&mut engine).await;

    assert!(result.is_err());

    let pools = databases().cluster(("pgdog", "pgdog")).unwrap().shards()[0].pools();
    let state = pools[0].state();
    assert_eq!(state.force_close, 1);
}
