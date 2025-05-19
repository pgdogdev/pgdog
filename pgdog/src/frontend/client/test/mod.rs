use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufStream},
    net::{TcpListener, TcpStream},
};

use bytes::{Buf, BufMut, BytesMut};

use crate::{
    backend::databases::databases,
    config::{
        test::{load_test, load_test_replicas},
        Role,
    },
    frontend::{
        client::{BufferEvent, Inner},
        Client, Command,
    },
    net::{
        bind::Parameter, Bind, CommandComplete, DataRow, Describe, Execute, Field, Format,
        FromBytes, Parse, Protocol, Query, ReadyForQuery, RowDescription, Sync, Terminate, ToBytes,
    },
    state::State,
};

use super::Stream;

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

pub async fn parallel_test_client() -> (TcpStream, Client) {
    let addr = format!("127.0.0.1:0");
    let conn_addr = addr.clone();
    let stream = TcpListener::bind(&conn_addr).await.unwrap();
    let port = stream.local_addr().unwrap().port();
    let connect_handle = tokio::spawn(async move {
        let (stream, addr) = stream.accept().await.unwrap();

        let stream = BufStream::new(stream);
        let stream = Stream::Plain(stream);

        Client::new_test(stream, addr)
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
        let inner = Inner::new(&client).unwrap();

        (conn, client, inner)
    }};
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
async fn test_test_client() {
    let (mut conn, mut client, mut inner) = new_client!(false);

    let query = Query::new("SELECT 1").to_bytes().unwrap();

    conn.write_all(&query).await.unwrap();

    client.buffer().await.unwrap();
    assert_eq!(client.request_buffer.len(), query.len());

    let disconnect = client.client_messages(inner.get()).await.unwrap();
    assert!(!disconnect);
    assert!(!client.in_transaction);
    assert_eq!(inner.stats.state, State::Active);
    // Buffer not cleared yet.
    assert_eq!(client.request_buffer.len(), query.len());

    assert!(inner.backend.connected());
    let command = inner
        .command(
            &mut client.request_buffer,
            &mut client.prepared_statements,
            &client.params,
        )
        .unwrap();
    assert!(matches!(command, Some(Command::Query(_))));

    let mut len = 0;

    for c in ['T', 'D', 'C', 'Z'] {
        let msg = inner.backend.read().await.unwrap();
        len += msg.len();
        assert_eq!(msg.code(), c);
        let disconnect = client.server_message(inner.get(), msg).await.unwrap();
        assert!(!disconnect);
    }

    let mut bytes = BytesMut::zeroed(len);
    conn.read_exact(&mut bytes).await.unwrap();

    for c in ['T', 'D', 'C', 'Z'] {
        let code = bytes.get_u8() as char;
        assert_eq!(code, c);
        let len = bytes.get_i32() - 4; // Len includes self which we just read.
        let _bytes = bytes.split_to(len as usize);
    }
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
    // This is kind of the problem: all queries go to one server.
    // In a sharded context, we need a way to split them up.
    assert!(shard.stats.counts.server_assignment_count < 50);
}

#[tokio::test]
async fn test_client_extended() {
    let (mut conn, mut client, _) = new_client!(false);

    let handle = tokio::spawn(async move {
        client.run().await.unwrap();
    });

    let mut buf = BytesMut::new();

    buf.put(Parse::named("test", "SELECT $1").to_bytes().unwrap());
    buf.put(
        Bind::test_params(
            "test",
            &[Parameter {
                len: 3,
                data: "123".into(),
            }],
        )
        .to_bytes()
        .unwrap(),
    );
    buf.put(Describe::new_statement("test").to_bytes().unwrap());
    buf.put(Execute::new().to_bytes().unwrap());
    buf.put(Sync.to_bytes().unwrap());
    buf.put(Terminate.to_bytes().unwrap());

    conn.write_all(&buf).await.unwrap();

    let _ = read!(conn, ['1', '2', 't', 'T', 'D', 'C', 'Z']);

    handle.await.unwrap();
}

#[tokio::test]
async fn test_client_with_replicas() {
    let (mut conn, mut client, _) = new_client!(true);

    let handle = tokio::spawn(async move {
        client.run().await.unwrap();
    });

    let mut len_sent = 0;
    let mut len_recv = 0;

    let buf =
        buffer!({ Query::new("CREATE TABLE IF NOT EXISTS test_client_with_replicas (id BIGINT)") });
    conn.write_all(&buf).await.unwrap();
    len_sent += buf.len();

    // Terminate messages are not sent to servers,
    // so they are not counted in bytes sent/recv.
    conn.write_all(&buffer!({ Terminate })).await.unwrap();

    loop {
        let msg = read_one!(conn);
        len_recv += msg.len();
        if msg[0] as char == 'Z' {
            break;
        }
    }

    handle.await.unwrap();

    let mut clients = vec![];
    for _ in 0..26 {
        let (mut conn, mut client) = parallel_test_client().await;
        let handle = tokio::spawn(async move {
            client.run().await.unwrap();
        });
        let buf = buffer!(
            { Parse::new_anonymous("SELECT * FROM test_client_with_replicas") },
            { Bind::test_statement("") },
            { Execute::new() },
            { Sync }
        );
        conn.write_all(&buf).await.unwrap();
        len_sent += buf.len();

        clients.push((conn, handle));
    }

    for (mut conn, handle) in clients {
        let msgs = read!(conn, ['1', '2', 'C', 'Z']);
        for msg in msgs {
            len_recv += msg.len();
        }

        // Terminate messages are not sent to servers,
        // so they are not counted in bytes sent/recv.
        conn.write_all(&buffer!({ Terminate })).await.unwrap();
        conn.flush().await.unwrap();
        handle.await.unwrap();
    }

    let healthcheck_len_recv = 5 + 6; // Empty query response + ready for query from health check
    let healthcheck_len_sent = Query::new(";").len(); // ; Health check query query

    let pools = databases().cluster(("pgdog", "pgdog")).unwrap().shards()[0].pools_with_roles();
    let mut pool_recv = 0;
    let mut pool_sent = 0;
    for (role, pool) in pools {
        let state = pool.state();
        // We're using round robin
        // and one write (create table) is going to primary.
        pool_recv += state.stats.counts.received as isize;
        pool_sent += state.stats.counts.sent as isize;

        match role {
            Role::Primary => {
                assert_eq!(state.stats.counts.server_assignment_count, 14);
                assert_eq!(state.stats.counts.bind_count, 13);
                assert_eq!(state.stats.counts.parse_count, 13);
                assert_eq!(state.stats.counts.rollbacks, 0);
                assert_eq!(state.stats.counts.healthchecks, 1);
                pool_recv -= (healthcheck_len_recv * state.stats.counts.healthchecks) as isize;
            }
            Role::Replica => {
                assert_eq!(state.stats.counts.server_assignment_count, 13);
                assert_eq!(state.stats.counts.bind_count, 13);
                assert_eq!(state.stats.counts.parse_count, 13);
                assert_eq!(state.stats.counts.rollbacks, 0);
                assert!(state.stats.counts.healthchecks >= 1);
                pool_sent -= (healthcheck_len_sent * state.stats.counts.healthchecks) as isize;
            }
        }
    }

    // TODO: find the missing bytes
    assert!((pool_recv - len_recv as isize).abs() < 20);
    assert!((pool_sent - len_sent as isize).abs() < 20);
}

#[tokio::test]
async fn test_abrupt_disconnect() {
    let (conn, mut client, _) = new_client!(false);

    drop(conn);

    let event = client.buffer().await.unwrap();
    assert_eq!(event, BufferEvent::DisconnectAbrupt);
    assert!(client.request_buffer.is_empty());

    // Client disconnects and returns gracefully.
    let (conn, mut client, _) = new_client!(false);
    drop(conn);
    client.run().await.unwrap();
}
