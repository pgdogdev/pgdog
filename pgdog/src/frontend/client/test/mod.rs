use std::time::{Duration, Instant};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufStream},
    net::{TcpListener, TcpStream},
    time::timeout,
};

use bytes::{Buf, BufMut, BytesMut};

use crate::{
    backend::databases::databases,
    config::{config, load_test, load_test_replicas, set, PreparedStatements, Role},
    frontend::{
        client::{BufferEvent, QueryEngine},
        prepared_statements, Client,
    },
    net::{
        bind::Parameter, Bind, Close, CommandComplete, DataRow, Describe, ErrorResponse, Execute,
        Field, Flush, Format, FromBytes, Parse, Protocol, Query, ReadyForQuery, RowDescription,
        Sync, Terminate, ToBytes,
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
    let addr = "127.0.0.1:0".to_string();
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
        let engine = QueryEngine::from_client(&client).unwrap();

        (conn, client, engine)
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
    let (mut conn, mut client, mut engine) = new_client!(false);

    let query = Query::new("SELECT 1").to_bytes().unwrap();

    conn.write_all(&query).await.unwrap();

    client.buffer(State::Idle).await.unwrap();
    assert_eq!(client.client_request.total_message_len(), query.len());

    client.client_messages(&mut engine).await.unwrap();
    assert!(client.transaction.is_none());
    assert_eq!(engine.stats().state, State::Active);
    // Buffer not cleared yet.
    assert_eq!(client.client_request.total_message_len(), query.len());

    assert!(engine.backend().connected());
    // let command = engine
    //     .command(
    //         &mut client.request_buffer,
    //         &mut client.prepared_statements,
    //         &client.params,
    //         client.in_transaction,
    //     )
    //     .unwrap();
    // assert!(matches!(command, Some(Command::Query(_))));

    let mut len = 0;

    for c in ['T', 'D', 'C', 'Z'] {
        let msg = engine.backend().read().await.unwrap();
        len += msg.len();
        assert_eq!(msg.code(), c);
        client.server_message(&mut engine, msg).await.unwrap();
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
        Bind::new_params(
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
    crate::logger();
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
            { Parse::named("test", "SELECT * FROM test_client_with_replicas") },
            { Bind::new_statement("test") },
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
        let idle = state.idle;
        // We're using round robin
        // and one write (create table) is going to primary.
        pool_recv += state.stats.counts.received as isize;
        pool_sent += state.stats.counts.sent as isize;

        match role {
            Role::Primary => {
                assert_eq!(
                    state.stats.counts.query_count,
                    state.stats.counts.server_assignment_count + state.stats.counts.healthchecks
                );
                assert_eq!(
                    state.stats.counts.xact_count,
                    state.stats.counts.server_assignment_count + state.stats.counts.healthchecks
                );
                assert_eq!(state.stats.counts.server_assignment_count, 14);
                assert_eq!(state.stats.counts.bind_count, 13);
                // strange behavior locally here, is `idle`/1 on CI, but 2 on local.
                assert!(state.stats.counts.parse_count >= idle);
                assert!(state.stats.counts.parse_count <= idle + 1);
                assert_eq!(state.stats.counts.rollbacks, 0);
                assert_eq!(state.stats.counts.healthchecks, idle);
                pool_recv -= (healthcheck_len_recv * state.stats.counts.healthchecks) as isize;
            }
            Role::Replica => {
                assert_eq!(state.stats.counts.server_assignment_count, 13);
                assert_eq!(
                    state.stats.counts.query_count,
                    state.stats.counts.server_assignment_count + state.stats.counts.healthchecks
                );
                assert_eq!(
                    state.stats.counts.xact_count,
                    state.stats.counts.server_assignment_count + state.stats.counts.healthchecks
                );
                assert_eq!(state.stats.counts.bind_count, 13);
                assert!(state.stats.counts.parse_count <= idle + 1); // TODO: figure out what's going on, I' guessing I need to wait a little bit for the connection to be checked in.
                assert_eq!(state.stats.counts.rollbacks, 0);
                assert!(state.stats.counts.healthchecks <= idle + 1); // TODO: same
                pool_sent -= (healthcheck_len_sent * state.stats.counts.healthchecks) as isize;
            }
        }
    }

    assert!(pool_sent <= len_sent as isize);
    assert!(pool_recv <= len_recv as isize);
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
async fn test_lock_session() {
    let (mut conn, mut client, mut engine) = new_client!(true);

    conn.write_all(&buffer!(
        { Query::new("SET application_name TO 'blah'") },
        { Query::new("SELECT pg_advisory_lock(1234)") }
    ))
    .await
    .unwrap();

    for _ in 0..2 {
        client.buffer(State::Idle).await.unwrap();
        client.client_messages(&mut engine).await.unwrap();
    }

    for c in ['T', 'D', 'C', 'Z'] {
        let msg = engine.read_backend().await.unwrap();
        assert_eq!(msg.code(), c);
        client.server_message(&mut engine, msg).await.unwrap();
    }

    // Session locked.
    assert!(engine.backend().is_dirty());
    assert!(!engine.backend().done());
    assert!(client.params.contains_key("application_name"));

    engine.backend().disconnect();
}

#[tokio::test]
async fn test_transaction_state() {
    let (mut conn, mut client, mut engine) = new_client!(true);

    conn.write_all(&buffer!({ Query::new("BEGIN") }))
        .await
        .unwrap();

    client.buffer(State::Idle).await.unwrap();
    client.client_messages(&mut engine).await.unwrap();
    read!(conn, ['C', 'Z']);

    assert!(client.transaction.is_some());
    assert!(engine.router().route().is_write());
    assert!(engine.router().in_transaction());

    conn.write_all(&buffer!(
        { Parse::named("test", "SELECT $1") },
        { Describe::new_statement("test") },
        { Sync }
    ))
    .await
    .unwrap();

    client.buffer(State::Idle).await.unwrap();
    client.client_messages(&mut engine).await.unwrap();

    assert!(client.transaction.is_some());
    assert!(engine.router().route().is_write());
    assert!(engine.router().in_transaction());

    for c in ['1', 't', 'T', 'Z'] {
        let msg = engine.backend().read().await.unwrap();
        assert_eq!(msg.code(), c);

        client.server_message(&mut engine, msg).await.unwrap();
    }

    read!(conn, ['1', 't', 'T', 'Z']);

    conn.write_all(&buffer!(
        {
            Bind::new_params(
                "test",
                &[Parameter {
                    len: 1,
                    data: "1".as_bytes().to_vec(),
                }],
            )
        },
        { Execute::new() },
        { Sync }
    ))
    .await
    .unwrap();

    client.buffer(State::Idle).await.unwrap();
    client.client_messages(&mut engine).await.unwrap();

    for c in ['2', 'D', 'C', 'Z'] {
        let msg = engine.backend().read().await.unwrap();
        assert_eq!(msg.code(), c);

        client.server_message(&mut engine, msg).await.unwrap();
    }

    read!(conn, ['2', 'D', 'C', 'Z']);

    assert!(client.transaction.is_some());
    assert!(engine.router().route().is_write());
    assert!(engine.router().in_transaction());

    conn.write_all(&buffer!({ Query::new("COMMIT") }))
        .await
        .unwrap();

    client.buffer(State::Idle).await.unwrap();
    client.client_messages(&mut engine).await.unwrap();

    for c in ['C', 'Z'] {
        let msg = engine.backend().read().await.unwrap();
        assert_eq!(msg.code(), c);

        client.server_message(&mut engine, msg).await.unwrap();
    }

    read!(conn, ['C', 'Z']);

    assert!(client.transaction.is_none());
}

#[tokio::test]
async fn test_close_parse() {
    crate::logger();

    let (mut conn, mut client, mut engine) = new_client!(true);

    conn.write_all(&buffer!({ Close::named("test") }, { Sync }))
        .await
        .unwrap();

    conn.write_all(&buffer!({ Query::new("SELECT 1") }))
        .await
        .unwrap();

    client.buffer(State::Idle).await.unwrap();
    client.client_messages(&mut engine).await.unwrap();

    client.buffer(State::Idle).await.unwrap();
    client.client_messages(&mut engine).await.unwrap();

    for _ in ['T', 'D', 'C', 'Z'] {
        let msg = engine.backend().read().await.unwrap();
        client.server_message(&mut engine, msg).await.unwrap();
    }

    read!(conn, ['3', 'Z', 'T', 'D', 'C', 'Z']);

    conn.write_all(&buffer!(
        { Close::named("test1") },
        { Parse::named("test1", "SELECT $1") },
        { Flush }
    ))
    .await
    .unwrap();

    client.buffer(State::Idle).await.unwrap();
    client.client_messages(&mut engine).await.unwrap();

    for _ in ['3', '1'] {
        let msg = engine.backend().read().await.unwrap();
        client.server_message(&mut engine, msg).await.unwrap();
    }

    read!(conn, ['3', '1']);
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
async fn test_prepared_syntax_error() {
    let (mut conn, mut client, mut engine) = new_client!(false);

    conn.write_all(&buffer!({ Parse::named("test", "SELECT sdfsf") }, { Sync }))
        .await
        .unwrap();

    client.buffer(State::Idle).await.unwrap();
    client.client_messages(&mut engine).await.unwrap();

    for c in ['E', 'Z'] {
        let msg = engine.backend().read().await.unwrap();
        assert_eq!(msg.code(), c);
        client.server_message(&mut engine, msg).await.unwrap();
    }
    read!(conn, ['E', 'Z']);

    let stmts = client.prepared_statements.global.clone();

    assert_eq!(stmts.read().statements().iter().next().unwrap().1.used, 1);

    conn.write_all(&buffer!({ Terminate })).await.unwrap();
    let event = client.buffer(State::Idle).await.unwrap();
    assert_eq!(event, BufferEvent::DisconnectGraceful);
    drop(client);

    assert_eq!(stmts.read().statements().iter().next().unwrap().1.used, 0);
}

#[tokio::test]
async fn test_close_parse_same_name_global_cache() {
    let (mut conn, mut client, mut engine) = new_client!(false);

    // Send Close and Parse for the same name with Flush
    conn.write_all(&buffer!(
        { Close::named("test_stmt") },
        { Parse::named("test_stmt", "SELECT $1") },
        { Flush }
    ))
    .await
    .unwrap();

    client.buffer(State::Idle).await.unwrap();
    client.client_messages(&mut engine).await.unwrap();

    // Read responses
    for c in ['3', '1'] {
        let msg = engine.backend().read().await.unwrap();
        assert_eq!(msg.code(), c);
        client.server_message(&mut engine, msg).await.unwrap();
    }
    read!(conn, ['3', '1']);

    // Verify the statement is registered correctly in the global cache
    let global_cache = client.prepared_statements.global.clone();
    assert_eq!(global_cache.read().len(), 1);
    let binding = global_cache.write();
    let (_, cached_stmt) = binding.statements().iter().next().unwrap();
    assert_eq!(cached_stmt.used, 1);

    // Verify the SQL content in the global cache
    let global_stmt_name = cached_stmt.name();
    let cached_query = binding.query(&global_stmt_name).unwrap();
    assert_eq!(cached_query, "SELECT $1");

    // Verify the client's local cache
    assert_eq!(client.prepared_statements.len_local(), 1);
    assert!(client.prepared_statements.name("test_stmt").is_some());

    conn.write_all(&buffer!({ Terminate })).await.unwrap();
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
    use crate::{config::config, frontend::comms::comms};
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
        let stream = BufStream::new(stream);
        let stream = Stream::Plain(stream);

        let mut params = crate::net::parameter::Parameters::default();
        params.insert("user", "pgdog");
        params.insert("database", "pgdog");

        Client::spawn(stream, params, addr, comms()).await
    });

    let conn = TcpStream::connect(&format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    sleep(Duration::from_millis(150)).await;

    drop(conn);

    handle.await.unwrap().unwrap();
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
