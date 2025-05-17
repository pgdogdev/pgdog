use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufStream},
    net::{TcpListener, TcpStream},
};

use bytes::{Buf, BufMut, BytesMut};

use crate::{
    backend::databases::databases,
    config::test::load_test,
    frontend::{client::Inner, Client, Command},
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

pub async fn test_client(port: u16) -> (TcpStream, Client) {
    load_test();

    let addr = format!("127.0.0.1:{}", port);
    let conn_addr = addr.clone();
    let stream = TcpListener::bind(&conn_addr).await.unwrap();
    let connect_handle = tokio::spawn(async move {
        let (stream, addr) = stream.accept().await.unwrap();

        let stream = BufStream::new(stream);
        let stream = Stream::Plain(stream);

        Client::new_test(stream, addr)
    });

    let conn = TcpStream::connect(&addr).await.unwrap();
    let client = connect_handle.await.unwrap();

    (conn, client)
}

macro_rules! new_client {
    ($port:expr) => {{
        let (conn, client) = test_client($port).await;
        let inner = Inner::new(&client).unwrap();

        (conn, client, inner)
    }};
}

#[tokio::test]
async fn test_test_client() {
    let (mut conn, mut client, mut inner) = new_client!(34000);

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
    let (mut conn, mut client, _) = new_client!(34001);

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
    let (mut conn, mut client, _) = new_client!(34002);

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

    for c in ['1', '2', 't', 'T', 'D', 'C', 'Z'] {
        assert_eq!(c, conn.read_u8().await.unwrap() as char);
        let len = conn.read_i32().await.unwrap();
        let mut the_rest = BytesMut::zeroed(len as usize - 4);
        conn.read_exact(&mut the_rest).await.unwrap();
    }

    handle.await.unwrap();
}
