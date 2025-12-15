mod insert {
    use tokio::{io::AsyncWriteExt, spawn};

    use crate::{
        frontend::client::{
            query_engine::multi_step::test::truncate_table,
            test::{read_messages, test_client_sharded},
        },
        net::{bind::Parameter, Bind, Describe, Execute, Flush, Parse, Sync, ToBytes},
    };

    #[tokio::test]
    async fn test_extended() {
        crate::logger();

        let (mut stream, mut client) = test_client_sharded().await;
        spawn(async move { client.run().await.unwrap() });

        truncate_table("sharded", &mut stream).await;

        let stmt = Parse::named(
            "test_multi",
            "INSERT INTO sharded (id, value) VALUES ($1, $2), ($3, $4), ($5, $6), ($7, 'test_value_4') RETURNING *",
        );
        let desc = Describe::new_statement("test_multi");
        let flush = Flush;

        let params = Bind::new_params(
            "test_multi",
            &[
                Parameter::new("123423425245".as_bytes()),
                Parameter::new("test_value_1".as_bytes()),
                Parameter::new("123423425246".as_bytes()),
                Parameter::new("test_value_2".as_bytes()),
                Parameter::new("123423425247".as_bytes()),
                Parameter::new("test_value_3".as_bytes()),
                Parameter::new("12342342524823424".as_bytes()),
            ],
        );
        let exec = Execute::new();
        let sync = Sync;

        stream.write_all(&stmt.to_bytes().unwrap()).await.unwrap();
        stream.write_all(&desc.to_bytes().unwrap()).await.unwrap();
        stream.write_all(&flush.to_bytes().unwrap()).await.unwrap();
        stream.flush().await.unwrap();
        println!("here");

        let _ = read_messages(&mut stream, &['1', 't', 'T']).await;

        stream.write_all(&params.to_bytes().unwrap()).await.unwrap();
        stream.write_all(&exec.to_bytes().unwrap()).await.unwrap();
        stream.write_all(&sync.to_bytes().unwrap()).await.unwrap();
        stream.flush().await.unwrap();

        let messages = read_messages(&mut stream, &['2', 'D', 'D', 'D', 'D', 'C', 'Z']).await;

        truncate_table("sharded", &mut stream).await;
    }
}
