mod insert {
    use rand::{rng, Rng};
    use tokio::{io::AsyncWriteExt, spawn};

    use crate::{
        frontend::client::{
            query_engine::multi_step::test::truncate_table,
            test::{read_messages, test_client_sharded},
        },
        net::{CommandComplete, FromBytes, Query, ToBytes},
    };

    #[tokio::test]
    async fn test_simple() {
        crate::logger();

        let (mut stream, mut client) = test_client_sharded().await;

        spawn(async move {
            client.run().await.unwrap();
        });

        let values = (0..5)
            .into_iter()
            .map(|_| {
                let val = rng().random::<i64>();
                (format!("'val_{}'", val), val)
            })
            .map(|tuple| format!("({}, {})", tuple.1, tuple.0))
            .collect::<Vec<_>>()
            .join(", ");

        stream
            .write_all(
                &Query::new(format!("INSERT INTO sharded (id, value) VALUES {}", values))
                    .to_bytes()
                    .unwrap(),
            )
            .await
            .unwrap();
        stream.flush().await.unwrap();

        let messages = read_messages(&mut stream, &['C', 'Z']).await;
        assert_eq!(messages.len(), 2);

        let command_complete =
            CommandComplete::from_bytes(messages[0].to_bytes().unwrap()).unwrap();
        assert_eq!(command_complete.rows().unwrap().unwrap(), 5);

        truncate_table("sharded", &mut stream).await;
    }
}
