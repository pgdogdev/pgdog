mod insert {
    use tokio::{io::AsyncWriteExt, spawn};

    use crate::{
        frontend::client::{
            query_engine::multi_step::test::truncate_table,
            test::{read_messages, test_client_sharded},
        },
        net::{
            bind::Parameter, Bind, CommandComplete, DataRow, Describe, Execute, Flush, Format,
            FromBytes, Parse, Sync, ToBytes,
        },
    };

    #[tokio::test]
    async fn test_prepared() {
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

        let _ = read_messages(&mut stream, &['1', 't', 'T']).await;

        stream.write_all(&params.to_bytes().unwrap()).await.unwrap();
        stream.write_all(&exec.to_bytes().unwrap()).await.unwrap();
        stream.write_all(&sync.to_bytes().unwrap()).await.unwrap();
        stream.flush().await.unwrap();

        let messages = read_messages(&mut stream, &['2', 'D', 'D', 'D', 'D', 'C', 'Z']).await;

        // Assert DataRow values (messages[1..5] are the 4 DataRow messages)
        let row1 = DataRow::from_bytes(messages[1].to_bytes().unwrap()).unwrap();
        assert_eq!(row1.get::<i64>(0, Format::Text), Some(123423425245));
        assert_eq!(row1.get_text(1), Some("test_value_1".to_string()));

        let row2 = DataRow::from_bytes(messages[2].to_bytes().unwrap()).unwrap();
        assert_eq!(row2.get::<i64>(0, Format::Text), Some(123423425246));
        assert_eq!(row2.get_text(1), Some("test_value_2".to_string()));

        let row3 = DataRow::from_bytes(messages[3].to_bytes().unwrap()).unwrap();
        assert_eq!(row3.get::<i64>(0, Format::Text), Some(123423425247));
        assert_eq!(row3.get_text(1), Some("test_value_3".to_string()));

        let row4 = DataRow::from_bytes(messages[4].to_bytes().unwrap()).unwrap();
        assert_eq!(row4.get::<i64>(0, Format::Text), Some(12342342524823424));
        assert_eq!(row4.get_text(1), Some("test_value_4".to_string()));

        // Assert CommandComplete returns 4 rows
        let cc = CommandComplete::from_bytes(messages[5].to_bytes().unwrap()).unwrap();
        assert_eq!(cc.rows().unwrap(), Some(4));

        truncate_table("sharded", &mut stream).await;
    }
}
