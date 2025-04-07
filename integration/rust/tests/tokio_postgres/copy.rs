// use futures_util::{TryStreamExt, pin_mut};
// use tokio_postgres::binary_copy::{BinaryCopyInWriter, BinaryCopyOutStream};
// use tokio_postgres::types::Type;

// use rust::setup::connections;

// #[tokio::test]
// async fn test_copy() {
//     for conn in connections().await {
//         conn.batch_execute(
//             "DROP SCHEMA IF EXISTS rust_test_insert CASCADE;
//             CREATE SCHEMA rust_test_insert;
//             CREATE TABLE rust_test_insert.sharded (id BIGINT PRIMARY KEY, value VARCHAR);
//             SET search_path TO rust_test_insert,public;",
//         )
//         .await
//         .unwrap();

//         let sink = conn
//             .copy_in("COPY sharded (id, value) FROM STDIN BINARY")
//             .await
//             .unwrap();
//         let writer = BinaryCopyInWriter::new(sink, &[Type::INT8, Type::TEXT]);
//         for i in 0..25 {
//             let writer = tokio::pin!(writer);
//             writer.
//                 .write(&[&1_i64, &"foobar"])
//                 .await
//                 .unwrap();
//         }
//     }
// }
