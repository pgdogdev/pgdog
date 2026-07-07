use crate::setup::{admin_sqlx, connections_sqlx};
use rust_decimal::prelude::*;
use sqlx::{Executor, Pool, Postgres, Row};

#[tokio::test]
async fn test_variance_numeric() {
    let conns = connections_sqlx().await;

    setup_schema(&conns, "test_variance_numeric", "int8").await;
    setup_data(&conns, "test_variance_numeric", TEST_DATA).await;

    let sharded = &conns[1];
    let unsharded = &conns[0];
    let data = sharded.fetch_one("SELECT var_pop(value), var_samp(value), stddev_pop(value), stddev_samp(value) FROM test_variance_numeric").await.unwrap();
    let expected = unsharded.fetch_one("SELECT var_pop(value), var_samp(value), stddev_pop(value), stddev_samp(value) FROM test_variance_numeric").await.unwrap();

    assert_dec_eq(data.get(0), expected.get(0));
    assert_dec_eq(data.get(1), expected.get(1));
    assert_dec_eq(data.get(2), expected.get(2));
    assert_dec_eq(data.get(3), expected.get(3));

    setup_data(&conns, "test_variance_numeric", TEST_DATA_2).await;

    let data = sharded.fetch_one("SELECT var_pop(value), var_samp(value), stddev_pop(value), stddev_samp(value) FROM test_variance_numeric").await.unwrap();
    let expected = unsharded.fetch_one("SELECT var_pop(value), var_samp(value), stddev_pop(value), stddev_samp(value) FROM test_variance_numeric").await.unwrap();

    assert_dec_eq(data.get(0), expected.get(0));
    assert_dec_eq(data.get(1), expected.get(1));
    assert_dec_eq(data.get(2), expected.get(2));
    assert_dec_eq(data.get(3), expected.get(3));
}

#[tokio::test]
async fn test_variance_int4() {
    let conns = connections_sqlx().await;

    setup_schema(&conns, "test_variance_int4", "int4").await;
    setup_data(&conns, "test_variance_int4", TEST_DATA).await;

    let sharded = &conns[1];
    let unsharded = &conns[0];
    let data = sharded.fetch_one("SELECT var_pop(value), var_samp(value), stddev_pop(value), stddev_samp(value) FROM test_variance_int4").await.unwrap();
    let expected = unsharded.fetch_one("SELECT var_pop(value), var_samp(value), stddev_pop(value), stddev_samp(value) FROM test_variance_int4").await.unwrap();

    assert_dec_eq(data.get(0), expected.get(0));
    assert_dec_eq(data.get(1), expected.get(1));
    assert_dec_eq(data.get(2), expected.get(2));
    assert_dec_eq(data.get(3), expected.get(3));

    setup_data(&conns, "test_variance_int4", TEST_DATA_2).await;

    let data = sharded.fetch_one("SELECT var_pop(value), var_samp(value), stddev_pop(value), stddev_samp(value) FROM test_variance_int4").await.unwrap();
    let expected = unsharded.fetch_one("SELECT var_pop(value), var_samp(value), stddev_pop(value), stddev_samp(value) FROM test_variance_int4").await.unwrap();

    assert_dec_eq(data.get(0), expected.get(0));
    assert_dec_eq(data.get(1), expected.get(1));
    assert_dec_eq(data.get(2), expected.get(2));
    assert_dec_eq(data.get(3), expected.get(3));
}

#[tokio::test]
async fn test_variance_double() {
    const CLOSE_ENOUGH: f64 = 1e-14; // 0.00000000000001%
    let conns = connections_sqlx().await;

    setup_schema(&conns, "test_variance_double", "DOUBLE PRECISION").await;
    setup_data(&conns, "test_variance_double", TEST_DATA).await;

    let sharded = &conns[1];
    let unsharded = &conns[0];
    let data = sharded.fetch_one("SELECT var_pop(value), var_samp(value), stddev_pop(value), stddev_samp(value) FROM test_variance_double").await.unwrap();
    let expected = unsharded.fetch_one("SELECT var_pop(value), var_samp(value), stddev_pop(value), stddev_samp(value) FROM test_variance_double").await.unwrap();

    assert_f64_close(data.get(0), expected.get(0), CLOSE_ENOUGH);
    assert_f64_close(data.get(1), expected.get(1), CLOSE_ENOUGH);
    assert_f64_close(data.get(2), expected.get(2), CLOSE_ENOUGH);
    assert_f64_close(data.get(3), expected.get(3), CLOSE_ENOUGH);

    setup_data(&conns, "test_variance_double", TEST_DATA_2).await;

    let data = sharded.fetch_one("SELECT var_pop(value), var_samp(value), stddev_pop(value), stddev_samp(value) FROM test_variance_double").await.unwrap();
    let expected = unsharded.fetch_one("SELECT var_pop(value), var_samp(value), stddev_pop(value), stddev_samp(value) FROM test_variance_double").await.unwrap();

    assert_f64_close(data.get(0), expected.get(0), CLOSE_ENOUGH);
    assert_f64_close(data.get(1), expected.get(1), CLOSE_ENOUGH);
    assert_f64_close(data.get(2), expected.get(2), CLOSE_ENOUGH);
    assert_f64_close(data.get(3), expected.get(3), CLOSE_ENOUGH);
}

#[tokio::test]
async fn test_variance_float() {
    const CLOSEST_WE_CAN_GET_WITH_32_BITS: f64 = 1e-5; // 0.001% yikes
    let conns = connections_sqlx().await;

    setup_schema(&conns, "test_variance_float", "real").await;
    setup_data(&conns, "test_variance_float", TEST_DATA).await;

    let sharded = &conns[1];
    let unsharded = &conns[0];
    let data = sharded.fetch_one("SELECT var_pop(value), var_samp(value), stddev_pop(value), stddev_samp(value) FROM test_variance_float").await.unwrap();
    let expected = unsharded.fetch_one("SELECT var_pop(value), var_samp(value), stddev_pop(value), stddev_samp(value) FROM test_variance_float").await.unwrap();

    assert_f64_close(
        data.get(0),
        expected.get(0),
        CLOSEST_WE_CAN_GET_WITH_32_BITS,
    );
    assert_f64_close(
        data.get(1),
        expected.get(1),
        CLOSEST_WE_CAN_GET_WITH_32_BITS,
    );
    assert_f64_close(
        data.get(2),
        expected.get(2),
        CLOSEST_WE_CAN_GET_WITH_32_BITS,
    );
    assert_f64_close(
        data.get(3),
        expected.get(3),
        CLOSEST_WE_CAN_GET_WITH_32_BITS,
    );

    setup_data(&conns, "test_variance_float", TEST_DATA_2).await;

    let data = sharded.fetch_one("SELECT var_pop(value), var_samp(value), stddev_pop(value), stddev_samp(value) FROM test_variance_float").await.unwrap();
    let expected = unsharded.fetch_one("SELECT var_pop(value), var_samp(value), stddev_pop(value), stddev_samp(value) FROM test_variance_float").await.unwrap();

    assert_f64_close(
        data.get(0),
        expected.get(0),
        CLOSEST_WE_CAN_GET_WITH_32_BITS,
    );
    assert_f64_close(
        data.get(1),
        expected.get(1),
        CLOSEST_WE_CAN_GET_WITH_32_BITS,
    );
    assert_f64_close(
        data.get(2),
        expected.get(2),
        CLOSEST_WE_CAN_GET_WITH_32_BITS,
    );
    assert_f64_close(
        data.get(3),
        expected.get(3),
        CLOSEST_WE_CAN_GET_WITH_32_BITS,
    );
}

/// Assert that two floating point values are as close to equal as we can
/// reasonably get when calculating variance on floats without the original
/// data set.
fn assert_f64_close(actual: f64, expected: f64, max_diff: f64) {
    let diff = (actual - expected).abs() / actual;
    assert!(
        diff < max_diff,
        r#"assertion `left ~= right` failed:
  left: {actual:?}
 right: {expected:?}
  diff: {diff:?}"#
    );
}

fn assert_dec_eq(mut actual: Decimal, expected: Decimal) {
    if actual.scale() > expected.scale() {
        actual.rescale(expected.scale());
    }
    assert_eq!(actual, expected);
}

async fn setup_schema(conns: &[Pool<Postgres>], table_name: &str, data_type: &str) {
    for conn in conns {
        conn.execute(&*format!("DROP TABLE IF EXISTS {table_name}"))
            .await
            .unwrap();
        conn.execute(&*format!(
            "CREATE TABLE {table_name} (customer_id BIGINT NOT NULL, value {data_type})",
        ))
        .await
        .unwrap();
        admin_sqlx().await.execute("RELOAD").await.unwrap();
    }
}

async fn setup_data<T: std::fmt::Display>(conns: &[Pool<Postgres>], table_name: &str, data: &[T]) {
    for conn in conns {
        conn.execute(&*format!("TRUNCATE TABLE {table_name}"))
            .await
            .unwrap();
        for (i, value) in data.iter().enumerate() {
            conn.execute(&*format!(
                "INSERT INTO {table_name} (customer_id, value) VALUES ({i}, {value})"
            ))
            .await
            .unwrap();
        }
    }
}

// Recent download counts of the 100 most popular crates as of 6/9/2026
const TEST_DATA: &[i64] = &[
    450329134, 386527085, 348521531, 332945790, 294970090, 290857550, 284694702, 258336768,
    258154767, 254485026, 252769246, 243199426, 242367895, 241286917, 232310474, 232236770,
    226494798, 221473566, 217170662, 217052736, 214435414, 213336069, 212489430, 210461816,
    208312515, 208265727, 207526230, 207070213, 206852142, 204018630, 202697271, 202693425,
    201437650, 198936088, 198839308, 197719610, 195889462, 195825405, 194010171, 186849163,
    185483765, 179751005, 179532029, 176706766, 175848062, 175612917, 175462223, 174889323,
    174793400, 174323450, 174267292, 174090376, 173919330, 170916707, 167707451, 167402946,
    166634912, 165022096, 164671200, 163911642, 163867443, 163320764, 160070670, 160057150,
    159339559, 158731264, 158348991, 157187336, 157085072, 155603749, 155072980, 153651318,
    153417913, 153228552, 153140443, 153083644, 152126010, 151856999, 151682119, 151456161,
    151268966, 150857425, 150471859, 149072981, 148671911, 148463017, 146608931, 145662573,
    145380393, 145252714, 144730199, 144540340, 144471568, 144049728, 143598846, 143552812,
    143152356, 143111186, 140360342, 139857277,
];
// Recent download counts of 200 random crates, guaranteed to be chosen by
// a fair dice roll.
const TEST_DATA_2: &[i64] = &[
    1132, 112, 10, 4, 63, 538, 7035, 4, 5, 4, 10, 116, 339, 108, 56, 849, 481, 311, 85, 67, 28,
    157, 7, 13, 5, 8, 14, 5, 8, 7, 194, 59, 139, 4, 4, 130, 304, 7, 1229, 71, 53, 42, 136919727,
    1793, 102, 155, 7, 45, 1, 742, 240, 182, 717, 14, 237, 38073, 4, 4, 6, 20, 4520, 113, 6, 82940,
    218, 9, 10, 427, 52, 1560, 242791, 5, 437, 4, 518, 219, 7, 4, 5, 4848, 12, 25, 145, 7, 4, 5,
    193, 274, 16, 6361, 53, 8, 66, 42, 62, 8, 589, 11, 1, 48, 4, 1158, 328, 88, 330, 32, 13, 616,
    99, 4, 4, 67, 4, 12, 4, 130, 72, 7457, 323, 23, 4091, 120, 5, 51, 76, 382, 1100, 11608, 4,
    23062, 57, 4, 33, 4, 309, 10, 17, 51, 38, 12196, 159, 1061, 10, 14, 29, 147, 61, 21, 57, 316,
    57, 6, 120, 6, 346, 13, 9, 1596, 14, 14, 142684, 11, 5, 510, 370, 53, 3, 6, 5, 79897, 13, 22,
    20, 16, 68, 62, 1, 156, 11, 21740, 70, 1039, 24, 12, 4, 69, 8, 1, 5, 4, 151, 5, 9, 7, 29,
    696802, 48, 1363535, 4, 97,
];
