use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, Utc};
use rand::{Rng, SeedableRng, rngs::StdRng};
use rust::setup::admin_sqlx;
use rust_decimal::Decimal;
use sqlx::{Executor, Row, postgres::PgPool, postgres::PgPoolOptions};

const TABLE: &str = "order_by_cross_shard_merge_test";

#[derive(Debug, Clone)]
struct TestRow {
    id: i64,
    v_text: String,
    v_varchar: String,
    v_bigint: i64,
    v_int: i32,
    v_numeric: Decimal,
    v_timestamp: NaiveDateTime,
    v_timestamptz: DateTime<Utc>,
    v_date: NaiveDate,
}

#[tokio::test]
async fn cross_shard_order_by_randomized_types() -> Result<(), Box<dyn std::error::Error>> {
    let sharded = sharded_pool().await;
    reset(&sharded).await?;

    let mut rng = StdRng::seed_from_u64(0xC0FFEE);
    let mut rows = vec![];

    // Insert deterministic random rows on each shard directly so each shard has data.
    for shard in [0, 1] {
        for i in 0..200_i64 {
            let id = (shard as i64) * 10_000 + i + 1;
            let ts = random_timestamp(&mut rng);
            let row = TestRow {
                id,
                v_text: format!("txt_{:04}", rng.random_range(0..250)),
                v_varchar: format!("varchar_{}_{}", rng.random_range(0..100), i % 11),
                v_bigint: rng.random_range(-5_000_000..5_000_000),
                v_int: rng.random_range(-100_000..100_000),
                v_numeric: Decimal::new(rng.random_range(-999_999_999..999_999_999), 3),
                v_timestamp: ts.naive_utc(),
                v_timestamptz: ts,
                v_date: ts.date_naive(),
            };

            insert_row(&sharded, shard, &row).await?;
            rows.push(row);
        }
    }

    let total_rows = sharded
        .fetch_one(format!("SELECT COUNT(*)::bigint AS n FROM {TABLE}").as_str())
        .await?
        .get::<i64, _>("n");
    assert_eq!(total_rows, rows.len() as i64);

    assert_sorted(&sharded, &rows, "ORDER BY v_text ASC, id ASC", |a, b| {
        a.v_text.cmp(&b.v_text).then(a.id.cmp(&b.id))
    })
    .await?;
    assert_sorted(&sharded, &rows, "ORDER BY v_text DESC, id DESC", |a, b| {
        b.v_text.cmp(&a.v_text).then(b.id.cmp(&a.id))
    })
    .await?;

    assert_sorted(&sharded, &rows, "ORDER BY v_varchar ASC, id ASC", |a, b| {
        a.v_varchar.cmp(&b.v_varchar).then(a.id.cmp(&b.id))
    })
    .await?;
    assert_sorted(
        &sharded,
        &rows,
        "ORDER BY v_varchar DESC, id DESC",
        |a, b| b.v_varchar.cmp(&a.v_varchar).then(b.id.cmp(&a.id)),
    )
    .await?;

    assert_sorted(&sharded, &rows, "ORDER BY v_bigint ASC, id ASC", |a, b| {
        a.v_bigint.cmp(&b.v_bigint).then(a.id.cmp(&b.id))
    })
    .await?;
    assert_sorted(
        &sharded,
        &rows,
        "ORDER BY v_bigint DESC, id DESC",
        |a, b| b.v_bigint.cmp(&a.v_bigint).then(b.id.cmp(&a.id)),
    )
    .await?;

    assert_sorted(&sharded, &rows, "ORDER BY v_int ASC, id ASC", |a, b| {
        a.v_int.cmp(&b.v_int).then(a.id.cmp(&b.id))
    })
    .await?;
    assert_sorted(&sharded, &rows, "ORDER BY v_int DESC, id DESC", |a, b| {
        b.v_int.cmp(&a.v_int).then(b.id.cmp(&a.id))
    })
    .await?;

    assert_sorted(&sharded, &rows, "ORDER BY v_numeric ASC, id ASC", |a, b| {
        a.v_numeric.cmp(&b.v_numeric).then(a.id.cmp(&b.id))
    })
    .await?;
    assert_sorted(
        &sharded,
        &rows,
        "ORDER BY v_numeric DESC, id DESC",
        |a, b| b.v_numeric.cmp(&a.v_numeric).then(b.id.cmp(&a.id)),
    )
    .await?;

    assert_sorted(
        &sharded,
        &rows,
        "ORDER BY v_timestamp ASC, id ASC",
        |a, b| a.v_timestamp.cmp(&b.v_timestamp).then(a.id.cmp(&b.id)),
    )
    .await?;
    assert_sorted(
        &sharded,
        &rows,
        "ORDER BY v_timestamp DESC, id DESC",
        |a, b| b.v_timestamp.cmp(&a.v_timestamp).then(b.id.cmp(&a.id)),
    )
    .await?;

    assert_sorted(
        &sharded,
        &rows,
        "ORDER BY v_timestamptz ASC, id ASC",
        |a, b| a.v_timestamptz.cmp(&b.v_timestamptz).then(a.id.cmp(&b.id)),
    )
    .await?;
    assert_sorted(
        &sharded,
        &rows,
        "ORDER BY v_timestamptz DESC, id DESC",
        |a, b| b.v_timestamptz.cmp(&a.v_timestamptz).then(b.id.cmp(&a.id)),
    )
    .await?;

    assert_sorted(&sharded, &rows, "ORDER BY v_date ASC, id ASC", |a, b| {
        a.v_date.cmp(&b.v_date).then(a.id.cmp(&b.id))
    })
    .await?;
    assert_sorted(&sharded, &rows, "ORDER BY v_date DESC, id DESC", |a, b| {
        b.v_date.cmp(&a.v_date).then(b.id.cmp(&a.id))
    })
    .await?;

    cleanup(&sharded).await;
    sharded.close().await;
    Ok(())
}

async fn assert_sorted(
    pool: &PgPool,
    rows: &[TestRow],
    order_by: &str,
    cmp: impl Fn(&TestRow, &TestRow) -> std::cmp::Ordering,
) -> Result<(), sqlx::Error> {
    // Include all sortable columns in the projection so ORDER BY keys are present
    // in RowDescription for PgDog's current cross-shard sorting/merge logic.
    let actual_ids: Vec<i64> = pool
        .fetch_all(
            format!(
                "SELECT id, v_text, v_varchar, v_bigint, v_int, v_numeric, v_timestamp, v_timestamptz, v_date FROM {TABLE} {order_by}"
            )
            .as_str(),
        )
        .await?
        .into_iter()
        .map(|r| r.get::<i64, _>("id"))
        .collect();

    let mut expected = rows.to_vec();
    expected.sort_by(cmp);
    let expected_ids: Vec<i64> = expected.into_iter().map(|r| r.id).collect();

    assert_eq!(
        actual_ids, expected_ids,
        "ORDER BY mismatch for query: SELECT id FROM {TABLE} {order_by}"
    );
    Ok(())
}

fn random_timestamp(rng: &mut StdRng) -> DateTime<Utc> {
    let base = NaiveDate::from_ymd_opt(2025, 1, 1)
        .unwrap()
        .and_hms_micro_opt(0, 0, 0, 0)
        .unwrap()
        .and_utc();
    base + Duration::seconds(rng.random_range(-5_000_000..5_000_000))
        + Duration::microseconds(rng.random_range(0..1_000_000))
}

async fn insert_row(pool: &PgPool, shard: usize, row: &TestRow) -> Result<(), sqlx::Error> {
    sqlx::query(
        format!(
            "/* pgdog_shard: {shard} */ \
             INSERT INTO {TABLE} \
             (id, v_text, v_varchar, v_bigint, v_int, v_numeric, v_timestamp, v_timestamptz, v_date, customer_id) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
        )
        .as_str(),
    )
    .bind(row.id)
    .bind(&row.v_text)
    .bind(&row.v_varchar)
    .bind(row.v_bigint)
    .bind(row.v_int)
    .bind(row.v_numeric)
    .bind(row.v_timestamp)
    .bind(row.v_timestamptz)
    .bind(row.v_date)
    .bind(row.id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn reset(pool: &PgPool) -> Result<(), sqlx::Error> {
    for shard in [0, 1] {
        pool.execute(format!("/* pgdog_shard: {shard} */ DROP TABLE IF EXISTS {TABLE}").as_str())
            .await
            .ok();
    }
    for shard in [0, 1] {
        pool.execute(
            format!(
                "/* pgdog_shard: {shard} */ \
                 CREATE TABLE {TABLE} (\
                   id BIGINT PRIMARY KEY, \
                   v_text TEXT COLLATE \"C\" NOT NULL, \
                   v_varchar VARCHAR(200) COLLATE \"C\" NOT NULL, \
                   v_bigint BIGINT NOT NULL, \
                   v_int INTEGER NOT NULL, \
                   v_numeric NUMERIC NOT NULL, \
                   v_timestamp TIMESTAMP NOT NULL, \
                   v_timestamptz TIMESTAMPTZ NOT NULL, \
                   v_date DATE NOT NULL, \
                   customer_id BIGINT\
                 )"
            )
            .as_str(),
        )
        .await?;
    }
    admin_sqlx().await.execute("RELOAD").await?;
    Ok(())
}

async fn cleanup(pool: &PgPool) {
    for shard in [0, 1] {
        pool.execute(format!("/* pgdog_shard: {shard} */ DROP TABLE IF EXISTS {TABLE}").as_str())
            .await
            .ok();
    }
}

async fn sharded_pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .connect(
            "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded?application_name=sqlx_order_by",
        )
        .await
        .unwrap()
}
