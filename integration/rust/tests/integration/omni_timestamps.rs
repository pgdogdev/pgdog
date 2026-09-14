use crate::setup::connection_sqlx_direct_db;
use crate::setup::connections_sqlx;
use chrono::DateTime;
use chrono::Duration;
use chrono::NaiveDateTime;
use chrono::Utc;
use chrono_tz::Tz;
use sqlx::PgTransaction;
use sqlx::Postgres;
use sqlx::Transaction;
use sqlx::postgres::PgRow;
use sqlx::{Executor, Row};

// TODO: Test raw postgres behavior against this for equivilence
// TODO: Other column types with the functions, e.g. text
// TODO: Test changing schema for a column while this is cached
// TODO: Test for other caching issues
// TODO: Test other functions (as well as present time vs transaction time vs statement time)
// TODO: Test to make sure this doesn't affect omnisharded tables (it doesn't; but doesn't hurt to assert that)
// TODO: Assert what happens if we don't explicitly set timezone

/// Re-usable harness for other tests (simple protocol, extended protocol, prepare/execute) to equally test if
/// different INSERT methods work correctly.
///
/// - Before running, it resets everything (re-create table on each shard)
/// - Tests with different timezones to ensure `timestamp` vs `timestamptz` works as intended;
///   more specifically, INSERT timezone is different from SELECT timezone.
/// - Tests DEFAULT schema.
/// - Tests functions within the VALUES list.
/// - Tests multiple VALUES lists (...), (....)
/// - Ensures time consistency across databases for the omnisharded column.
/// - Ensures consistency across columns within the same row (if multiple time funcs)
///
/// Does all of this within transactions, and after conclusion, performs a rollback.
async fn run_test<F>(perform_insert: F)
where
    F: AsyncFn(&mut PgTransaction),
{
    for conn in [
        connection_sqlx_direct_db("shard_0").await,
        connection_sqlx_direct_db("shard_1").await,
    ] {
        conn.execute("DROP TABLE IF EXISTS public.test_omni_ts")
            .await
            .unwrap();
        conn.execute("CREATE TABLE IF NOT EXISTS public.test_omni_ts(id BIGSERIAL PRIMARY KEY, created_at TIMESTAMP, created_at_tz TIMESTAMPTZ, created_at_default TIMESTAMP DEFAULT CURRENT_TIMESTAMP, created_at_tz_default TIMESTAMPTZ DEFAULT TRANSACTION_TIMESTAMP())").await.unwrap();
    }

    let conn = connections_sqlx().await;
    let db = conn.get(1).unwrap();

    for (insertion_tz, fetch_tz) in [
        ("America/Los_Angeles", "America/New_York"),
        ("America/New_York", "America/Los_Angeles"),
        // TODO: could also check equal
    ] {
        let mut sesh = db.begin().await.unwrap();

        sesh.execute(format!("SET TIME ZONE '{insertion_tz}'").as_str())
            .await
            .unwrap();

        perform_insert(&mut sesh).await;
        check_shards(&mut sesh, fetch_tz, insertion_tz).await;

        sesh.rollback().await.unwrap();
    }
}

/// Simple protocol case (see `run_test` for details)
#[tokio::test]
async fn omni_timestamp_rewrite_simple_protocol() {
    run_test(async |sesh| {
        // Routes to both (shard 0, shard 1) because it's omnisharded.
        sqlx::raw_sql(
                "INSERT INTO test_omni_ts(id, created_at, created_at_tz) VALUES (1, now(), now()), (2, now(), now())",
            )
            .execute(&mut **sesh)
            .await
            .unwrap();

        sqlx::raw_sql(
            "INSERT INTO test_omni_ts(id, created_at, created_at_tz) VALUES (3, now(), now())",
        )
        .execute(&mut **sesh)
        .await
        .unwrap();
    }).await;
}

/// Extended protocol case (see `run_test` for details)
#[tokio::test]
async fn omni_timestamp_rewrite_extended_protocol() {
    run_test(async |sesh| {
        // Routes to both (shard 0, shard 1) because it's omnisharded.
        sqlx::query(
                "INSERT INTO test_omni_ts(id, created_at, created_at_tz) VALUES ($1, NOW(), now()), ($2, TRANSACTION_TIMESTAMP(), CURRENT_TIMESTAMP)",
            )
            .bind(1).bind(2)
            .execute(&mut **sesh)
            .await
            .unwrap();

        sqlx::query(
            "INSERT INTO test_omni_ts(id, created_at, created_at_tz) VALUES ($1, now(), now())",
        )
        .bind(3)
        .execute(&mut **sesh)
        .await
        .unwrap();
    }).await;
}

/// Prepare/execute case (see `run_test` for details)
#[tokio::test]
async fn omni_timestamp_rewrite_prepare_execute() {
    run_test(async |sesh| {
        // Routes to both (shard 0, shard 1) because it's omnisharded.
        sqlx::raw_sql(
                "PREPARE stmt AS INSERT INTO test_omni_ts(id, created_at, created_at_tz) VALUES ($1, now(), now()), ($2, now(), now())",
            )
            .execute(&mut **sesh)
            .await
            .unwrap();

        sqlx::raw_sql(
            "PREPARE stmt2 AS INSERT INTO test_omni_ts(id, created_at, created_at_tz) VALUES ($1, now(), now())"
        ).execute(&mut **sesh).await.unwrap();

        sqlx::raw_sql("EXECUTE stmt(1, 2)")
            .execute(&mut **sesh)
            .await
            .unwrap();

        sqlx::raw_sql("EXECUTE stmt2(3)")
            .execute(&mut **sesh)
            .await
            .unwrap();
    }).await;
}

/// Doc comment.
async fn check_shards(sesh: &mut Transaction<'_, Postgres>, fetch_tz: &str, insertion_tz: &str) {
    let now = Utc::now();
    let (shard_0_rows, shard_1_rows) = fetch_rows_with_tz(sesh, fetch_tz).await;

    for (i, (shard_0_row, shard_1_row)) in shard_0_rows.iter().zip(&shard_1_rows).enumerate() {
        println!("Iteration #{i}");
        assert_timestamp_col_validity(
            shard_0_row,
            shard_1_row,
            ColumnType::ByDefault,
            &now,
            insertion_tz,
        )
        .await;
        assert_timestamp_tz_col_validity(shard_0_row, shard_1_row, ColumnType::ByDefault, &now)
            .await;

        assert_timestamp_col_validity(
            shard_0_row,
            shard_1_row,
            ColumnType::Regular,
            &now,
            insertion_tz,
        )
        .await;
        assert_timestamp_tz_col_validity(shard_0_row, shard_1_row, ColumnType::Regular, &now).await;

        // Same shard time equality in same column?
        {
            let created_at = shard_0_row.get::<DateTime<Utc>, &str>("created_at_tz");
            let created_at_default =
                shard_0_row.get::<DateTime<Utc>, &str>("created_at_tz_default");

            assert_eq!(created_at, created_at_default);
        }
    }

    {
        // Same INSERT: VALUES (...), (...)
        // now() should be the same.
        let shard_0_row_1 = shard_0_rows.first().unwrap();
        let shard_0_row_2 = shard_0_rows.get(1).unwrap();

        let created_at_first_insert = shard_0_row_1.get::<DateTime<Utc>, &str>("created_at_tz");
        let created_at_second_insert = shard_0_row_2.get::<DateTime<Utc>, &str>("created_at_tz");

        assert_eq!(created_at_first_insert, created_at_second_insert);
    }

    {
        // Added in same transaction. Separate INSERTs.
        // now() should be the same.
        let shard_0_row_1 = shard_0_rows.first().unwrap();
        let shard_0_row_3 = shard_0_rows.get(2).unwrap();

        let created_at_first_insert = shard_0_row_1.get::<DateTime<Utc>, &str>("created_at_tz");
        let created_at_second_insert = shard_0_row_3.get::<DateTime<Utc>, &str>("created_at_tz");

        assert_eq!(created_at_first_insert, created_at_second_insert);
    }
}

enum ColumnType {
    /// Col has DEFAULT in table schema.
    ByDefault,
    /// Function called and specified explicitly in VALUES list for the INSERT
    Regular,
}

/// These will be the same (based on UTC) regardless of being inserted/fetched in different timezones.
async fn assert_timestamp_tz_col_validity(
    shard_0_row: &PgRow,
    shard_1_row: &PgRow,
    col_type: ColumnType,
    now: &DateTime<Utc>,
) {
    let created_at_tz_col = match col_type {
        ColumnType::ByDefault => "created_at_tz_default",
        ColumnType::Regular => "created_at_tz",
    };

    let first = shard_0_row.get::<DateTime<Utc>, &str>(created_at_tz_col);
    let second = shard_1_row.get::<DateTime<Utc>, &str>(created_at_tz_col);

    // Cross-shard equality?
    assert_eq!(first, second);

    // Equal to UTC?
    assert!(*now > first);
    assert!((*now - first) < Duration::seconds(5));
}

/// `fetch_time_zone` will differ from `insert_time_zone` with timestamp column (created_at)
/// This is because offset information is stripped when inserted in Postgres.
async fn assert_timestamp_col_validity(
    shard_0_row: &PgRow,
    shard_1_row: &PgRow,
    col_type: ColumnType,
    now: &DateTime<Utc>,
    insertion_tz: &str,
) {
    let created_at_col = match col_type {
        ColumnType::ByDefault => "created_at_default",
        ColumnType::Regular => "created_at",
    };

    let first = shard_0_row.get::<NaiveDateTime, &str>(created_at_col);
    let second = shard_1_row.get::<NaiveDateTime, &str>(created_at_col);

    // Cross-shard equality?
    assert_eq!(first, second);

    // Local time in LA and NY.
    let (los_angeles_time, new_york_time) = (
        now.with_timezone(&Tz::America__Los_Angeles).naive_local(),
        now.with_timezone(&Tz::America__New_York).naive_local(),
    );

    let (la_time_diff, ny_time_diff) = (
        (los_angeles_time - first).abs(),
        (new_york_time - first).abs(),
    );

    if insertion_tz.eq("America/Los_Angeles") {
        // insert LA tz, fetch NY tz
        assert!(ny_time_diff > Duration::hours(2) && ny_time_diff < Duration::hours(4));
        assert!(la_time_diff < Duration::seconds(5));
    } else {
        // insert NY tz, fetch LA tz
        assert!(la_time_diff > Duration::hours(2) && la_time_diff < Duration::hours(4));
        assert!(ny_time_diff < Duration::seconds(5));
    }
}

/// TODO: Docs
async fn fetch_rows_with_tz(
    sesh: &mut Transaction<'_, Postgres>,
    fetch_tz: &str,
) -> (Vec<PgRow>, Vec<PgRow>) {
    sesh.execute(format!("SET TIME ZONE '{fetch_tz}'").as_str())
        .await
        .unwrap();

    // Force to route to the individual shards to ensure no divergence.
    (
        sesh.fetch_all(
            "/* pgdog_shard: 0 */ SELECT * FROM public.test_omni_ts WHERE id IN (1, 2, 3)",
        )
        .await
        .unwrap(),
        sesh.fetch_all(
            "/* pgdog_shard: 1 */ SELECT * FROM public.test_omni_ts WHERE id IN (1, 2, 3)",
        )
        .await
        .unwrap(),
    )
}
