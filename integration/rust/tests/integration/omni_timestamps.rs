use std::ops::Sub;

use crate::setup::admin_sqlx;
use crate::setup::connection_sqlx_direct_db;
use crate::setup::connections_sqlx;
use chrono::DateTime;
use chrono::Duration;
use chrono::FixedOffset;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use chrono::NaiveTime;
use chrono::Utc;
use chrono_tz::Tz;
use sqlx::PgTransaction;
use sqlx::Pool;
use sqlx::Postgres;
use sqlx::Transaction;
use sqlx::postgres::PgRow;
use sqlx::postgres::types::PgTimeTz;
use sqlx::{Executor, Row};

// TODO: Test changing schema for a column while this is cached
// TODO: Test for other caching issues
// TODO: Test to make sure this doesn't affect harded tables (it doesn't; but doesn't hurt to assert that)
// TODO: Assert what happens if we don't explicitly set timezone

/// LOCAL_TIME testing
/// - Case 1: `test_time_text` has no DEFAULT w/ precision arg & text col.
/// - Case 2: `test_time_regular` has DEFAULT w/ no precision arg & time col.
///
/// Tests against NYC timezone.
#[tokio::test]
async fn omni_timestamp_rewrite_local_time() {
    let schema = "test_time_text text, test_time_regular time DEFAULT LOCALTIME";
    let insertion_col = "test_time_text";
    let func_call = "LOCALTIME(3)";

    reusable_func_test(schema, insertion_col, func_call, async |pg_row, dog_row| {
        assert_text_format(pg_row, dog_row, insertion_col, |s| {
            NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
        })
        .await;
        assert_equality(
            pg_row,
            dog_row,
            "test_time_regular",
            Duration::seconds(5),
            |t: NaiveTime| t,
        )
        .await;
    })
    .await;
}

/// CURRENT_TIME testing
/// - Case 1: `test_time_text` has no DEFAULT w/ precision arg & text col.
/// - Case 2: `test_time_regular` has DEFAULT w/ no precision arg & timetz col.
#[tokio::test]
async fn omni_timestamp_rewrite_current_time() {
    let schema = "test_time_text text, test_time_regular timetz DEFAULT CURRENT_TIME";
    let insertion_col = "test_time_text";
    let func_call = "CURRENT_TIME(3)";

    reusable_func_test(schema, insertion_col, func_call, async |pg_row, dog_row| {
        assert_text_format(pg_row, dog_row, insertion_col, |s| {
            NaiveTime::parse_from_str(s, "%H:%M:%S%.f%#z")
        })
        .await;
        assert_equality(
            pg_row,
            dog_row,
            "test_time_regular",
            Duration::seconds(5),
            |t: PgTimeTz<NaiveTime, FixedOffset>| t.time - t.offset,
        )
        .await;
    })
    .await;
}

/// CURRENT_DATE testing
/// - Case 1: `test_date_text` has no DEFAULT and it uses text col.
/// - Case 2: `test_date_regular` has DEFAULT and it uses date col.
#[tokio::test]
async fn omni_timestamp_rewrite_current_date() {
    let schema = "test_date_text text, test_date_regular date DEFAULT CURRENT_DATE";
    let insertion_col = "test_date_text";
    let func_call = "CURRENT_DATE";

    reusable_func_test(schema, insertion_col, func_call, async |pg_row, dog_row| {
        assert_text_format(pg_row, dog_row, insertion_col, |s| {
            NaiveDate::parse_from_str(s, "%Y-%m-%d")
        })
        .await;

        assert_equality(
            pg_row,
            dog_row,
            "test_date_regular",
            Duration::hours(25),
            |d: NaiveDate| d,
        )
        .await;
    })
    .await;
}

/// LOCALTIMESTAMP testing
/// - Case 1: `test_timestamp_text` has no DEFAULT w/ precision arg & text col.
/// - Case 2: `test_timestamp_regular` has DEFAULT w/ no precision arg & timestamp col.
#[tokio::test]
async fn omni_timestamp_rewrite_local_timestamp() {
    let schema =
        "test_timestamp_text text, test_timestamp_regular timestamp DEFAULT LOCALTIMESTAMP";
    let insertion_col = "test_timestamp_text";
    let func_call = "LOCALTIMESTAMP(2)";

    reusable_func_test(schema, insertion_col, func_call, async |pg_row, dog_row| {
        assert_text_format(pg_row, dog_row, insertion_col, |s| {
            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        })
        .await;
        assert_equality(
            pg_row,
            dog_row,
            "test_timestamp_regular",
            Duration::seconds(5),
            |t: NaiveDateTime| t,
        )
        .await;
    })
    .await;
}

/// clock_timestamp() testing
/// - Case 1: `test_clock_text` has no DEFAULT & text col.
/// - Case 2: `test_clock_regular` has DEFAULT & timestamptz col.
#[tokio::test]
async fn omni_timestamp_rewrite_clock_timestamp() {
    let schema = "test_clock_text text, test_clock_regular timestamptz DEFAULT clock_timestamp()";
    let insertion_col = "test_clock_text";
    let func_call = "clock_timestamp()";

    reusable_func_test(schema, insertion_col, func_call, async |pg_row, dog_row| {
        assert_text_format(pg_row, dog_row, insertion_col, |s| {
            DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z")
        })
        .await;
        assert_equality(
            pg_row,
            dog_row,
            "test_clock_regular",
            Duration::seconds(5),
            |t: DateTime<Utc>| t,
        )
        .await;
    })
    .await;
}

/// statement_timestamp() testing
/// - Case 1: `test_statement_text` has no DEFAULT & text col.
/// - Case 2: `test_statement_regular` has DEFAULT & timestamptz col.
#[tokio::test]
async fn omni_timestamp_rewrite_statement_timestamp() {
    let schema = "test_statement_text text, test_statement_regular timestamptz DEFAULT statement_timestamp()";
    let insertion_col = "test_statement_text";
    let func_call = "statement_timestamp()";

    reusable_func_test(schema, insertion_col, func_call, async |pg_row, dog_row| {
        assert_text_format(pg_row, dog_row, insertion_col, |s| {
            DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z")
        })
        .await;
        assert_equality(
            pg_row,
            dog_row,
            "test_statement_regular",
            Duration::seconds(5),
            |t: DateTime<Utc>| t,
        )
        .await;
    })
    .await;
}

/// statement_timestamp() = same for every value in one statement (explicit and DEFAULT, across rows)
/// However it changes between statements in the same transaction.
#[tokio::test]
async fn omni_timestamp_rewrite_statement_timestamp_consistency() {
    let conn = connections_sqlx().await;
    let conn = conn.get(1).unwrap();
    conn.execute("DROP TABLE IF EXISTS dummy_omni_table")
        .await
        .unwrap();
    conn.execute(
        "CREATE TABLE dummy_omni_table(id BIGSERIAL PRIMARY KEY, explicit timestamptz, by_default timestamptz DEFAULT statement_timestamp())",
    )
    .await
    .unwrap();
    admin_sqlx().await.execute("RELOAD").await.unwrap();

    let mut transaction = conn.begin().await.unwrap();

    let first_rows = sqlx::query(
        "INSERT INTO dummy_omni_table(id, explicit) VALUES ($1, statement_timestamp()), ($2, statement_timestamp()) RETURNING *",
    )
    .bind(1)
    .bind(2)
    .fetch_all(&mut *transaction)
    .await
    .unwrap();

    let second_row = sqlx::query(
        "INSERT INTO dummy_omni_table(id, explicit) VALUES ($1, statement_timestamp()) RETURNING *",
    )
    .bind(3)
    .fetch_one(&mut *transaction)
    .await
    .unwrap();

    transaction.rollback().await.unwrap();
    conn.execute("DROP TABLE dummy_omni_table").await.unwrap();

    let times = |row: &PgRow| {
        (
            row.get::<DateTime<Utc>, _>("explicit"),
            row.get::<DateTime<Utc>, _>("by_default"),
        )
    };

    let (row_1_explicit, row_1_default) = times(&first_rows[0]);
    let (row_2_explicit, row_2_default) = times(&first_rows[1]);
    let (row_3_explicit, _) = times(&second_row);

    assert_eq!(row_1_explicit, row_2_explicit);
    assert_eq!(row_1_explicit, row_1_default);
    assert_eq!(row_1_default, row_2_default);
    assert_ne!(row_1_explicit, row_3_explicit);
}

/// timeofday() testing
/// - Case 1: `test_timeofday_text` has no DEFAULT & text col.
#[tokio::test]
async fn omni_timestamp_rewrite_time_of_day() {
    const TIME_OF_DAY_FORMAT: &str = "%a %b %d %H:%M:%S%.f %Y %Z";

    let schema = "test_timeofday_text text";
    let insertion_col = "test_timeofday_text";
    let func_call = "timeofday()";

    reusable_func_test(schema, insertion_col, func_call, async |pg_row, dog_row| {
        assert_text_format(pg_row, dog_row, insertion_col, |s| {
            NaiveDateTime::parse_from_str(s, TIME_OF_DAY_FORMAT)
        })
        .await;
        assert_equality(
            pg_row,
            dog_row,
            insertion_col,
            Duration::seconds(5),
            |s: String| NaiveDateTime::parse_from_str(&s, TIME_OF_DAY_FORMAT).unwrap(),
        )
        .await;
    })
    .await;
}

/// Asserts, after normalization, that the value for `col_name` for `pg_row` and `dog_row` are within
/// the bound of `acceptable_diff`
async fn assert_equality<T, U>(
    pg_row: &PgRow,
    dog_row: &PgRow,
    col_name: &str,
    acceptable_diff: Duration,
    normalize: impl Fn(T) -> U,
) where
    T: for<'r> sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
    U: Sub<Output = Duration>,
{
    let (pg_value, dog_value) = (
        normalize(pg_row.get::<T, _>(col_name)),
        normalize(dog_row.get::<T, _>(col_name)),
    );

    assert!((pg_value - dog_value).abs() < acceptable_diff);
}

/// Asserts that the parsed value for `col_name` from `pg_row` and `dog_row` both work;
/// proving that both (Postgres and PgDog) work the same, and that both are correctly formatted.
async fn assert_text_format<T>(
    pg_row: &PgRow,
    dog_row: &PgRow,
    col_name: &str,
    parse: impl Fn(&str) -> chrono::ParseResult<T>,
) {
    let (pg_text, dog_text) = (
        pg_row.get::<&str, &str>(col_name),
        dog_row.get::<&str, &str>(col_name),
    );

    assert!(parse(pg_text).is_ok());
    assert!(parse(dog_text).is_ok());
}

/// Inserts 3 rows
/// - Row 1: Simple
/// - Row 2: Extended
/// - Row 3: Prepare / Execute
///
///  Uses RETURNING * on each, and returns all the PgRows for analysis.
async fn test_simple_extended_and_prepare(
    conn: &Pool<Postgres>,
    cols: &str,
    vals: &str,
) -> Vec<PgRow> {
    // TODO: Could be useful to test BOTH UTC and NYC.
    let mut transaction = conn.begin().await.unwrap();

    // Allows us to test things like local time (instead of everything being UTC)
    transaction
        .execute("SET TIME ZONE 'America/New_York'")
        .await
        .unwrap();

    let row1 = sqlx::raw_sql(
        format!("INSERT INTO dummy_omni_table(id, {cols}) VALUES (1, {vals}) RETURNING *").as_str(),
    )
    .fetch_one(&mut *transaction)
    .await
    .unwrap();

    // By putting the Bind parameter first, it forces us to use the Binary text format, testing vs the already tested String.
    let row2 = sqlx::query(
        format!("INSERT INTO dummy_omni_table(id, {cols}) VALUES ($1, {vals}) RETURNING *")
            .as_str(),
    )
    .bind(2)
    .fetch_one(&mut *transaction)
    .await
    .unwrap();

    sqlx::raw_sql(
        format!(
            "PREPARE stmt AS INSERT INTO dummy_omni_table(id, {cols}) VALUES ($1, {vals}) RETURNING *"
        )
        .as_str(),
    )
    .execute(&mut *transaction)
    .await
    .unwrap();

    let row3 = sqlx::raw_sql("EXECUTE stmt(3)")
        .fetch_one(&mut *transaction)
        .await
        .unwrap();

    transaction.rollback().await.unwrap();

    vec![row1, row2, row3]
}

async fn reusable_func_test(
    schema: &str,
    insertion_col: &str,
    func_call: &str,
    validate: impl AsyncFn(&PgRow, &PgRow),
) {
    let conn = connections_sqlx().await;
    let conn = conn.get(1).unwrap();
    conn.execute("DROP TABLE IF EXISTS dummy_omni_table")
        .await
        .unwrap();
    conn.execute(
        format!("CREATE TABLE IF NOT EXISTS dummy_omni_table(id BIGSERIAL PRIMARY KEY, {schema})")
            .as_str(),
    )
    .await
    .unwrap();
    admin_sqlx().await.execute("RELOAD").await.unwrap();

    let pg_rows = test_simple_extended_and_prepare(
        &connection_sqlx_direct_db("shard_0").await,
        insertion_col,
        func_call,
    )
    .await;

    let dog_rows = test_simple_extended_and_prepare(
        connections_sqlx().await.get(1).unwrap(),
        insertion_col,
        func_call,
    )
    .await;

    for (pg_row, dog_row) in pg_rows.iter().zip(&dog_rows) {
        validate(pg_row, dog_row).await;
    }

    conn.execute("DROP TABLE dummy_omni_table").await.unwrap();
}

/// NOTE: The tests below assert that everything is intercepted and handled; therefore, I didn't try to mimic that in the above tests,
///       given that they share a re-usable abstraction (would be redundant)
///
/// Re-usable harness for other tests (simple protocol, extended protocol, prepare/execute) to equally test if
/// different INSERT methods work correctly.
///
/// - Before running, it resets everything (re-create table on each shard)
/// - Tests with different timezones to ensure `timestamp` vs `timestamptz` works as intended;
///   more specifically, INSERT timezone is different from SELECT timezone.
/// - Tests DEFAULT schema (both implicit and explicit)
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
    let conn = connections_sqlx().await;
    let conn = conn.get(1).unwrap();
    conn.execute("DROP TABLE IF EXISTS public.test_omni_ts")
        .await
        .unwrap();
    conn.execute("CREATE TABLE IF NOT EXISTS public.test_omni_ts(id BIGSERIAL PRIMARY KEY, created_at TIMESTAMP, created_at_tz TIMESTAMPTZ, created_at_default TIMESTAMP DEFAULT CURRENT_TIMESTAMP, created_at_tz_default TIMESTAMPTZ DEFAULT TRANSACTION_TIMESTAMP())").await.unwrap();
    admin_sqlx().await.execute("RELOAD").await.unwrap();

    for (insertion_tz, fetch_tz) in [
        ("America/Los_Angeles", "America/New_York"),
        ("America/New_York", "America/Los_Angeles"),
        // TODO: could also check equal
    ] {
        let mut sesh = conn.begin().await.unwrap();

        sesh.execute(format!("SET TIME ZONE '{insertion_tz}'").as_str())
            .await
            .unwrap();

        perform_insert(&mut sesh).await;
        check_shards(&mut sesh, fetch_tz, insertion_tz).await;

        sesh.rollback().await.unwrap();
    }

    conn.execute("DROP TABLE public.test_omni_ts")
        .await
        .unwrap();
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

        sqlx::raw_sql(
            "INSERT INTO test_omni_ts(id, created_at, created_at_tz, created_at_default, created_at_tz_default) VALUES (4, now(), now(), DEFAULT, DEFAULT)",
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

        sqlx::query(
            "INSERT INTO test_omni_ts(id, created_at, created_at_tz, created_at_default, created_at_tz_default) VALUES ($1, now(), now(), DEFAULT, DEFAULT)",
        )
        .bind(4)
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

        sqlx::raw_sql(
            "PREPARE stmt3 AS INSERT INTO test_omni_ts(id, created_at, created_at_tz, created_at_default, created_at_tz_default) VALUES ($1, now(), now(), DEFAULT, DEFAULT)"
        ).execute(&mut **sesh).await.unwrap();

        sqlx::raw_sql("EXECUTE stmt(1, 2)")
            .execute(&mut **sesh)
            .await
            .unwrap();

        sqlx::raw_sql("EXECUTE stmt2(3)")
            .execute(&mut **sesh)
            .await
            .unwrap();


        sqlx::raw_sql("EXECUTE stmt3(4)")
            .execute(&mut **sesh)
            .await
            .unwrap();
    }).await;
}

/// TODO: Doc comment.
async fn check_shards(sesh: &mut Transaction<'_, Postgres>, fetch_tz: &str, insertion_tz: &str) {
    let now = Utc::now();
    let (shard_0_rows, shard_1_rows) = fetch_rows_with_tz(sesh, fetch_tz).await;

    for (shard_0_row, shard_1_row) in shard_0_rows.iter().zip(&shard_1_rows) {
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
            "/* pgdog_shard: 0 */ SELECT * FROM public.test_omni_ts WHERE id IN (1, 2, 3, 4)",
        )
        .await
        .unwrap(),
        sesh.fetch_all(
            "/* pgdog_shard: 1 */ SELECT * FROM public.test_omni_ts WHERE id IN (1, 2, 3, 4)",
        )
        .await
        .unwrap(),
    )
}
