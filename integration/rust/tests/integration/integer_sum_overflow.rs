use crate::setup::{admin_sqlx, connection_sqlx_direct, connections_sqlx};
use sqlx::{Executor, Row};

#[tokio::test]
async fn integer_sum_bounds_across_shards() -> Result<(), Box<dyn std::error::Error>> {
    let connections = connections_sqlx().await;
    let sharded = &connections[1];
    let direct = connection_sqlx_direct().await;
    sharded
        .execute("DROP TABLE IF EXISTS integer_sum_overflow_test")
        .await?;
    sharded
        .execute("CREATE TABLE integer_sum_overflow_test (customer_id BIGINT, amount BIGINT)")
        .await?;
    admin_sqlx().await.execute("RELOAD").await?;

    let result = async {
        for (sql_type, minimum, maximum) in [
            ("smallint", i64::from(i16::MIN), i64::from(i16::MAX)),
            ("integer", i64::from(i32::MIN), i64::from(i32::MAX)),
            ("bigint", i64::MIN, i64::MAX),
        ] {
            for (left, right, expected) in [
                (maximum, 1, None),
                (minimum, -1, None),
                (maximum - 1, 1, Some(maximum)),
                (minimum + 1, -1, Some(minimum)),
                (maximum, minimum, Some(-1)),
            ] {
                sharded
                    .execute("TRUNCATE integer_sum_overflow_test")
                    .await?;
                for (shard, amount) in [(0, left), (1, right)] {
                    sharded
                        .execute(
                            format!(
                                "/* pgdog_shard: {shard} */ INSERT INTO integer_sum_overflow_test
                                 VALUES ({shard}, {amount})"
                            )
                            .as_str(),
                        )
                        .await?;
                }

                let query =
                    format!("SELECT SUM(amount)::{sql_type} FROM integer_sum_overflow_test");
                let reference = format!(
                    "SELECT SUM(amount)::{sql_type}
                     FROM (VALUES ({left}::bigint), ({right}::bigint)) AS sample(amount)"
                );
                for binary in [false, true] {
                    let actual = if binary {
                        sqlx::query(&query).fetch_one(sharded).await
                    } else {
                        sharded.fetch_one(query.as_str()).await
                    };
                    let postgres = if binary {
                        sqlx::query(&reference).fetch_one(&direct).await
                    } else {
                        direct.fetch_one(reference.as_str()).await
                    };
                    for row in [postgres, actual] {
                        if let Some(expected) = expected {
                            let row = row?;
                            let value = match sql_type {
                                "smallint" => i64::from(row.try_get::<i16, _>(0)?),
                                "integer" => i64::from(row.try_get::<i32, _>(0)?),
                                _ => row.try_get::<i64, _>(0)?,
                            };
                            assert_eq!(value, expected, "{sql_type}, binary={binary}");
                        } else {
                            let error = row.expect_err("integer sum must report overflow");
                            assert_eq!(
                                error
                                    .as_database_error()
                                    .and_then(|error| error.code())
                                    .as_deref(),
                                Some("22003"),
                                "{sql_type}, binary={binary}: {error}"
                            );
                        }
                    }
                    // The same one-connection pool must remain usable after an error.
                    let value: i32 = sqlx::query_scalar("SELECT 1").fetch_one(sharded).await?;
                    assert_eq!(value, 1);
                }
            }
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;

    sharded
        .execute("DROP TABLE integer_sum_overflow_test")
        .await?;
    result
}
