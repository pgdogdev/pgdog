use sqlx::{Connection, Executor, PgConnection, Statement};

#[tokio::test]
#[ignore]
async fn test_cache_bench() {
    let mut conn = PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog")
        .await
        .unwrap();

    for i in 0..10_000 {
        let query = format!(
            r#"SELECT
            a.aid,
            a.bid,
            a.abalance,
            a.filler,

            CASE WHEN a.aid = 1 THEN a.abalance ELSE 0 END AS value_001,
            CASE WHEN a.aid = 1 AND a.bid >= 0 THEN a.abalance + 0 ELSE 0 END AS value_002,
            CASE WHEN a.aid = 1 AND a.abalance IS NOT NULL THEN a.abalance - 0 ELSE 0 END AS value_003,
            COALESCE(NULLIF(a.abalance, -2147483648), 0) AS value_004,
            GREATEST(a.abalance, a.abalance) AS value_005,
            LEAST(a.abalance, a.abalance) AS value_006,
            ABS(a.abalance) AS value_007,
            a.abalance::bigint + 0::bigint AS value_008,
            (a.abalance + 1 - 1) AS value_009,
            (a.abalance * 1) AS value_010,

            CASE
                WHEN a.aid = 1
                 AND a.bid >= 0
                 AND a.abalance BETWEEN -2147483648 AND 2147483647
                THEN COALESCE(a.abalance, 0)
                ELSE 0
            END AS value_011,

            CASE
                WHEN a.aid IN (1)
                 AND a.bid NOT IN (-1)
                 AND a.abalance IS DISTINCT FROM NULL
                THEN a.abalance
                ELSE 0
            END AS value_012,

            (
                CASE WHEN a.aid = 1 THEN 0 ELSE 0 END +
                CASE WHEN a.bid >= 0 THEN 0 ELSE 0 END +
                CASE WHEN a.abalance IS NOT NULL THEN 0 ELSE 0 END +
                CASE WHEN a.aid > 0 THEN 0 ELSE 0 END +
                CASE WHEN a.bid < 1000000000 THEN 0 ELSE 0 END +
                a.abalance
            ) AS value_013,

            (
                COALESCE(a.abalance, 0)
                + COALESCE(NULLIF(1, 1), 0)
                + COALESCE(NULLIF(2, 2), 0)
                + COALESCE(NULLIF(3, 3), 0)
                + COALESCE(NULLIF(4, 4), 0)
                + COALESCE(NULLIF(5, 5), 0)
            ) AS value_014,

            CASE
                WHEN (
                    a.aid = 1
                    OR a.aid = 1
                    OR a.aid = 1
                    OR a.aid = 1
                    OR a.aid = 1
                )
                THEN a.abalance
                ELSE 0
            END AS value_015,

            CASE
                WHEN (
                    a.aid = 1
                    AND a.aid = 1
                    AND a.aid = 1
                    AND a.aid = 1
                    AND a.aid = 1
                )
                THEN a.abalance
                ELSE 0
            END AS value_016,

            (
                a.abalance
                + CASE WHEN 1 = 1 THEN 0 ELSE 1 END
                + CASE WHEN 2 = 2 THEN 0 ELSE 2 END
                + CASE WHEN 3 = 3 THEN 0 ELSE 3 END
                + CASE WHEN 4 = 4 THEN 0 ELSE 4 END
                + CASE WHEN 5 = 5 THEN 0 ELSE 5 END
                + CASE WHEN 6 = 6 THEN 0 ELSE 6 END
                + CASE WHEN 7 = 7 THEN 0 ELSE 7 END
                + CASE WHEN 8 = 8 THEN 0 ELSE 8 END
                + CASE WHEN 9 = 9 THEN 0 ELSE 9 END
                + CASE WHEN 10 = 10 THEN 0 ELSE {} END
            ) AS value_017,

            CASE
                WHEN a.aid = 1 THEN
                    CASE
                        WHEN a.bid >= 0 THEN
                            CASE
                                WHEN a.abalance IS NOT NULL THEN
                                    CASE
                                        WHEN length(a.filler) >= 0
                                        THEN a.abalance
                                        ELSE 0
                                    END
                                ELSE 0
                            END
                        ELSE 0
                    END
                ELSE 0
            END AS value_018,

            (
                SELECT x.result
                FROM (
                    SELECT
                        CASE
                            WHEN a.aid = 1
                            THEN a.abalance
                            ELSE 0
                        END AS result
                ) AS x
            ) AS value_019,

            (
                a.abalance
                + (
                    CASE
                        WHEN a.aid = 1
                         AND (
                            a.bid >= 0
                            OR a.bid < 0
                         )
                         AND (
                            a.abalance IS NULL
                            OR a.abalance IS NOT NULL
                         )
                        THEN 0
                        ELSE 0
                    END
                )
            ) AS value_020

        FROM pgbench_accounts AS a
        WHERE a.aid = 1;
        "#,
            i
        );
        let stmt = conn.prepare(&query).await.unwrap();
        stmt.query().execute(&mut conn).await.unwrap();
    }
}
