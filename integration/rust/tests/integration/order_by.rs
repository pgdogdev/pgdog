use crate::setup::connections_sqlx;
use sqlx::{Column, Executor, Row, postgres::PgRow};

enum Protocol {
    Simple,
    Extended,
}

#[tokio::test]
async fn order_by_case_simple_protocol() -> Result<(), Box<dyn std::error::Error>> {
    check_order_by_case(Protocol::Simple).await
}

#[tokio::test]
async fn order_by_case_extended_protocol() -> Result<(), Box<dyn std::error::Error>> {
    check_order_by_case(Protocol::Extended).await
}

async fn check_order_by_case(protocol: Protocol) -> Result<(), Box<dyn std::error::Error>> {
    let pools = connections_sqlx().await;
    let mut transaction = pools[1].begin().await?;

    transaction.execute("TRUNCATE sharded").await?;
    // Interleave priorities and IDs across shards so neither shard-local
    // ordering nor sorting only by ID can produce the expected result.
    for (shard, values) in [
        (0, "(1, 'other'), (4, 'pay'), (6, 'later')"),
        (1, "(2, 'pay'), (3, 'other'), (5, 'pay')"),
    ] {
        let inserted = transaction
            .execute(
                format!(
                    "/* pgdog_shard: {shard} */ INSERT INTO sharded (id, value, enabled)
                     SELECT batch * 6 + fixture.id, fixture.value, batch % 2 = 0
                     FROM generate_series(0, 9) AS batch
                     CROSS JOIN (VALUES {values}) AS fixture(id, value)"
                )
                .as_str(),
            )
            .await?;
        assert_eq!(inserted.rows_affected(), 30);
    }

    let ascending: Vec<_> = (0_i64..10)
        .flat_map(|batch| [2, 4, 5].map(|id| (batch * 6 + id, "pay")))
        .chain((0_i64..10).flat_map(|batch| {
            [(1, "other"), (3, "other"), (6, "later")].map(|(id, name)| (batch * 6 + id, name))
        }))
        .collect();
    let descending: Vec<_> = (0_i64..10)
        .rev()
        .flat_map(|batch| [3, 1].map(|id| (batch * 6 + id, "other")))
        .chain((0_i64..10).rev().map(|batch| (batch * 6 + 6, "later")))
        .chain(
            (0_i64..10)
                .rev()
                .flat_map(|batch| [5, 4, 2].map(|id| (batch * 6 + id, "pay"))),
        )
        .collect();

    let grouped: Vec<_> = ascending
        .iter()
        .copied()
        .filter(|(id, _)| ((id - 1) / 6) % 2 == 0)
        .chain(
            ascending
                .iter()
                .copied()
                .filter(|(id, _)| ((id - 1) / 6) % 2 == 1),
        )
        .collect();

    let cases = [
        ("CASE value WHEN $1 THEN 0 ELSE 1 END, id", ascending),
        (
            "CASE value WHEN $1 THEN 0 ELSE 1 END DESC,
             CASE value WHEN 'other' THEN 0 ELSE 1 END, id DESC",
            descending,
        ),
        (
            "enabled DESC, CASE value WHEN $1 THEN 0 ELSE 1 END, id",
            grouped,
        ),
    ];

    for (order_by, expected) in &cases {
        let query = format!("SELECT id, value AS name FROM sharded ORDER BY {order_by}");
        let rows = match protocol {
            Protocol::Simple => {
                sqlx::raw_sql(&query.replace("$1", "'pay'"))
                    .fetch_all(&mut *transaction)
                    .await?
            }
            Protocol::Extended => {
                sqlx::query(&query)
                    .bind("pay")
                    .fetch_all(&mut *transaction)
                    .await?
            }
        };
        assert_rows(&rows, expected);
    }

    transaction.rollback().await?;
    Ok(())
}

fn assert_rows(rows: &[PgRow], expected: &[(i64, &str)]) {
    assert_eq!(rows.len(), expected.len());
    for row in rows {
        assert_eq!(row.columns().len(), 2, "sort helpers must stay hidden");
        assert_eq!(row.columns()[0].name(), "id");
        assert_eq!(row.columns()[1].name(), "name");
    }
    let actual: Vec<_> = rows
        .iter()
        .map(|row| (row.get::<i64, _>(0), row.get::<&str, _>(1)))
        .collect();
    assert_eq!(actual, expected);
}
