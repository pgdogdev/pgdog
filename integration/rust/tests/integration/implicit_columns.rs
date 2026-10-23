use crate::setup::{admin_sqlx, connection_sqlx_direct_db, connections_sqlx};
use sqlx::Executor;

#[tokio::test]
async fn test_omni_implicit_insert_columns() -> Result<(), Box<dyn std::error::Error>> {
    let shard_0 = connection_sqlx_direct_db("shard_0").await;
    let shard_1 = connection_sqlx_direct_db("shard_1").await;
    for shard in [&shard_0, &shard_1] {
        shard
            .execute(
                "DROP TABLE IF EXISTS public.omni_implicit_columns;
                 CREATE TABLE public.omni_implicit_columns (
                     value TEXT NOT NULL,
                     id BIGSERIAL PRIMARY KEY
                 )",
            )
            .await?;
    }
    let admin = admin_sqlx().await;
    admin
        .execute("SET rewrite_primary_key TO 'rewrite_omni'")
        .await?;
    let sharded = connections_sqlx().await.pop().expect("sharded pool");

    sharded
        .execute("INSERT INTO public.omni_implicit_columns VALUES ('supplied', 42)")
        .await?;
    sqlx::query("INSERT INTO public.omni_implicit_columns VALUES ($1, $2)")
        .bind("bound")
        .bind(43_i64)
        .execute(&sharded)
        .await?;
    sharded
        .execute("INSERT INTO public.omni_implicit_columns VALUES ('omitted')")
        .await?;
    sqlx::query("INSERT INTO public.omni_implicit_columns VALUES ($1, DEFAULT)")
        .bind("default")
        .execute(&sharded)
        .await?;

    let rows: Vec<(String, i64)> =
        sqlx::query_as("SELECT value, id FROM public.omni_implicit_columns ORDER BY value")
            .fetch_all(&shard_0)
            .await?;
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0], ("bound".into(), 43));
    assert_eq!(rows[3], ("supplied".into(), 42));
    assert!(rows[1].1 > 43 && rows[2].1 > 43);
    assert_ne!(rows[1].1, rows[2].1);
    let other: Vec<(String, i64)> =
        sqlx::query_as("SELECT value, id FROM public.omni_implicit_columns ORDER BY value")
            .fetch_all(&shard_1)
            .await?;
    assert_eq!(rows, other);

    for shard in [&shard_0, &shard_1] {
        shard
            .execute("DROP TABLE public.omni_implicit_columns")
            .await?;
    }
    sharded.close().await;
    admin
        .execute("SET rewrite_primary_key TO 'rewrite'")
        .await?;
    Ok(())
}
