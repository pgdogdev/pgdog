use rust::setup::*;
use sqlx::Executor;

#[tokio::test]
async fn test_omni_only_pk_rewrite() {
    let shard_0 = connection_sqlx_direct_db("shard_0").await;
    let shard_1 = connection_sqlx_direct_db("shard_1").await;

    for (shard, pool) in [&shard_0, &shard_1].iter().enumerate() {
        pool.execute(
            "DROP TABLE IF EXISTS public.test_omni_rewrite_pk_omni, test_omni_rewrite_pk_sharded CASCADE",
        )
        .await
        .unwrap();

        pool.execute("CREATE TABLE IF NOT EXISTS public.test_omni_rewrite_pk_omni(id BIGSERIAL PRIMARY KEY, value TEXT NOT NULL)").await.unwrap();
        pool.execute("CREATE TABLE IF NOT EXISTS public.test_omni_rewrite_pk_sharded(id BIGSERIAL PRIMARY KEY, customer_id BIGINT NOT NULL, value TEXT NOT NULL)").await.unwrap();

        pool.execute(
            "SELECT pgdog.install_shadow_table('public', 'test_omni_rewrite_pk_sharded', 'id')",
        )
        .await
        .unwrap();

        // Configure sharding.
        let mut t = pool.begin().await.unwrap();
        t.execute("DELETE FROM pgdog.config").await.unwrap();
        t.execute(
            format!(
                "INSERT INTO pgdog.config (shard, shards) VALUES ({}, 2)",
                shard
            )
            .as_str(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
    }

    let sharded = connections_sqlx().await.pop().unwrap();
    let admin = admin_sqlx().await;

    // This will reload the schema as well.
    admin
        .execute("SET rewrite_primary_key TO 'rewrite_omni'")
        .await
        .unwrap();

    let starting_id: (i64,) = sqlx::query_as("SELECT pgdog.unique_id()")
        .fetch_one(&sharded)
        .await
        .unwrap();

    for run in 0..25 {
        let omni_id: (i64,) = sqlx::query_as(
            "INSERT INTO public.test_omni_rewrite_pk_omni (value) VALUES ($1) RETURNING id",
        )
        .bind(format!("test_{}", run))
        .fetch_one(&sharded)
        .await
        .unwrap();

        assert!(
            omni_id.0 > starting_id.0,
            "omni ID should be unique_id, but got {}",
            omni_id.0
        );

        let sharded_id: (i64,) = sqlx::query_as(
            "INSERT INTO public.test_omni_rewrite_pk_sharded (customer_id, value) VALUES ($1, $2) RETURNING id",
    )
        .bind(run as i64)
        .bind(format!("test_{}", run))
    .fetch_one(&sharded)
    .await
    .unwrap();

        assert!(
            sharded_id.0 < omni_id.0,
            "sharded ID should not be unique_id, but got {}",
            sharded_id.0
        );
    }

    sharded.close().await;
    let sharded = connections_sqlx().await.pop().unwrap();

    // Re-enable sharded unique ID.
    admin
        .execute("SET rewrite_primary_key TO 'rewrite'")
        .await
        .unwrap();

    // The rewrite is cached in prepared statements
    // and the query cache, so we need to be careful to rest it
    // _after_ the client disconnected. In production, changing this setting
    // definitely requires a restart.
    admin.execute("RESET QUERY_CACHE").await.unwrap();
    admin.execute("RESET PREPARED").await.unwrap();

    for run in 25..50 {
        let omni_id: (i64,) = sqlx::query_as(
            "INSERT INTO public.test_omni_rewrite_pk_omni (value) VALUES ($1) RETURNING id",
        )
        .bind(format!("test_{}", run))
        .fetch_one(&sharded)
        .await
        .unwrap();

        assert!(
            omni_id.0 > starting_id.0,
            "omni ID should be unique_id, but got {}",
            omni_id.0
        );

        let sharded_id: (i64,) = sqlx::query_as(
            "INSERT INTO public.test_omni_rewrite_pk_sharded (customer_id, value) VALUES ($1, $2) RETURNING id",
    )
        .bind(run as i64)
        .bind(format!("test_{}", run))
    .fetch_one(&sharded)
    .await
    .unwrap();

        assert!(
            sharded_id.0 > omni_id.0,
            "sharded ID should be unique_id, but got {}",
            sharded_id.0
        );
    }
}
