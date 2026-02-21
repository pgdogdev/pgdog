//! Integration tests for COPY with FK lookup sharding.
//!
//! Tests that pgdog can resolve sharding keys via FK relationships during COPY.
//! The child table (copy_orders) does NOT have the sharding key (customer_id) directly;
//! pgdog must look it up via the FK to copy_users.

use rust::setup::{admin_sqlx, connections_sqlx};
use sqlx::postgres::PgPoolCopyExt;
use sqlx::{Executor, Pool, Postgres};

async fn setup_fk_tables(pool: &Pool<Postgres>, admin: &Pool<Postgres>) {
    // Drop tables if they exist (in public schema)
    pool.execute("DROP TABLE IF EXISTS public.copy_orders, public.copy_users")
        .await
        .unwrap();

    // Create users table with sharding key (customer_id) in public schema
    pool.execute(
        "CREATE TABLE public.copy_users (
            id BIGINT PRIMARY KEY,
            customer_id BIGINT NOT NULL
        )",
    )
    .await
    .unwrap();

    // Create orders table with FK to users - no customer_id column!
    // pgdog must look up customer_id via the FK
    pool.execute(
        "CREATE TABLE public.copy_orders (
            id BIGINT PRIMARY KEY,
            user_id BIGINT REFERENCES public.copy_users(id)
        )",
    )
    .await
    .unwrap();

    // Reload schema so pgdog picks up the new tables and FK relationships
    admin.execute("RELOAD").await.unwrap();

    // Wait for schema reload to complete (happens asynchronously)
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
}

async fn cleanup_fk_tables(pool: &Pool<Postgres>, admin: &Pool<Postgres>) {
    pool.execute("DROP TABLE IF EXISTS public.copy_orders, public.copy_users")
        .await
        .ok();
    admin.execute("RELOAD").await.unwrap();
}

#[tokio::test]
async fn test_copy_fk_lookup_text() {
    let admin = admin_sqlx().await;
    let mut pools = connections_sqlx().await;
    let pool = pools.swap_remove(1); // pgdog_sharded

    setup_fk_tables(&pool, &admin).await;

    // Insert users with varying customer_id (sharding key)
    for i in 1i64..=100 {
        sqlx::query("INSERT INTO public.copy_users (id, customer_id) VALUES ($1, $2)")
            .bind(i)
            .bind(i * 100 + i % 17)
            .execute(&pool)
            .await
            .unwrap();
    }

    // Use COPY to insert orders referencing users
    // Only pass id and user_id - pgdog should look up customer_id via FK
    let copy_data: String = (1i64..=100)
        .map(|i| format!("{}\t{}\n", i * 10, i)) // order_id, user_id (FK)
        .collect();

    let mut copy_in = pool
        .copy_in_raw("COPY public.copy_orders (id, user_id) FROM STDIN")
        .await
        .unwrap();
    copy_in.send(copy_data.as_bytes()).await.unwrap();
    copy_in.finish().await.unwrap();

    // Verify all orders were inserted
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM public.copy_orders")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count.0, 100, "expected 100 orders");

    // Verify orders data is correct (no JOINs - pgdog doesn't support sharded JOINs)
    let orders: Vec<(i64, i64)> =
        sqlx::query_as("SELECT id, user_id FROM public.copy_orders ORDER BY id LIMIT 10")
            .fetch_all(&pool)
            .await
            .unwrap();

    assert_eq!(orders.len(), 10);
    for (i, (order_id, user_id)) in orders.into_iter().enumerate() {
        let expected_order_id = ((i + 1) * 10) as i64;
        let expected_user_id = (i + 1) as i64;
        assert_eq!(order_id, expected_order_id);
        assert_eq!(user_id, expected_user_id);
    }

    cleanup_fk_tables(&pool, &admin).await;
}

#[tokio::test]
async fn test_copy_fk_lookup_binary() {
    let admin = admin_sqlx().await;
    let mut pools = connections_sqlx().await;
    let pool = pools.swap_remove(1); // pgdog_sharded

    setup_fk_tables(&pool, &admin).await;

    // Insert users with varying customer_id (sharding key)
    for i in 1i64..=50 {
        sqlx::query("INSERT INTO public.copy_users (id, customer_id) VALUES ($1, $2)")
            .bind(i)
            .bind(i * 100 + i % 13)
            .execute(&pool)
            .await
            .unwrap();
    }

    // Use binary COPY to insert orders
    // Only pass id and user_id - pgdog should look up customer_id via FK
    let mut binary_data = Vec::new();

    // Binary COPY header: PGCOPY\n\377\r\n\0
    binary_data.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    // Flags (4 bytes) + header extension length (4 bytes)
    binary_data.extend_from_slice(&[0u8; 8]);

    // Insert 50 rows
    for i in 1i64..=50 {
        // Number of columns (2 bytes)
        binary_data.extend_from_slice(&2i16.to_be_bytes());
        // Column 1: order id (8 bytes for BIGINT)
        binary_data.extend_from_slice(&8i32.to_be_bytes()); // length
        binary_data.extend_from_slice(&(i * 10).to_be_bytes()); // value
        // Column 2: user_id FK (8 bytes for BIGINT)
        binary_data.extend_from_slice(&8i32.to_be_bytes()); // length
        binary_data.extend_from_slice(&i.to_be_bytes()); // value
    }

    // Trailer: -1 (2 bytes)
    binary_data.extend_from_slice(&(-1i16).to_be_bytes());

    let mut copy_in = pool
        .copy_in_raw("COPY public.copy_orders (id, user_id) FROM STDIN WITH (FORMAT binary)")
        .await
        .unwrap();
    copy_in.send(binary_data.as_slice()).await.unwrap();
    let result = copy_in.finish().await;
    if let Err(e) = &result {
        eprintln!("Binary COPY failed: {:?}", e);
    }
    let rows_copied = result.unwrap();
    assert_eq!(rows_copied, 50, "expected 50 rows copied");

    // Small delay before verify
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Verify all orders were inserted
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM public.copy_orders")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count.0, 50, "expected 50 orders");

    // Verify data integrity
    let rows: Vec<(i64, i64)> =
        sqlx::query_as("SELECT id, user_id FROM public.copy_orders ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();

    for (i, (order_id, user_id)) in rows.into_iter().enumerate() {
        let expected_order_id = ((i + 1) * 10) as i64;
        let expected_user_id = (i + 1) as i64;
        assert_eq!(order_id, expected_order_id);
        assert_eq!(user_id, expected_user_id);
    }

    cleanup_fk_tables(&pool, &admin).await;
}

#[tokio::test]
async fn test_copy_direct_sharding_key() {
    let admin = admin_sqlx().await;
    let mut pools = connections_sqlx().await;
    let pool = pools.swap_remove(1); // pgdog_sharded

    // For this test, we COPY directly to copy_users which has the sharding key
    pool.execute("DROP TABLE IF EXISTS public.copy_orders, public.copy_users")
        .await
        .unwrap();

    pool.execute(
        "CREATE TABLE public.copy_users (
            id BIGINT PRIMARY KEY,
            customer_id BIGINT NOT NULL
        )",
    )
    .await
    .unwrap();

    admin.execute("RELOAD").await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Use COPY to insert users directly (sharding by customer_id column)
    let copy_data: String = (1i64..=100)
        .map(|i| format!("{}\t{}\n", i, i * 100 + i % 7))
        .collect();

    let mut copy_in = pool
        .copy_in_raw("COPY public.copy_users (id, customer_id) FROM STDIN")
        .await
        .unwrap();
    copy_in.send(copy_data.as_bytes()).await.unwrap();
    copy_in.finish().await.unwrap();

    // Verify all users were inserted
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM public.copy_users")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count.0, 100, "expected 100 users");

    pool.execute("DROP TABLE IF EXISTS public.copy_users")
        .await
        .unwrap();
    admin.execute("RELOAD").await.unwrap();
}
