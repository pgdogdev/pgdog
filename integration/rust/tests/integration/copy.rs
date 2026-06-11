use rust::setup::{connections_sqlx, connections_tokio};

#[tokio::test]
async fn copy_from_csv() -> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_tokio().await;
    let client = &conns[0];

    client
        .execute("DROP TABLE IF EXISTS copy_csv_test", &[])
        .await?;
    client
        .execute("CREATE TABLE copy_csv_test (id BIGINT, name TEXT)", &[])
        .await?;

    let pools = connections_sqlx().await;
    let pool = &pools[0];
    let mut conn = pool.acquire().await?;

    let mut copy = conn
        .copy_in_raw("COPY copy_csv_test (id, name) FROM STDIN (FORMAT CSV)")
        .await?;
    copy.send(b"1,Alice\n2,Bob\n3,Charlie\n".as_ref()).await?;
    copy.finish().await?;

    client.execute("DROP TABLE copy_csv_test", &[]).await?;

    Ok(())
}

#[tokio::test]
async fn copy_from_csv_with_chunked_buffer() -> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_tokio().await;
    let client = &conns[0];

    client
        .execute("DROP TABLE IF EXISTS copy_csv_large_test", &[])
        .await?;
    client
        .execute(
            "CREATE TABLE copy_csv_large_test (id BIGINT, data JSONB)",
            &[],
        )
        .await?;

    let pools = connections_sqlx().await;
    let pool = &pools[0];
    let mut conn = pool.acquire().await?;

    // Single row with a large JSONB value sent as n 4k CopyData chunks.
    let chunk_size = 4096;
    let n_chunks = 4;
    let json = format!("{{\"value\":\"{}\"}}", "e".repeat(chunk_size * n_chunks));
    let csv = format!("1,{}\n", json);

    let mut copy = conn
        .copy_in_raw("COPY copy_csv_large_test (id, data) FROM STDIN (FORMAT CSV)")
        .await?;
    for chunk in csv.as_bytes().chunks(chunk_size) {
        copy.send(chunk).await?;
    }
    let rows_inserted = copy.finish().await?;
    assert_eq!(rows_inserted, 1);

    client
        .execute("DROP TABLE copy_csv_large_test", &[])
        .await?;

    Ok(())
}
