use pgdog_regress::RegressTest;

#[tokio::test]
async fn test_guc() {
    RegressTest::new_pg_regress(&["guc"])
        .run_simple_ci()
        .await
        .unwrap()
}
