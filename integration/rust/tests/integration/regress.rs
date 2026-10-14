use pgdog_regress::RegressTest;

#[tokio::test]
#[ignore]
async fn test_guc() {
    RegressTest::new_pg_regress(&["guc"])
        .known_bad_queries(&[
            "SET plpgsql.extra_foo_warnings = true", // We don't support LOAD
            "SHOW custom.my_guc", // We-use connections and RESET doesn't remove the setting from GUC, just empties it.
        ])
        .run_simple_ci()
        .await
        .unwrap()
}
