use pgdog_regress::RegressTest;

#[tokio::test]
async fn test_set_application_name() {
    RegressTest::new(&[
        "SET application_name TO 'apples'",
        "SHOW application_name",
        "RESET application_name",
        "/* server_reset */ SHOW application_name",
    ])
    .known_bad_queries(&["/* server_reset */ SHOW application_name"]) // This will return PgDog for pgdog connection because its being reset to its startup value on the server side, which is set to PgDog by default.
    .run_simple_ci()
    .await
    .unwrap()
}

#[tokio::test]
async fn test_bad_parameter() {
    RegressTest::new(&[
        "SET statement_timeout TO 'sdf'",
        "SELECT 1",
        "SHOW statement_timeout",
    ])
    .run_simple_ci()
    .await
    .unwrap();
}

#[tokio::test]
async fn test_enum() {
    RegressTest::new(&[
        "SET intervalstyle TO 'iso_8601'",
        "SET intervalstyle TO 'invalid'",
        "SHOW intervalstyle",
    ])
    .run_simple_ci()
    .await
    .expect("enum validation and hint match PostgreSQL");
}

#[tokio::test]
async fn test_enum_tx() {
    RegressTest::new(&[
        "SET intervalstyle TO 'iso_8601'",
        "BEGIN",
        "SET intervalstyle TO 'invalid'",
        "ROLLBACK",
        "SHOW intervalstyle",
    ])
    .run_simple_ci()
    .await
    .expect("invalid enums abort the transaction and preserve the session value");
}

#[tokio::test]
async fn test_int_bounds() {
    RegressTest::new(&[
        "SET extra_float_digits TO -15",
        "SET extra_float_digits TO -16",
        "SHOW extra_float_digits",
        "SET extra_float_digits TO 3",
        "SET extra_float_digits TO 4",
        "SHOW extra_float_digits",
    ])
    .run_simple_ci()
    .await
    .expect("integer bounds match PostgreSQL");
}

#[tokio::test]
async fn test_rounding() {
    RegressTest::new(&[
        "SET extra_float_digits TO '2.5'",
        "SHOW extra_float_digits",
        "SET extra_float_digits TO '3.5'",
        "SHOW extra_float_digits",
    ])
    .run_simple_ci()
    .await
    .expect("integer rounding and range errors match PostgreSQL");
}

#[tokio::test]
async fn test_time_units() {
    RegressTest::new(&[
        "SET statement_timeout TO '1.5 min'",
        "SHOW statement_timeout",
        "SET statement_timeout TO '-1s'",
        "SHOW statement_timeout",
    ])
    .run_simple_ci()
    .await
    .expect("time units and range errors match PostgreSQL");
}

#[tokio::test]
async fn test_mem_units() {
    RegressTest::new(&[
        "SET work_mem TO '16MB'",
        "SHOW work_mem",
        "SET work_mem TO '63.5kB'",
        "SHOW work_mem",
        "SET work_mem TO '1B'",
        "SHOW work_mem",
    ])
    .run_simple_ci()
    .await
    .expect("memory units and range errors match PostgreSQL");
}

#[tokio::test]
async fn test_real_bounds() {
    RegressTest::new(&[
        "SET geqo_selection_bias TO 1.5",
        "SET geqo_selection_bias TO 1.4",
        "SHOW geqo_selection_bias",
        "SET geqo_selection_bias TO 2",
        "SET geqo_selection_bias TO 2.1",
        "SHOW geqo_selection_bias",
    ])
    .run_simple_ci()
    .await
    .expect("real bounds match PostgreSQL");
}

#[tokio::test]
async fn test_real_units() {
    RegressTest::new(&[
        "SET vacuum_cost_delay TO '0.5ms'",
        "SHOW vacuum_cost_delay",
        "SET vacuum_cost_delay TO '0.1s'",
        "SHOW vacuum_cost_delay",
        "SET vacuum_cost_delay TO '10s'",
        "SHOW vacuum_cost_delay",
    ])
    .run_simple_ci()
    .await
    .expect("real units and range errors match PostgreSQL");
}

#[tokio::test]
async fn test_nonfinite() {
    RegressTest::new(&[
        "SET seq_page_cost TO 1",
        "SET seq_page_cost TO 'NaN'",
        "SET seq_page_cost TO 'inf'",
        "SET seq_page_cost TO '-inf'",
        "SHOW seq_page_cost",
    ])
    .run_simple_ci()
    .await
    .expect("nonfinite values and scientific bounds match PostgreSQL");
}

#[tokio::test]
async fn test_repeat() {
    RegressTest::new(&[
        "SET application_name TO 'apples'",
        "SET application_name TO 'apples'",
        "SET application_name TO 'oranges'",
        "SET application_name TO 'oranges'",
        "SHOW application_name",
    ])
    .run_simple_ci()
    .await
    .expect("unchanged values do not emit duplicate parameter statuses");
}

#[tokio::test]
async fn test_reset() {
    RegressTest::new(&[
        "SET application_name TO 'apples'",
        "RESET application_name",
        "RESET application_name",
        "SELECT 1",
    ])
    .run_simple_ci()
    .await
    .expect("reset reports the effective value only when it changes");
}

#[tokio::test]
async fn test_bad_tx() {
    RegressTest::new(&[
        "SET extra_float_digits TO 1",
        "BEGIN",
        "SET extra_float_digits TO 4",
        "ROLLBACK",
        "SHOW extra_float_digits",
        "SELECT 1",
    ])
    .run_simple_ci()
    .await
    .expect("invalid parameters abort the transaction without changing session values");
}

#[tokio::test]
async fn test_local() {
    RegressTest::new(&[
        "SET extra_float_digits TO 1",
        "BEGIN",
        "SET LOCAL extra_float_digits TO 2",
        "SHOW extra_float_digits",
        "COMMIT",
        "SHOW extra_float_digits",
    ])
    .run_simple_ci()
    .await
    .expect("commit restores the session value after SET LOCAL");
}

#[tokio::test]
async fn test_multi_set() {
    RegressTest::new(&["SET application_name TO 'z'; SET statement_timeout TO 1234;"])
        .run_simple_ci()
        .await
        .unwrap()
}

#[tokio::test]
async fn multiple_set_and_reset_report_each_change() {
    RegressTest::new(&[
        "SET application_name = 'startup'",
        "SET timezone = 'UTC'",
        "SET application_name = 'first';
         SET application_name = 'first';
         SET timezone = 'Europe/Paris';
         RESET application_name;
         SET work_mem = '4MB';
         SET application_name = 'last'",
        "SET application_name = 'last'",
        "SHOW application_name",
        "SHOW timezone",
        "SHOW work_mem",
    ])
    .run_simple_ci()
    .await
    .expect(
        "multi-SET completions, final parameter statuses, and repeated values match PostgreSQL",
    );
}

#[tokio::test]
async fn multiple_set_local_transaction() {
    RegressTest::new(&[
        "BEGIN",
        "SET application_name = 'startup'",
        "SET timezone = 'UTC'",
        "SET work_mem = '4MB';
         SET LOCAL application_name = 'ignored';
         SET LOCAL timezone = 'Europe/Paris';
         SET lock_timeout = '1s'",
        "SHOW application_name",
        "SHOW timezone",
        "SHOW work_mem",
        "SET lock_timeout TO 5000",
        "SHOW lock_timeout",
        "END",
    ])
    .run_simple_ci()
    .await
    .expect("SET LOCAL batch notices and effective session values match PostgreSQL");
}

#[tokio::test]
async fn set_config_returns_row_and_parameter_status() {
    RegressTest::new(&[
        "SET application_name = 'startup'",
        "SELECT set_config('application_name', 'changed', false)",
        "SELECT set_config('application_name', NULL, false)",
        "/* server_reset */ SHOW application_name",
    ])
    .known_bad_queries(&["/* server_reset */ SHOW application_name"])
    .run_simple_ci()
    .await
    .expect("set_config rows, NULL handling, completions, and parameter statuses match PostgreSQL");
}

#[tokio::test]
async fn datestyle_batch_uses_each_preceding_value_and_reset() {
    RegressTest::new(&[
        "SET datestyle = 'ISO, YMD'",
        "SET datestyle = SQL, YMD;
         SET datestyle = 'MDY';
         SET datestyle = 'SQL';
         RESET datestyle;
         SET datestyle = 'MDY'",
        "SHOW datestyle",
    ])
    .run_simple_ci()
    .await
    .expect("DateStyle batch inheritance, RESET, and final status match PostgreSQL");
}

#[tokio::test]
async fn datestyle_set_config_returns_normalized_value() {
    RegressTest::new(&[
        "SET datestyle = 'SQL, YMD'",
        "SELECT set_config('DateStyle', 'MDY', false)",
        "SHOW datestyle",
    ])
    .run_simple_ci()
    .await
    .expect("set_config returns and reports the normalized DateStyle value");
}

#[tokio::test]
async fn datestyle_local_inheritance_and_transaction_end() {
    for end in ["ROLLBACK", "COMMIT"] {
        RegressTest::new(&[
            "SET datestyle = 'ISO, MDY'",
            "BEGIN",
            "SET LOCAL datestyle = 'SQL'",
            "SET datestyle = 'DMY'",
            end,
            "SHOW datestyle",
            "SET datestyle = 'DEFAULT, YMD'",
            "SHOW datestyle",
        ])
        .run_simple_ci()
        .await
        .expect("DateStyle inheritance and transaction end match PostgreSQL");
    }
}

#[tokio::test]
async fn invalid_datestyle_batch_preserves_state_and_aborts_transaction() {
    for in_transaction in [false, true] {
        let mut queries = vec!["SET datestyle = 'ISO, MDY'"];
        if in_transaction {
            queries.push("BEGIN");
        }
        queries.extend([
            "SET datestyle = 'SQL'; SET datestyle = 'ISO, SQL'",
            "SHOW datestyle",
        ]);
        if in_transaction {
            queries.extend(["ROLLBACK", "SHOW datestyle"]);
        }

        RegressTest::new(&queries)
            .run_simple_ci()
            .await
            .expect("invalid DateStyle batches preserve state and report PostgreSQL errors");
    }
}
