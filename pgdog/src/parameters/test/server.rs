use super::*;
use crate::backend::server::test::test_server;

#[tokio::test]
async fn test_escape_chars() {
    let mut server = test_server().await;

    for quote in ["'", "\""] {
        let base = r#"my_app_nameQUOTE;CREATE/**/TABLE/**/poc_table_two/**/(dummy_column/**/INTEGER);SET/**/application_name/**/TO/**/QUOTEyour_app_name"#;
        let param = base.replace("QUOTE", quote);
        // Postgres truncates identifiers.
        let truncated = r#"my_app_nameQUOTE;CREATE/**/TABLE/**/poc_table_two/**/(dummy_column/"#
            .replace("QUOTE", quote);

        let mut params = ParameterStore::default();
        params.set("application_name", param.clone());

        let query = ParameterStore::default()
            .sync(&params)
            .first()
            .unwrap()
            .clone();

        assert!(query.query().contains(&param));

        server.execute(query).await.unwrap();

        let param: Vec<String> = server.fetch_all("SHOW application_name").await.unwrap();
        let param = param.first().unwrap().clone();

        assert_eq!(param, truncated);
    }
}

#[tokio::test]
async fn test_set_with_server() {
    let mut server = test_server().await;

    let mut params = ParameterStore::default();
    params.set("application_name", "test_set_with_server");

    let query = ParameterStore::default()
        .sync(&params)
        .first()
        .unwrap()
        .clone();
    server.execute(query).await.unwrap();

    let param: Vec<String> = server.fetch_all("SHOW application_name").await.unwrap();
    let param = param.first().unwrap();
    assert_eq!(param, "test_set_with_server");
}

#[tokio::test]
async fn test_reset_with_server() {
    let mut server = test_server().await;

    // Set initial params on server
    let mut params = ParameterStore::default();
    params.set("application_name", "test_reset_app");
    params.set("statement_timeout", "5000");

    for query in ParameterStore::default().sync(&params) {
        server.execute(query).await.unwrap();
    }

    // Verify params are set
    let app_name: Vec<String> = server.fetch_all("SHOW application_name").await.unwrap();
    assert_eq!(app_name.first().unwrap(), "test_reset_app");

    let timeout: Vec<String> = server.fetch_all("SHOW statement_timeout").await.unwrap();
    assert_eq!(timeout.first().unwrap(), "5s");

    // Synchronize to an empty store to reset tracked parameters.
    let other = ParameterStore::default();
    let reset_queries = params.sync(&other);
    assert_eq!(reset_queries.len(), 2);

    // Execute reset queries on server
    for query in reset_queries {
        server.execute(query).await.unwrap();
    }

    // Update local tracking
    params.reset_all();

    // Verify params are reset to defaults on server
    let timeout: Vec<String> = server.fetch_all("SHOW statement_timeout").await.unwrap();
    assert_eq!(timeout.first().unwrap(), "0");

    // Synchronizing an empty store requires no SET queries.
    assert!(ParameterStore::default().sync(&params).is_empty());
}

#[tokio::test]
async fn test_reset_all_with_server() {
    let mut server = test_server().await;

    // Set initial params on server
    let mut params = ParameterStore::default();
    params.set("application_name", "test_reset_all_app");
    params.set("statement_timeout", "5000");

    for query in ParameterStore::default().sync(&params) {
        server.execute(query).await.unwrap();
    }

    // Verify params are set
    let app_name: Vec<String> = server.fetch_all("SHOW application_name").await.unwrap();
    assert_eq!(app_name.first().unwrap(), "test_reset_all_app");

    let timeout: Vec<String> = server.fetch_all("SHOW statement_timeout").await.unwrap();
    assert_eq!(timeout.first().unwrap(), "5s");

    // Get reset queries and execute on server
    let other = ParameterStore::default();
    let reset_queries = params.sync(&other);
    for query in reset_queries {
        server.execute(query).await.unwrap();
    }

    // Update local tracking
    params.reset_all();

    // Verify params are reset to defaults on server
    let timeout: Vec<String> = server.fetch_all("SHOW statement_timeout").await.unwrap();
    assert_eq!(timeout.first().unwrap(), "0");

    // Synchronizing an empty store requires no SET queries.
    assert!(ParameterStore::default().sync(&params).is_empty());

    // Set params again to verify the full synchronization cycle.
    params.set("statement_timeout", "3000");
    for query in ParameterStore::default().sync(&params) {
        server.execute(query).await.unwrap();
    }

    let timeout: Vec<String> = server.fetch_all("SHOW statement_timeout").await.unwrap();
    assert_eq!(timeout.first().unwrap(), "3s");
}
