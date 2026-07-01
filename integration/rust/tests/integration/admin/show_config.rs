use std::collections::HashMap;

use rust::setup::admin_sqlx;
use sqlx::{Executor, Row};

use super::assert_layout;

/// `SHOW CONFIG` returns `name`/`value` TEXT columns, one row per setting.
#[tokio::test]
async fn test_show_config_reports_settings() {
    let admin = admin_sqlx().await;
    let rows = admin.fetch_all("SHOW CONFIG").await.unwrap();

    assert_layout(&rows, &[("name", "TEXT"), ("value", "TEXT")]);

    let settings: HashMap<String, String> = rows
        .iter()
        .map(|row| (row.get("name"), row.get("value")))
        .collect();

    assert_eq!(settings["host"], "0.0.0.0");
    assert_eq!(settings["port"], "6432");

    admin.close().await;
}
