use crate::setup::admin_sqlx;
use sqlx::{Executor, Row};

use super::assert_layout;

/// `SHOW VERSION` returns a single `version` TEXT row carrying the banner.
#[tokio::test]
async fn test_show_version_reports_banner() {
    let admin = admin_sqlx().await;
    let rows = admin.fetch_all("SHOW VERSION").await.unwrap();

    assert_eq!(rows.len(), 1, "SHOW VERSION should return exactly one row");
    assert_layout(&rows, &[("version", "TEXT")]);

    let version: &str = rows[0].get("version");
    assert!(
        version.starts_with("PgDog v"),
        "version should start with the PgDog banner, got: {version:?}"
    );

    admin.close().await;
}
