use super::setup::admin_sqlx;
use sqlx::{Executor, Row};

pub async fn assert_setting_str(name: &str, expected: &str) {
    let admin = admin_sqlx().await;
    let rows = admin.fetch_all("SHOW CONFIG").await.unwrap();
    let mut found = false;
    for row in rows {
        let db_name: String = row.get(0);
        let value: String = row.get(1);

        if name == db_name {
            found = true;
            assert_eq!(value, expected);
        }
    }

    assert!(found);
}
