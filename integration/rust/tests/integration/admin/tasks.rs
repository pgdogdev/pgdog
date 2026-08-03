use crate::setup::admin_sqlx;
use sqlx::{Executor, Row};

#[tokio::test]
async fn test_stop_nonexistent_task() {
    let admin = admin_sqlx().await;

    let row = admin.fetch_one("STOP_TASK 999999999").await.unwrap();
    assert_eq!(row.get::<String, _>("stop_task"), "task not found");
}
