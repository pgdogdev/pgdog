use std::time::Duration;

use rust::{
    setup::{admin_sqlx, connection_sqlx_direct},
    utils::{Message, connect},
};
use sqlx::{Executor, Pool, Postgres};
use tokio::{spawn, time::sleep};

#[tokio::test]
async fn test_partial_request_disconnect() {
    let admin = admin_sqlx().await;
    let direct = connection_sqlx_direct().await;

    admin.execute("SET auth_type TO 'trust'").await.unwrap();

    multiple_clients!(
        {
            let mut stream = connect().await;

            Message::new_parse("test", "SELECT $1")
                .send(&mut stream)
                .await
                .unwrap();
            // Message::new_flush().send(&mut stream).await.unwrap();

            drop(stream);
        },
        50
    );

    sleep(Duration::from_millis(100)).await;

    let acr = active_client_read(&direct).await;
    assert_eq!(acr, 0);
}

macro_rules! multiple_clients {
    ($code:block, $times:expr) => {{
        let mut handles = vec![];

        for _ in 0..$times {
            let handle = spawn(async move {
                $code;
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }};
}

use multiple_clients;

async fn active_client_read(pool: &Pool<Postgres>) -> i32 {
    let count: i32 = sqlx::query_scalar(
        "SELECT COUNT(*)::integer FROM pg_stat_activity WHERE state != 'idle' AND wait_event = 'ClientRead'"
    ).fetch_one(pool).await.unwrap();

    count
}
