use std::sync::Arc;

use parking_lot::Mutex;
use sqlx::{Connection, Executor, PgConnection, postgres::PgListener};
use tokio::{select, spawn, sync::Barrier};

#[tokio::test]
async fn test_notify() {
    let messages = Arc::new(Mutex::new(vec![]));
    let mut tasks = vec![];
    let mut listeners = vec![];
    let barrier = Arc::new(Barrier::new(5));

    for i in 0..5 {
        let task_msgs = messages.clone();

        let mut listener = PgListener::connect("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog")
            .await
            .unwrap();

        listener
            .listen(format!("test_notify_{}", i).as_str())
            .await
            .unwrap();

        let barrier = barrier.clone();
        listeners.push(spawn(async move {
            let mut received = 0;
            loop {
                select! {
                    msg = listener.recv() => {
                        let msg = msg.unwrap();
                        received += 1;
                        task_msgs.lock().push(msg);
                        if received == 10 {
                            break;
                        }
                    }

                }
            }
            barrier.wait().await;
        }));
    }

    for i in 0..50 {
        let handle = spawn(async move {
            let mut conn = PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog")
                .await
                .unwrap();
            conn.execute(format!("NOTIFY test_notify_{}, 'test_notify_{}'", i % 5, i % 5).as_str())
                .await
                .unwrap();
        });

        tasks.push(handle);
    }

    for task in tasks {
        task.await.unwrap();
    }

    for listener in listeners {
        listener.await.unwrap();
    }

    assert_eq!(messages.lock().len(), 50);
    let messages = messages.lock();
    for message in messages.iter() {
        assert_eq!(message.channel(), message.payload());
    }
}
