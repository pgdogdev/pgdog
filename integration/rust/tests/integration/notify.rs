use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use sqlx::{Connection, Executor, PgConnection, postgres::PgListener};
use tokio::{select, spawn, sync::Barrier, time::timeout};

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

#[tokio::test]
async fn test_notify_only_delivered_after_transaction_commit() {
    let messages = Arc::new(Mutex::new(vec![]));

    // Set up a listener for the test channel
    let mut listener = PgListener::connect("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog")
        .await
        .unwrap();

    listener.listen("test_tx_notify").await.unwrap();

    let listener_messages = messages.clone();
    let listener_task = spawn(async move {
        loop {
            select! {
                msg = listener.recv() => {
                    let msg = msg.unwrap();
                    listener_messages.lock().push((msg.channel().to_string(), msg.payload().to_string()));
                }
            }
        }
    });

    // Give the listener a moment to be fully set up
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Start a transaction and send a NOTIFY inside it
    let mut conn = PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog")
        .await
        .unwrap();

    // Begin transaction
    conn.execute("BEGIN").await.unwrap();

    // Send NOTIFY inside the transaction
    conn.execute("NOTIFY test_tx_notify, 'inside_transaction'")
        .await
        .unwrap();

    // Wait a bit to ensure that if NOTIFY were delivered immediately, we'd see it
    tokio::time::sleep(Duration::from_millis(200)).await;

    // At this point, the NOTIFY should NOT have been delivered yet
    assert_eq!(
        messages.lock().len(),
        0,
        "NOTIFY should not be delivered before transaction commit"
    );

    // Commit the transaction
    conn.execute("COMMIT").await.unwrap();

    // Wait for the NOTIFY to be delivered after commit
    let result = timeout(Duration::from_secs(5), async {
        loop {
            if messages.lock().len() > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "NOTIFY should be delivered after transaction commit"
    );
    assert_eq!(
        messages.lock().len(),
        1,
        "Exactly one NOTIFY should be delivered"
    );

    let messages_guard = messages.lock();
    let (channel, payload) = &messages_guard[0];
    assert_eq!(channel, "test_tx_notify");
    assert_eq!(payload, "inside_transaction");

    listener_task.abort();
}

#[tokio::test]
async fn test_notify_not_delivered_after_transaction_rollback() {
    let messages = Arc::new(Mutex::new(vec![]));

    // Set up a listener for the test channel
    let mut listener = PgListener::connect("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog")
        .await
        .unwrap();

    listener.listen("test_tx_rollback_notify").await.unwrap();

    let listener_messages = messages.clone();
    let listener_task = spawn(async move {
        loop {
            select! {
                msg = listener.recv() => {
                    let msg = msg.unwrap();
                    listener_messages.lock().push((msg.channel().to_string(), msg.payload().to_string()));
                }
            }
        }
    });

    // Give the listener a moment to be fully set up
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Start a transaction and send a NOTIFY inside it
    let mut conn = PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog")
        .await
        .unwrap();

    // Begin transaction
    conn.execute("BEGIN").await.unwrap();

    // Send NOTIFY inside the transaction
    conn.execute("NOTIFY test_tx_rollback_notify, 'inside_rolled_back_transaction'")
        .await
        .unwrap();

    // Wait a bit to ensure that if NOTIFY were delivered immediately, we'd see it
    tokio::time::sleep(Duration::from_millis(200)).await;

    // At this point, the NOTIFY should NOT have been delivered yet
    assert_eq!(
        messages.lock().len(),
        0,
        "NOTIFY should not be delivered before transaction commit"
    );

    // Rollback the transaction
    conn.execute("ROLLBACK").await.unwrap();

    // Wait to see if any NOTIFY gets delivered (it shouldn't)
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The NOTIFY should NOT be delivered after rollback
    assert_eq!(
        messages.lock().len(),
        0,
        "NOTIFY should not be delivered after transaction rollback"
    );

    listener_task.abort();
}
