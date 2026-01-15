use sqlx::Connection;
use tokio::sync::mpsc::{Sender, channel};
use tokio::{select, spawn};

#[tokio::test]
#[ignore]
async fn slam_with_prepared() -> Result<(), Box<dyn std::error::Error>> {
    let conns = 1000;
    let statements = 50_000;
    let mut signals: Vec<Sender<()>> = vec![];
    let mut tasks = vec![];

    for _ in 0..conns {
        let (tx, mut rx) = channel(1);
        signals.push(tx);

        let handle = spawn(async move {
            let mut conn =
                sqlx::PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog").await?;

            loop {
                let r = rand::random_range(0..statements);

                let query = format!("SELECT 1, 2, 3, 4, $1, 'apples and oranges', 'blah', {}", r);
                let query = sqlx::query(query.as_str()).bind(r).execute(&mut conn);

                select! {
                    res = query => {
                         res?;
                    }

                    _ = rx.recv() => { break; }
                }
            }

            Ok::<(), sqlx::Error>(())
        });

        tasks.push(handle);
    }

    for task in tasks {
        task.await??;
    }

    Ok(())
}
