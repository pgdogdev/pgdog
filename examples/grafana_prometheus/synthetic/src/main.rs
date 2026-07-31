use std::env;
use std::str::FromStr;
use std::time::Duration;

use rand::Rng;
use tokio::signal;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_postgres::{Client, NoTls};

fn env_var<T: FromStr>(name: &str, default: T) -> T {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() {
    let pg_url = env::var("PG_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@pgdog:6432/postgres".into());
    let concurrency: usize = env_var("CONCURRENCY", 10usize);
    let lock_rate: f64 = env_var("LOCK_RATE", 0.05);
    let tx_rate: f64 = env_var("TX_RATE", 0.05);
    let key_space: i64 = env_var("KEY_SPACE", 10_000i64);

    wait_for_pgdog(&pg_url).await;
    init(&pg_url).await;
    println!(
        "synthetic: {} workers, lock_rate={}, tx_rate={}, key_space={} -> {}",
        concurrency, lock_rate, tx_rate, key_space, pg_url
    );

    let mut workers = JoinSet::new();
    for id in 0..concurrency {
        let url = pg_url.clone();
        workers.spawn(async move {
            worker(id, url, lock_rate, tx_rate, key_space).await;
        });
    }

    tokio::select! {
        _ = drain(&mut workers) => {}
        _ = signal::ctrl_c() => {
            println!("\nctrl-c received, shutting down");
            workers.shutdown().await;
        }
    }
}

async fn drain(workers: &mut JoinSet<()>) {
    while workers.join_next().await.is_some() {}
}

async fn connect(url: &str) -> Client {
    let (client, conn) = tokio_postgres::connect(url, NoTls)
        .await
        .expect("connect to pgdog");
    tokio::spawn(async move {
        if let Err(err) = conn.await {
            eprintln!("connection error: {err}");
        }
    });
    client
}

async fn wait_for_pgdog(url: &str) {
    for attempt in 0..60 {
        if let Ok((client, conn)) = tokio_postgres::connect(url, NoTls).await {
            let handle = tokio::spawn(conn);
            if client.simple_query("SELECT 1").await.is_ok() {
                drop(client);
                let _ = handle.await;
                return;
            }
        }
        if attempt == 0 {
            println!("waiting for pgdog...");
        }
        sleep(Duration::from_secs(1)).await;
    }
    panic!("pgdog never became ready");
}

async fn init(url: &str) {
    let client = connect(url).await;
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS kv (
                 id         bigint PRIMARY KEY,
                 value      text NOT NULL,
                 updated_at timestamptz NOT NULL DEFAULT now()
             )",
        )
        .await
        .expect("create kv table");
}

async fn worker(id: usize, url: String, lock_rate: f64, tx_rate: f64, key_space: i64) {
    let mut client = connect(&url).await;
    loop {
        let roll: f64 = rand::thread_rng().r#gen();
        let result = if roll < lock_rate {
            advisory_lock_once(&client, key_space).await
        } else if roll < lock_rate + tx_rate {
            transaction_once(&client, key_space).await
        } else if roll < lock_rate + tx_rate + 0.4 {
            upsert_once(&client, key_space).await
        } else {
            read_once(&client, key_space).await
        };
        if let Err(err) = result {
            eprintln!("worker {id}: {err:?}");
            sleep(Duration::from_millis(500)).await;
            client = connect(&url).await;
        }
    }
}

fn random_id(key_space: i64) -> i64 {
    rand::thread_rng().gen_range(0..key_space)
}

fn random_value() -> String {
    format!("{:016x}", rand::thread_rng().r#gen::<u64>())
}

async fn read_once(client: &Client, key_space: i64) -> Result<(), tokio_postgres::Error> {
    let id = random_id(key_space);
    client
        .simple_query(&format!("SELECT value FROM kv WHERE id = {id}"))
        .await?;
    Ok(())
}

async fn upsert_once(client: &Client, key_space: i64) -> Result<(), tokio_postgres::Error> {
    let id = random_id(key_space);
    let value = random_value();
    client
        .simple_query(&format!(
            "INSERT INTO kv (id, value) VALUES ({id}, '{value}') \
             ON CONFLICT (id) DO UPDATE \
               SET value = EXCLUDED.value, updated_at = now()"
        ))
        .await?;
    Ok(())
}

async fn advisory_lock_once(client: &Client, key_space: i64) -> Result<(), tokio_postgres::Error> {
    let key = random_id(key_space);

    // Session-level advisory lock. PgDog pins the client to a single server
    // for as long as the lock is held (see regex_parser.rs — `pg_advisory_lock`
    // triggers pinning), which is what surfaces on the Locked % gauge.
    client
        .simple_query(&format!("SELECT pg_advisory_lock({key})"))
        .await?;

    let work = advisory_lock_work(client).await;

    // Always release the lock, even if the work errored. If the release itself
    // fails, the server-side session will still clean up on disconnect.
    let _ = client
        .simple_query(&format!("SELECT pg_advisory_unlock({key})"))
        .await;

    work
}

async fn advisory_lock_work(client: &Client) -> Result<(), tokio_postgres::Error> {
    let hold_ms = 15 + rand::thread_rng().gen_range(0..25);

    sleep(Duration::from_millis(hold_ms)).await;

    // Shard-neutral query — the client is pinned to whichever shard the
    // advisory lock landed on.
    client.simple_query("SELECT 1").await?;

    Ok(())
}

async fn transaction_once(client: &Client, key_space: i64) -> Result<(), tokio_postgres::Error> {
    let id = random_id(key_space);
    let value = random_value();

    client.simple_query("BEGIN").await?;
    client
        .simple_query(&format!(
            "INSERT INTO kv (id, value) VALUES ({id}, '{value}') \
             ON CONFLICT (id) DO UPDATE \
               SET value = EXCLUDED.value, updated_at = now()"
        ))
        .await?;

    let hold_ms = 15 + rand::thread_rng().gen_range(0..25);

    sleep(Duration::from_millis(hold_ms)).await;

    client
        .simple_query(&format!("SELECT value FROM kv WHERE id = {id}"))
        .await?;
    client.simple_query("COMMIT").await?;

    Ok(())
}
