use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

#[cfg(unix)]
use nix::{
    sys::signal::{kill, Signal},
    unistd::Pid,
};

use once_cell::sync::Lazy;
use regex::Regex;
use tempfile::TempDir;
use tokio::{
    fs::remove_dir_all,
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
    select, spawn,
    sync::Notify,
    time::{sleep, timeout},
};
use tracing::{error, info};

use crate::backend::{
    pool::{Address, Config, PoolConfig},
    ConnectReason, Pool, Server, ServerOptions,
};

use super::{Error, PostgresConfig};

static LOG_PREFIX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^(LOG|WARNING|ERROR|FATAL|PANIC|DEBUG\d?|INFO|NOTICE):\s+").unwrap());

struct PostgresProcessAsync {
    child: Child,
    initdb_dir: PathBuf,
    notify: Arc<Notify>,
}

impl PostgresProcessAsync {
    /// Stop Postgres and cleanup.
    async fn stop(&mut self) -> Result<(), Error> {
        #[cfg(unix)]
        {
            let pid = self.child.id().expect("child has no pid") as i32;
            let pid = Pid::from_raw(pid);
            kill(pid, Signal::SIGINT)?;
        }

        #[cfg(not(unix))]
        self.child.kill().await?;

        self.child.wait().await?;

        // Delete data dir, its ephemeral.
        remove_dir_all(&self.initdb_dir).await?;

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PostgresProcess {
    postres: PathBuf,
    initdb: PathBuf,
    initdb_dir: PathBuf,
    notify: Arc<Notify>,
    port: u16,
}

impl PostgresProcess {
    pub(crate) fn new(initdb_path: Option<&Path>, port: u16) -> Result<Self, Error> {
        let notify = Arc::new(Notify::new());

        let initdb_path = if let Some(path) = initdb_path {
            path.to_owned()
        } else {
            TempDir::new()?.keep()
        };

        Ok(Self {
            postres: PathBuf::from("postgres"),
            initdb: PathBuf::from("initdb"),
            initdb_dir: initdb_path,
            notify,
            port,
        })
    }

    /// Setup and launch Postgres process.
    pub(crate) async fn launch(&mut self) -> Result<(), Error> {
        info!("[fdw] initializing \"{}\"", self.initdb_dir.display());

        let process = Command::new(&self.initdb)
            .arg("-D")
            .arg(&self.initdb_dir)
            .arg("--username")
            .arg("postgres")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await?;

        if !process.status.success() {
            error!("{}", String::from_utf8_lossy(&process.stdout));
            error!("{}", String::from_utf8_lossy(&process.stderr));
            return Err(Error::InitDb);
        }

        // Configure Postgres.
        PostgresConfig::new(&self.initdb_dir.join("postgresql.conf"))
            .await?
            .configure_and_save(self.port)
            .await?;

        let child = Command::new(&self.postres)
            .arg("-D")
            .arg(&self.initdb_dir)
            .arg("-k")
            .arg(&self.initdb_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let mut process = PostgresProcessAsync {
            child,
            notify: self.notify.clone(),
            initdb_dir: self.initdb_dir.clone(),
        };

        spawn(async move {
            info!("[fdw] postgres process running");

            let reader = process
                .child
                .stderr
                .take()
                .map(|stdout| BufReader::new(stdout));

            let mut reader = if let Some(reader) = reader {
                reader
            } else {
                error!("[fdw] failed to start subprocess: no stderr");
                if let Err(err) = process.stop().await {
                    error!("[fdw] failed to abort subprocess: {}", err);
                }
                return;
            };

            loop {
                let mut line = String::new();
                select! {
                    _ = process.notify.notified() => {
                        if let Err(err) = process.stop().await {
                            error!("[fdw] shutdown error: {}", err);
                        }
                        break;
                    }

                    _ = process.child.wait() => {
                        error!("[fdw] postgres shut down unexpectedly");
                    }

                    res = reader.read_line(&mut line) => {
                        if let Err(err) = res {
                            error!("[fdw] process error: {}", err);
                            break;
                        }

                        if !line.is_empty() {
                            let line = LOG_PREFIX.replace(&line, "");
                            info!("[fdw/subprocess] {}", line.trim());
                        }
                    }
                }
            }

            process.notify.notify_one();
        });

        Ok(())
    }

    /// Create server connection.
    pub(crate) async fn connection(&self) -> Result<Server, Error> {
        let address = self.address();

        let server =
            Server::connect(&address, ServerOptions::default(), ConnectReason::Other).await?;

        Ok(server)
    }

    fn address(&self) -> Address {
        Address {
            host: "127.0.0.1".into(),
            port: 6000,
            user: "postgres".into(),
            database_name: "postgres".into(),
            ..Default::default()
        }
    }

    /// Connection pool that connections directly to the server.
    pub(crate) fn pool(&self) -> Pool {
        Pool::new(&PoolConfig {
            address: self.address(),
            config: Config {
                inner: pgdog_stats::Config {
                    max: 10,
                    ..Default::default()
                },
            },
        })
    }

    /// Wait until process is ready and accepting connections.
    pub(crate) async fn wait_ready(&self) -> Result<(), Error> {
        timeout(Duration::from_millis(5000), self.wait_ready_internal()).await?;

        Ok(())
    }

    async fn wait_ready_internal(&self) {
        while let Err(_) = self.connection().await {
            sleep(Duration::from_millis(100)).await;
            continue;
        }
    }

    pub(crate) async fn stop(&self) {
        self.notify.notify_one();
        self.notify.notified().await;
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[tokio::test]
    async fn test_postgres_process() {
        crate::logger();

        let mut process = PostgresProcess::new(None, 6000).unwrap();

        process.launch().await.unwrap();
        process.wait_ready().await.unwrap();
        let mut server = process.connection().await.unwrap();
        server.execute("SELECT 1").await.unwrap();
        server
            .execute("CREATE TABLE test (id BIGINT)")
            .await
            .unwrap();
        server.execute("INSERT INTO test VALUES (1)").await.unwrap();
        server.execute("CHECKPOINT").await.unwrap();
        process.stop().await;
    }
}
