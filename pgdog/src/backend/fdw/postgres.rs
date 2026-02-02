use std::{
    collections::{HashMap, HashSet},
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
use pgdog_config::Role;
use rand::random_range;
use regex::Regex;
use tempfile::TempDir;
use tokio::{
    fs::remove_dir_all,
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
    select, spawn,
    sync::Notify,
    time::{sleep, Instant},
};
use tracing::{error, info, warn};

use crate::backend::{
    pool::{Address, Config, PoolConfig, Request},
    schema::postgres_fdw::{quote_identifier, ForeignTableSchema},
    Cluster, ConnectReason, Pool, Server, ServerOptions,
};

use super::{bins::Bins, Error, PostgresConfig};

static LOG_PREFIX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^(LOG|WARNING|ERROR|FATAL|PANIC|DEBUG\d?|INFO|NOTICE):\s+").unwrap());

struct PostgresProcessAsync {
    child: Child,
    initdb_dir: PathBuf,
    notify: Arc<Notify>,
    version: f32,
    port: u16,
}

impl PostgresProcessAsync {
    /// Stop Postgres and cleanup.
    async fn stop(&mut self) -> Result<(), Error> {
        warn!(
            "[fdw] stopping PostgreSQL {} running on 0.0.0.0:{}",
            self.version, self.port
        );

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

    /// Force stop immediately.
    async fn force_stop(&mut self) -> Result<(), Error> {
        self.child.kill().await?;
        self.child.wait().await?;
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
    databases: HashSet<String>,
    users: HashSet<String>,
    pid: Option<i32>,
    version: f32,
}

impl PostgresProcess {
    pub(crate) async fn new(initdb_path: Option<&Path>, port: u16) -> Result<Self, Error> {
        let notify = Arc::new(Notify::new());

        let initdb_path = if let Some(path) = initdb_path {
            path.to_owned()
        } else {
            TempDir::new()?.keep()
        };

        let bins = Bins::new().await?;

        Ok(Self {
            postres: bins.postgres,
            initdb: bins.initdb,
            initdb_dir: initdb_path,
            notify,
            port,
            databases: HashSet::new(),
            users: HashSet::new(),
            pid: None,
            version: bins.version,
        })
    }

    /// Setup and launch Postgres process.
    pub(crate) async fn launch(&mut self) -> Result<(), Error> {
        info!(
            "[fdw] initializing \"{}\" (PostgreSQL {})",
            self.initdb_dir.display(),
            self.version
        );

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
            .configure_and_save(self.port, self.version)
            .await?;

        info!(
            "[fdw] launching PostgreSQL {} on 0.0.0.0:{}",
            self.version, self.port
        );

        let child = Command::new(&self.postres)
            .arg("-D")
            .arg(&self.initdb_dir)
            .arg("-k")
            .arg(&self.initdb_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        self.pid = child.id().map(|pid| pid as i32);

        let mut process = PostgresProcessAsync {
            child,
            notify: self.notify.clone(),
            initdb_dir: self.initdb_dir.clone(),
            port: self.port,
            version: self.version,
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
                        break;
                    }

                    res = reader.read_line(&mut line) => {
                        if let Err(err) = res {
                            error!("[fdw] process error: {}", err);
                            break;
                        }

                        if !line.is_empty() {
                            let line = LOG_PREFIX.replace(&line, "");
                            info!("[fdw::subprocess] {}", line.trim());
                        }
                    }
                }
            }

            process.notify.notify_waiters();
        });

        Ok(())
    }

    /// Access the notify channel.
    pub(super) fn notify(&self) -> Arc<Notify> {
        self.notify.clone()
    }

    pub(super) fn connection_pools(&self, cluster: &Cluster) -> Result<Vec<Pool>, Error> {
        Ok(Self::pools_to_databases(cluster, 0)?
            .into_iter()
            .map(|(database, _)| {
                let address = Address {
                    host: "127.0.0.1".into(),
                    port: self.port,
                    database_name: database.clone(),
                    password: "".into(), // We use trust
                    user: cluster.identifier().user.clone(),
                    database_number: 0,
                };

                Pool::new(&PoolConfig {
                    address,
                    config: Config {
                        inner: pgdog_stats::Config {
                            max: 10,
                            ..Default::default()
                        },
                    },
                })
            })
            .collect())
    }

    fn pools_to_databases(
        cluster: &Cluster,
        shard: usize,
    ) -> Result<Vec<(String, Address)>, Error> {
        let shard = cluster
            .shards()
            .get(shard)
            .ok_or(Error::ShardsHostsMismatch)?;

        Ok(shard
            .pools()
            .iter()
            .enumerate()
            .map(|(number, pool)| {
                let database = format!("{}_{}", cluster.identifier().database, number);
                (database, pool.addr().clone())
            })
            .collect())
    }

    async fn setup_databases(&mut self, cluster: &Cluster) -> Result<bool, Error> {
        let hosts: Vec<_> = cluster
            .shards()
            .iter()
            .map(|shard| {
                let mut roles: Vec<_> = shard
                    .pools_with_roles()
                    .iter()
                    .map(|(role, _)| role)
                    .cloned()
                    .collect();
                roles.sort();
                roles
            })
            .collect();
        let identical = hosts.windows(2).all(|w| w.get(0) == w.get(1));
        if !identical {
            return Err(Error::ShardsHostsMismatch);
        }

        let mut admin_connection = self.admin_connection().await?;
        let mut created = false;

        for (database, _) in Self::pools_to_databases(cluster, 0)? {
            if !self.databases.contains(&database) {
                admin_connection
                    .execute(format!(
                        r#"CREATE DATABASE {}"#,
                        quote_identifier(&database)
                    ))
                    .await?;
                created = true;
            }
        }

        let user = cluster.identifier().user.clone();

        if !self.users.contains(&user) {
            admin_connection
                .execute(format!(
                    "CREATE USER {} SUPERUSER LOGIN",
                    quote_identifier(&cluster.identifier().user)
                ))
                .await?;
            self.users.insert(user);
        }

        Ok(created)
    }

    /// Create the same load-balancing and sharding setup we have in pgdog.toml
    /// for this cluster.
    pub(crate) async fn configure(&mut self, cluster: &Cluster) -> Result<(), Error> {
        let new_database = self.setup_databases(cluster).await?;
        let now = Instant::now();

        info!(
            "[fdw] setting up database={} user={}",
            cluster.identifier().database,
            cluster.identifier().user,
        );

        let sharding_schema = cluster.sharding_schema();

        let schema = {
            // TODO: Double check schemas are identical on all shards.
            let shard = random_range(0..sharding_schema.shards);
            let mut server = cluster
                .primary_or_replica(shard, &Request::default())
                .await?;
            ForeignTableSchema::load(&mut server).await?
        };

        // Setup persistent connections.
        let mut connections = HashMap::new();

        // We checked that all shards have the same number of replicas.
        let databases: Vec<_> = Self::pools_to_databases(cluster, 0)?
            .into_iter()
            .map(|(database, _)| database)
            .collect();

        for database in &databases {
            let identifier = (cluster.identifier().user.clone(), database.clone());
            let mut connection = self.connection(&identifier.0, database).await?;

            if new_database {
                connection
                    .execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
                    .await?;
            }

            connections.insert(identifier, connection);
        }

        for (number, _) in cluster.shards().iter().enumerate() {
            for (database, address) in Self::pools_to_databases(cluster, number)? {
                let identifier = (cluster.identifier().user.clone(), database.clone());
                let connection = connections
                    .get_mut(&identifier)
                    .expect("connection is gone");

                if new_database {
                    connection
                        .execute(format!(
                            r#"CREATE SERVER IF NOT EXISTS "shard_{}"
                                FOREIGN DATA WRAPPER postgres_fdw
                                OPTIONS (host '{}', port '{}', dbname '{}')"#,
                            number, address.host, address.port, address.database_name,
                        ))
                        .await?;
                }

                connection
                    .execute(format!(
                        r#"
                            CREATE USER MAPPING IF NOT EXISTS
                            FOR {}
                            SERVER "shard_{}"
                            OPTIONS (user '{}', password '{}')"#,
                        quote_identifier(&identifier.0),
                        number,
                        address.user,
                        address.password
                    ))
                    .await?;
            }
        }

        if new_database {
            for database in &databases {
                let identifier = (cluster.identifier().user.clone(), database.clone());
                let mut connection = connections
                    .get_mut(&identifier)
                    .expect("connection is gone");
                schema.setup(&mut connection, &sharding_schema).await?;
            }
        }

        let elapsed = now.elapsed();

        info!(
            "[fdw] setup complete for database={} user={} in {:.3}ms",
            cluster.identifier().database,
            cluster.identifier().user,
            elapsed.as_secs_f32() * 1000.0,
        );

        Ok(())
    }

    /// Create server connection.
    pub(crate) async fn admin_connection(&self) -> Result<Server, Error> {
        self.connection("postgres", "postgres").await
    }

    /// Get a connection with the user and database.
    pub(crate) async fn connection(&self, user: &str, database: &str) -> Result<Server, Error> {
        let address = self.address(user, database);

        let server =
            Server::connect(&address, ServerOptions::default(), ConnectReason::Other).await?;

        Ok(server)
    }

    fn address(&self, user: &str, database: &str) -> Address {
        Address {
            host: "127.0.0.1".into(),
            port: self.port,
            user: user.into(),
            database_name: database.into(),
            ..Default::default()
        }
    }

    /// Wait until process is ready and accepting connections.
    pub(crate) async fn wait_ready(&self) {
        self.wait_ready_internal().await;
    }

    async fn wait_ready_internal(&self) {
        while let Err(_) = self.admin_connection().await {
            sleep(Duration::from_millis(100)).await;
            continue;
        }
    }

    pub(crate) async fn stop_wait(&mut self) {
        self.notify.notify_one();
        self.notify.notified().await;
        self.pid.take();
    }

    pub(crate) fn request_stop(&mut self) {
        self.notify.notify_one();
        self.pid.take();
    }
}

impl Drop for PostgresProcess {
    fn drop(&mut self) {
        if let Some(pid) = self.pid.take() {
            warn!("[fdw] dirty shutdown initiated");

            #[cfg(unix)]
            {
                if let Err(err) = kill(Pid::from_raw(pid), Signal::SIGKILL) {
                    error!("[fdw] dirty shutdown error: {}", err);
                }

                if let Err(err) = std::fs::remove_dir_all(&self.initdb_dir) {
                    error!("[fdw] dirty shutdown cleanup error: {}", err);
                }
            }
        }
    }
}

#[cfg(test)]
mod test {

    use crate::config::config;

    use super::*;

    #[tokio::test]
    async fn test_postgres_process() {
        crate::logger();
        let cluster = Cluster::new_test(&config());
        cluster.launch();

        {
            let mut primary = cluster.primary(0, &Request::default()).await.unwrap();
            primary
                .execute("CREATE TABLE IF NOT EXISTS test_postgres_process (customer_id BIGINT)")
                .await
                .unwrap();
        }

        let mut process = PostgresProcess::new(None, 45012).await.unwrap();

        process.launch().await.unwrap();
        process.wait_ready().await;
        process.configure(&cluster).await.unwrap();
        let mut server = process.admin_connection().await.unwrap();
        let backends = server
            .fetch_all::<String>("SELECT backend_type::text FROM pg_stat_activity ORDER BY 1")
            .await
            .unwrap();

        assert_eq!(
            backends,
            [
                "background writer",
                "checkpointer",
                "client backend",
                "walwriter"
            ]
        );

        let mut server = process.connection("pgdog", "pgdog_p").await.unwrap();
        server
            .execute("SELECT * FROM pgdog.test_postgres_process")
            .await
            .unwrap();

        process.stop_wait().await;

        {
            let mut primary = cluster.primary(0, &Request::default()).await.unwrap();
            primary
                .execute("DROP TABLE test_postgres_process")
                .await
                .unwrap();
        }

        cluster.shutdown();
    }
}
