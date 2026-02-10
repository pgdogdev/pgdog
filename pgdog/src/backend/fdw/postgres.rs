use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    process::Stdio,
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
    sync::watch,
    time::{sleep, Instant},
};
use tracing::{error, info, warn};

use crate::backend::{
    pool::{Address, Config, PoolConfig, Request},
    schema::postgres_fdw::{quote_identifier, FdwServerDef, ForeignTableSchema},
    Cluster, ConnectReason, Server, ServerOptions,
};

use super::{bins::Bins, Error, PostgresConfig};

static LOG_PREFIX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^(LOG|WARNING|ERROR|FATAL|PANIC|DEBUG\d?|INFO|NOTICE):\s+").unwrap());

struct PostgresProcessAsync {
    child: Child,
    initdb_dir: PathBuf,
    shutdown: watch::Receiver<bool>,
    shutdown_complete: watch::Sender<bool>,
    version: f32,
    port: u16,
}

impl PostgresProcessAsync {
    /// Stop Postgres and cleanup.
    async fn stop(&mut self) -> Result<(), Error> {
        info!(
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
}

#[derive(Debug, Clone)]
pub(crate) struct FdwBackend {
    pub(crate) config: Config,
    pub(crate) address: Address,
    pub(crate) database_name: String,
    pub(crate) role: Role,
}

#[derive(Debug)]
pub(crate) struct PostgresProcess {
    postres: PathBuf,
    initdb: PathBuf,
    initdb_dir: PathBuf,
    shutdown: watch::Sender<bool>,
    shutdown_complete: watch::Sender<bool>,
    port: u16,
    pid: Option<i32>,
    version: f32,
    /// Tracks which cluster databases have been fully configured.
    /// Subsequent clusters with the same database only get user mappings.
    configured_databases: HashSet<String>,
}

impl PostgresProcess {
    pub(crate) async fn new(initdb_path: Option<&Path>, port: u16) -> Result<Self, Error> {
        let initdb_path = if let Some(path) = initdb_path {
            path.to_owned()
        } else {
            TempDir::new()?.keep()
        };

        let bins = Bins::new().await?;

        let (shutdown, _) = watch::channel(false);
        let (shutdown_complete, _) = watch::channel(false);

        Ok(Self {
            postres: bins.postgres,
            initdb: bins.initdb,
            initdb_dir: initdb_path,
            shutdown,
            shutdown_complete,
            port,
            pid: None,
            version: bins.version,
            configured_databases: HashSet::new(),
        })
    }

    /// Kill any existing process listening on the given port.
    /// This handles orphaned postgres processes from previous crashes.
    #[cfg(unix)]
    async fn kill_existing_on_port(port: u16) {
        // Use fuser to find and kill any process on the port
        let result = Command::new("fuser")
            .arg("-k")
            .arg(format!("{}/tcp", port))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await;

        if let Ok(status) = result {
            if status.success() {
                warn!(
                    "[fdw] killed orphaned process on port {} from previous run",
                    port
                );
                // Give it a moment to fully terminate
                sleep(Duration::from_millis(100)).await;
            }
        }
    }

    #[cfg(not(unix))]
    async fn kill_existing_on_port(_port: u16) {
        // Not implemented for non-unix platforms
    }

    /// Setup and launch Postgres process.
    pub(crate) async fn launch(&mut self) -> Result<(), Error> {
        // Clean up any orphaned postgres from previous crashes
        Self::kill_existing_on_port(self.port).await;

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

        let mut cmd = Command::new(&self.postres);
        cmd.arg("-D")
            .arg(&self.initdb_dir)
            .arg("-k")
            .arg(&self.initdb_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        #[cfg(unix)]
        cmd.process_group(0); // Prevent sigint from terminal.

        // SAFETY: prctl(PR_SET_PDEATHSIG) is async-signal-safe and doesn't
        // access any shared state. It tells the kernel to send SIGKILL to
        // this process when its parent dies, preventing orphaned processes.
        #[cfg(target_os = "linux")]
        {
            #[allow(unused_imports)]
            use std::os::unix::process::CommandExt;
            unsafe {
                cmd.pre_exec(|| {
                    const PR_SET_PDEATHSIG: nix::libc::c_int = 1;
                    nix::libc::prctl(PR_SET_PDEATHSIG, nix::libc::SIGKILL);
                    Ok(())
                });
            }
        }

        let child = cmd.spawn()?;

        self.pid = child.id().map(|pid| pid as i32);

        let mut process = PostgresProcessAsync {
            child,
            shutdown: self.shutdown.subscribe(),
            shutdown_complete: self.shutdown_complete.clone(),
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
                    _ = process.shutdown.changed() => {
                        if *process.shutdown.borrow() {
                            if let Err(err) = process.stop().await {
                                error!("[fdw] shutdown error: {}", err);
                            }
                            break;
                        }
                    }

                    exit_status = process.child.wait() => {
                        // Drain remaining stderr before reporting shutdown
                        loop {
                            let mut remaining = String::new();
                            match reader.read_line(&mut remaining).await {
                                Ok(0) => break, // EOF
                                Ok(_) => {
                                    if !remaining.is_empty() {
                                        let remaining = LOG_PREFIX.replace(&remaining, "");
                                        info!("[fdw::subprocess] {}", remaining.trim());
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                        error!("[fdw] postgres shut down unexpectedly, exit status: {:?}", exit_status);
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

            let _ = process.shutdown_complete.send(true);
        });

        Ok(())
    }

    /// Get a receiver for shutdown completion notification.
    pub(super) fn shutdown_receiver(&self) -> watch::Receiver<bool> {
        self.shutdown_complete.subscribe()
    }

    pub(crate) fn connection_pool_configs(
        port: u16,
        cluster: &Cluster,
    ) -> Result<Vec<(Role, PoolConfig)>, Error> {
        Ok(Self::pools_to_fdw_backends(cluster, 0)?
            .into_iter()
            .enumerate()
            .map(|(database_number, backend)| {
                let address = Address {
                    host: "127.0.0.1".into(),
                    port,
                    database_name: backend.database_name.clone(),
                    password: "".into(), // We use trust
                    user: cluster.identifier().user.clone(),
                    database_number,
                };

                (
                    backend.role,
                    PoolConfig {
                        address,
                        config: backend.config,
                    },
                )
            })
            .collect())
    }

    pub(super) fn pools_to_fdw_backends(
        cluster: &Cluster,
        shard: usize,
    ) -> Result<Vec<FdwBackend>, Error> {
        let shard = cluster
            .shards()
            .get(shard)
            .ok_or(Error::ShardsHostsMismatch)?;

        Ok(shard
            .pools_with_roles()
            .into_iter()
            .enumerate()
            .map(|(number, (role, pool))| {
                let database_name = format!("{}_{}", cluster.identifier().database, number);
                FdwBackend {
                    config: pool.config().clone(),
                    address: pool.addr().clone(),
                    database_name,
                    role,
                }
            })
            .collect())
    }

    async fn setup_databases(&mut self, cluster: &Cluster) -> Result<(), Error> {
        let hosts: Vec<_> = cluster
            .shards()
            .iter()
            .map(|shard| {
                let roles: Vec<_> = shard
                    .pools_with_roles()
                    .iter()
                    .map(|(role, _)| role)
                    .cloned()
                    .collect();
                roles
            })
            .collect();
        let identical = hosts.windows(2).all(|w| w.get(0) == w.get(1));
        if !identical {
            return Err(Error::ShardsHostsMismatch);
        }

        let mut admin_connection = self.admin_connection().await?;

        for backend in Self::pools_to_fdw_backends(cluster, 0)? {
            let exists: Vec<String> = admin_connection
                .fetch_all(&format!(
                    "SELECT datname FROM pg_database WHERE datname = '{}'",
                    backend.database_name.replace('\'', "''")
                ))
                .await?;

            if exists.is_empty() {
                admin_connection
                    .execute(format!(
                        "CREATE DATABASE {}",
                        quote_identifier(&backend.database_name)
                    ))
                    .await?;
            }
        }

        let user = cluster.identifier().user.clone();

        let user_exists: Vec<String> = admin_connection
            .fetch_all(&format!(
                "SELECT rolname FROM pg_roles WHERE rolname = '{}'",
                user.replace('\'', "''")
            ))
            .await?;

        if user_exists.is_empty() {
            admin_connection
                .execute(format!(
                    "CREATE USER {} SUPERUSER LOGIN",
                    quote_identifier(&user)
                ))
                .await?;
        }

        Ok(())
    }

    /// Create the same load-balancing and sharding setup we have in pgdog.toml
    /// for this cluster. This function is idempotent.
    pub(crate) async fn configure(&mut self, cluster: &Cluster) -> Result<(), Error> {
        self.setup_databases(cluster).await?;
        let now = Instant::now();

        let cluster_db = cluster.identifier().database.clone();
        let first_setup = !self.configured_databases.contains(&cluster_db);

        info!(
            "[fdw] setting up database={} user={} initial={}",
            cluster.identifier().database,
            cluster.identifier().user,
            first_setup,
        );

        let sharding_schema = cluster.sharding_schema();

        let schema = if first_setup {
            // TODO: Double check schemas are identical on all shards.
            let shard = random_range(0..sharding_schema.shards);
            let mut server = cluster
                .primary_or_replica(shard, &Request::default())
                .await?;
            Some(ForeignTableSchema::load(&mut server).await?)
        } else {
            None
        };

        // Setup persistent connections.
        let mut connections = HashMap::new();

        // We checked that all shards have the same number of replicas.
        let databases: Vec<_> = Self::pools_to_fdw_backends(cluster, 0)?
            .into_iter()
            .map(|backend| backend.database_name)
            .collect();

        for database in &databases {
            let identifier = (cluster.identifier().user.clone(), database.clone());
            let mut connection = self.connection(&identifier.0, database).await?;

            if first_setup {
                // Create extension in a dedicated schema that won't be dropped.
                // This prevents DROP SCHEMA public CASCADE from removing postgres_fdw and its servers.
                connection
                    .execute("CREATE SCHEMA IF NOT EXISTS pgdog_internal")
                    .await?;
                connection
                    .execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw SCHEMA pgdog_internal")
                    .await?;
            }

            connections.insert(identifier, connection);
        }

        // Build server definitions for each database and run setup
        let num_pools = Self::pools_to_fdw_backends(cluster, 0)?.len();
        for pool_position in 0..num_pools {
            let database = format!("{}_{}", cluster.identifier().database, pool_position);
            let identifier = (cluster.identifier().user.clone(), database);
            let mut connection = connections
                .get_mut(&identifier)
                .expect("connection is gone");

            // Collect server definitions for all shards using this pool position
            let mut server_defs = Vec::new();
            for (shard_num, _) in cluster.shards().iter().enumerate() {
                let backends = Self::pools_to_fdw_backends(cluster, shard_num)?;
                if let Some(backend) = backends.get(pool_position) {
                    server_defs.push(FdwServerDef {
                        shard_num,
                        host: backend.address.host.clone(),
                        port: backend.address.port,
                        database_name: backend.address.database_name.clone(),
                        user: backend.address.user.clone(),
                        password: backend.address.password.clone(),
                        mapping_user: cluster.identifier().user.clone(),
                    });
                }
            }

            if first_setup {
                schema
                    .as_ref()
                    .unwrap()
                    .setup(&mut connection, &sharding_schema, &server_defs)
                    .await?;
            } else {
                ForeignTableSchema::setup_user_mappings(&mut connection, &server_defs).await?;
            }
        }

        if first_setup {
            self.configured_databases.insert(cluster_db);
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

    pub(crate) fn configuration_complete(&mut self) {
        self.configured_databases.clear();
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
        let mut receiver = self.shutdown_complete.subscribe();

        // Check if already complete (process may have exited).
        if *receiver.borrow() {
            self.pid.take();
            return;
        }

        // Signal shutdown.
        self.shutdown.send_modify(|v| *v = true);

        // Wait for shutdown to complete.
        while receiver.changed().await.is_ok() {
            if *receiver.borrow() {
                break;
            }
        }
        self.pid.take();
    }

    /// Clear the pid to prevent dirty shutdown warning.
    /// Used when the process has already exited.
    pub(crate) fn clear_pid(&mut self) {
        self.pid.take();
    }
}

impl Drop for PostgresProcess {
    fn drop(&mut self) {
        if let Some(pid) = self.pid.take() {
            warn!("[fdw] dirty shutdown initiated for pid {}", pid);

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

        let mut server = process.connection("pgdog", "pgdog_0").await.unwrap();
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
