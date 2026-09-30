use dashmap::{DashMap, Entry};
use parking_lot::Mutex;
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
    str::FromStr,
    sync::{
        Arc, LazyLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info, trace, warn};

use super::super::Error;
use super::super::ee::{replication_slot_create, replication_slot_error, replication_slot_update};
use crate::config::config;
use crate::{
    backend::{ConnectReason, Server, ServerOptions, pool::Address},
    frontend::client::query_engine::two_pc,
    net::{
        CopyData, CopyDone, DataRow, ErrorResponse, Format, FromBytes, Protocol, Query, ToBytes,
        replication::StatusUpdate,
    },
    util::random_string,
};

use crate::tasks;
use crate::util::safe_timeout;
use pgdog_config::CopyFormat;

pub(crate) use pgdog_stats::Lsn;
use pgdog_stats::TaskId;

static REPLICATION_SLOTS: LazyLock<ReplicationSlots> = LazyLock::new(ReplicationSlots::default);

/// Every replication slot pgdog currently holds as [`Weak`] so the
/// registry never keeps one alive by itself
#[derive(Default, Clone, Debug)]
pub(crate) struct ReplicationSlots {
    slots: Arc<DashMap<String, Weak<ReplicationSlotInner>>>,
}

impl ReplicationSlots {
    fn get() -> Self {
        REPLICATION_SLOTS.clone()
    }

    fn register(slot: &Arc<ReplicationSlotInner>) {
        let registry = Self::get();
        let key = slot.status_key();

        match registry.slots.entry(key) {
            Entry::Occupied(mut entry) => {
                if entry.get().strong_count() == 0 {
                    entry.insert(Arc::downgrade(slot));
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(Arc::downgrade(slot));
            }
        }
    }

    fn remove(slot: &ReplicationSlotInner) {
        Self::get()
            .slots
            .remove_if(&slot.status_key(), |_, weak| weak.strong_count() == 0);
    }

    pub(crate) fn snapshot() -> Vec<pgdog_stats::ReplicationSlot> {
        let registry = Self::get();

        registry.slots.retain(|_, slot| slot.strong_count() > 0);
        let alive: Vec<Arc<ReplicationSlotInner>> = registry
            .slots
            .iter()
            .filter_map(|entry| entry.value().upgrade())
            .collect();

        alive.iter().map(|slot| slot.stats()).collect()
    }
}

/// Long lived slot that could be reacquired multiple times,
/// should be explicitly created and dropped.
#[derive(Debug, Clone)]
pub(crate) struct Permanent;

/// Short lived slot inside the transaction and dropped
/// implicitly when connection is closed.
#[derive(Debug)]
pub(crate) struct Temporary;

/// Representation for the replication slot on PG instance. The slot
/// representation could be shared, but actual stream for updates
/// will be acquired only once.
///
/// Depending on the type K it exposes different behavior:
///
/// - For [`Permanent`] - the actual PG slot is either created with [`Self::create`] or
///   existing slot could be fetched with [`Self::get_existing`], which returns
///   [`ReplicationSlotStream`] that could be acquired only once at a time.
///   The slot should be dropped explicitly with [`Self::drop_slot`]
/// - For [`Temporary`] - the temporary slot is created only with [`Self::create`] and
///   the [`ReplicationSlotStream`] is returned inline that will drop the slot when
///   dropped.
#[derive(Debug, Clone)]
pub(crate) struct ReplicationSlot<K> {
    inner: Arc<ReplicationSlotInner>,
    kind: PhantomData<K>,
}

impl<K> Deref for ReplicationSlot<K> {
    type Target = ReplicationSlotInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl ReplicationSlot<Permanent> {
    /// Create new [`Permanent`] slot that should be managed explicitly.
    pub(crate) fn new_permanent(
        publication: &str,
        address: &Address,
        name: Option<String>,
        shard: usize,
    ) -> Self {
        let name = name.unwrap_or(format!("__pgdog_repl_{}", random_string(18).to_lowercase()));
        let name = format!("{}_{}", name, shard);

        Self::wrap(ReplicationSlotInner {
            address: address.clone(),
            name,
            publication: publication.to_string(),
            available: Arc::new(Semaphore::new(1)),
            temporary: false,
            existing: AtomicBool::new(false),
            remove_on_drop: AtomicBool::new(false),
            status: Mutex::default(),
        })
    }

    pub(crate) async fn create(&self) -> Result<(), Error> {
        self.create_permanent().await
    }

    pub(crate) async fn verify_exists(&self) -> Result<(), Error> {
        let mut server = self.connect().await?;
        let lsn = self.confirmed_lsn(&mut server).await?;
        self.mark_existing();
        self.set_lsn(lsn);

        info!(
            "using existing replication slot \"{}\" at lsn {} [{}]",
            self.name, lsn, self.address
        );

        Ok(())
    }

    pub(crate) async fn reuse_or_create(&self) -> Result<(), Error> {
        match self.verify_exists().await {
            Err(Error::MissingReplicationSlot(_)) => self.create().await,
            result => result,
        }
    }

    async fn create_permanent(&self) -> Result<(), Error> {
        let _permit = self.acquire()?;
        let mut server = self.connect_for_creation().await?;

        let lsn = self
            .execute_creation(&mut server, &self.create_slot_query())
            .await?;
        self.set_lsn(lsn);

        Ok(())
    }

    async fn confirmed_lsn(&self, server: &mut Server) -> Result<Lsn, Error> {
        let existing_slot = format!(
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{}'",
            self.name
        );

        let existing: Option<DataRow> = server.fetch_all(existing_slot).await?.pop();
        let confirmed = existing
            .and_then(|slot| slot.get::<String>(0, Format::Text))
            .ok_or_else(|| Error::MissingReplicationSlot(self.name.clone()))?;

        Ok(Lsn::from_str(&confirmed)?)
    }

    fn acquire(&self) -> Result<OwnedSemaphorePermit, Error> {
        self.inner
            .available
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::SlotInUse(self.name.clone()))
    }

    fn create_slot_query(&self) -> String {
        format!(
            r#"CREATE_REPLICATION_SLOT "{}" LOGICAL "pgoutput" (SNAPSHOT 'nothing')"#,
            self.name
        )
    }

    /// Get the existing remote replication slot.
    ///
    /// If the slot is not present the call will fail.
    pub(crate) async fn get_existing(&self) -> Result<ReplicationSlotGuard, Error> {
        let permit = self.acquire()?;
        let mut server = self.connect().await?;

        let lsn = self.confirmed_lsn(&mut server).await?;

        info!(
            "replication slot \"{}\" opened at confirmed flush lsn {} [{}]",
            self.name, lsn, self.address,
        );

        Ok(ReplicationSlotGuard {
            stream: self.stream(server, lsn),
            _permit: permit,
        })
    }

    /// Remove the slot from the backend.
    ///
    /// Waits until no stream uses the slot, then keeps the permit forever so
    /// no new stream can start.
    pub(crate) async fn drop_slot(self) -> Result<(), Error> {
        if let Ok(permit) = self.inner.available.acquire().await {
            permit.forget();
        }

        self.inner.arm_removal();

        match Arc::try_unwrap(self.inner) {
            Ok(inner) => {
                let result = inner.remove_slot().await;
                if result.is_ok() {
                    inner.disarm_removal();
                }
                result
            }
            Err(inner) => {
                info!(
                    "replication slot \"{}\" is in use, removal deferred [{}]",
                    inner.name, inner.address
                );
                Ok(())
            }
        }
    }
}

impl ReplicationSlot<Temporary> {
    /// Create new [`Temporary`] that is cleaned up automatically on drop.
    pub(crate) fn new_temporary(publication: &str, address: &Address) -> Self {
        let name = format!("__pgdog_{}", random_string(24).to_lowercase());

        Self::wrap(ReplicationSlotInner {
            address: address.clone(),
            name,
            publication: publication.to_string(),
            available: Arc::new(Semaphore::new(1)),
            temporary: true,
            existing: AtomicBool::new(false),
            remove_on_drop: AtomicBool::new(false),
            status: Mutex::default(),
        })
    }

    /// Start the transaction and create the replication slot for it.
    pub(crate) async fn create(self) -> Result<ReplicationSlotStream, Error> {
        let mut server = self.connect_for_creation().await?;

        server
            .execute("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ")
            .await?;

        let create_slot = format!(
            r#"CREATE_REPLICATION_SLOT "{}" TEMPORARY LOGICAL "pgoutput" (SNAPSHOT 'use')"#,
            self.name
        );
        let lsn = self.execute_creation(&mut server, &create_slot).await?;

        Ok(self.stream(server, lsn))
    }
}

impl<K> ReplicationSlot<K> {
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn addr(&self) -> &Address {
        &self.address
    }

    fn wrap(inner: ReplicationSlotInner) -> Self {
        let inner = Arc::new(inner);

        ReplicationSlots::register(&inner);
        replication_slot_create(&inner.stats());

        Self {
            inner,
            kind: PhantomData,
        }
    }

    async fn connect_for_creation(&self) -> Result<Server, Error> {
        let mut server = self.connect().await?;

        debug!(
            "creating replication slot \"{}\" [{}]",
            self.name, self.address
        );

        let two_pc = two_pc::server_transactions::load(&mut server).await?;
        if !two_pc.is_empty() {
            warn!(
                "{} open two-phase transactions, this may block replication slot creation [{}]",
                two_pc.len(),
                server.addr()
            );
        }

        Ok(server)
    }

    async fn execute_creation(&self, server: &mut Server, query: &str) -> Result<Lsn, Error> {
        let mut result = server.fetch_all::<DataRow>(query).await?;
        let result = result.pop().ok_or(Error::MissingData)?;
        let lsn = result
            .get::<String>(1, Format::Text)
            .ok_or(Error::MissingData)?;
        let lsn = Lsn::from_str(&lsn)?;

        info!(
            "replication slot \"{}\" at lsn {} created [{}]",
            self.name, lsn, self.address,
        );

        Ok(lsn)
    }

    async fn connect(&self) -> Result<Server, Error> {
        connect_replication(&self.address).await
    }

    fn stream(&self, server: Server, lsn: Lsn) -> ReplicationSlotStream {
        self.inner.set_lsn(lsn);

        ReplicationSlotStream {
            slot: self.inner.clone(),
            stopped: false,
            server,
            meta_server: None,
        }
    }
}

/// The slot progress
#[derive(Debug, Default)]
struct SlotStatus {
    lsn: Lsn,
    lag: i64,
    last_transaction: Option<SystemTime>,
    task_id: Option<TaskId>,
}

#[derive(Debug)]
pub(crate) struct ReplicationSlotInner {
    address: Address,
    name: String,
    publication: String,
    available: Arc<Semaphore>,
    temporary: bool,
    existing: AtomicBool,
    remove_on_drop: AtomicBool,
    status: Mutex<SlotStatus>,
}

impl Drop for ReplicationSlotInner {
    fn drop(&mut self) {
        ReplicationSlots::remove(self);

        // escape hatch to remove the actual slot, if we haven't managed to do
        // it during the explicit run.
        if self.remove_on_drop.load(Ordering::Acquire) {
            let removal = self.remove_slot();
            tasks::spawn("replication slot removal", async move {
                if let Err(err) = removal.await {
                    warn!("failed to remove deferred replication slot: {err}");
                }
            });
        }
    }
}

impl ReplicationSlotInner {
    pub(crate) fn remove_slot(&self) -> impl Future<Output = Result<(), Error>> + Send + use<> {
        let address = self.address.clone();
        let name = self.name.clone();
        async move {
            let mut server = connect_replication(&address).await?;
            server
                .execute_checked(&format!(r#"DROP_REPLICATION_SLOT "{}" WAIT"#, name))
                .await?;
            warn!("replication slot \"{}\" dropped [{}]", name, address);
            Ok(())
        }
    }

    /// Make the destructor drop the backend slot if the explicit removal
    /// never completes.
    fn arm_removal(&self) {
        self.remove_on_drop.store(true, Ordering::Release);
    }

    fn disarm_removal(&self) {
        self.remove_on_drop.store(false, Ordering::Release);
    }

    /// Whether the slot was already on the backend, so this process reused
    /// it instead of creating it.
    pub(crate) fn existing(&self) -> bool {
        self.existing.load(Ordering::Acquire)
    }

    fn mark_existing(&self) {
        self.existing.store(true, Ordering::Release);
    }

    fn status_key(&self) -> String {
        format!("{}@{}", self.name, self.address)
    }

    /// Snapshot of what `SHOW REPLICATION_SLOTS` reports for this slot.
    pub(crate) fn stats(&self) -> pgdog_stats::ReplicationSlot {
        let status = self.status.lock();

        pgdog_stats::ReplicationSlot {
            name: self.name.clone(),
            lsn: status.lsn,
            temporary: self.temporary,
            existing: self.existing(),
            lag: status.lag,
            address: self.address.clone().into(),
            last_transaction: status.last_transaction,
            task_id: status.task_id,
        }
    }

    pub(crate) fn lsn(&self) -> Lsn {
        self.status.lock().lsn
    }

    fn set_lsn(&self, lsn: Lsn) {
        self.status.lock().lsn = lsn;
    }

    fn advance_lsn(&self, lsn: Lsn) {
        {
            let mut status = self.status.lock();
            status.lsn = lsn;
            status.last_transaction = Some(SystemTime::now());
        }
        replication_slot_update(&self.stats());
    }

    pub(crate) fn set_task_id(&self, task_id: TaskId) {
        self.status.lock().task_id = Some(task_id);
        replication_slot_update(&self.stats());
    }

    fn set_lag(&self, lag: i64) {
        self.status.lock().lag = lag;
        replication_slot_update(&self.stats());
    }
}

async fn connect_replication(address: &Address) -> Result<Server, Error> {
    Ok(Box::pin(Server::connect(
        address,
        ServerOptions::new_replication(),
        ConnectReason::Resharding,
        Default::default(),
    ))
    .await?)
}

/// Slot stream to manage update
#[derive(Debug)]
pub(crate) struct ReplicationSlotStream {
    slot: Arc<ReplicationSlotInner>,
    /// CopyDone sent — our copy-in half is closed, so no further CopyData
    /// (status updates included) may be written until the stream restarts.
    stopped: bool,
    server: Server,
    /// Separate connection used to fetch meta info like replication_lag
    meta_server: Option<Server>,
}

#[derive(Debug, Clone)]
pub(crate) enum ReplicationData {
    CopyData(CopyData),
    CopyDone,
}

impl ReplicationSlotStream {
    pub(crate) fn server(&mut self) -> &mut Server {
        &mut self.server
    }

    /// Get or create a separate server connection for meta commands.
    async fn meta_server(&mut self) -> Result<&mut Server, Error> {
        if self.meta_server.is_none() {
            self.meta_server = Some(
                Server::connect(
                    &self.slot.address,
                    ServerOptions::default(),
                    ConnectReason::Resharding,
                    Default::default(),
                )
                .await?,
            );
        }
        Ok(self
            .meta_server
            .as_mut()
            .expect("metadata connection is established"))
    }

    /// Replication lag in bytes for this slot.
    pub(crate) async fn replication_lag(&mut self) -> Result<i64, Error> {
        let lag = self.query_replication_lag().await;

        if lag.is_err() {
            self.meta_server = None;
        }

        lag
    }

    async fn query_replication_lag(&mut self) -> Result<i64, Error> {
        let query = format!(
            "SELECT pg_current_wal_lsn() - confirmed_flush_lsn \
             FROM pg_replication_slots \
             WHERE slot_name = '{}'",
            self.slot.name
        );
        let mut lag: Vec<i64> = self.meta_server().await?.fetch_all(&query).await?;

        let lag = lag
            .pop()
            .ok_or(Error::MissingReplicationSlot(self.slot.name.clone()))?;

        self.slot.set_lag(lag);

        Ok(lag)
    }

    /// Start replication.
    pub(crate) async fn start_replication(&mut self) -> Result<(), Error> {
        // Fresh copy stream (including after a reconnect): re-enable status updates.
        self.stopped = false;
        let is_binary = config().config.general.resharding_copy_format == CopyFormat::Binary;
        // TODO: This is definitely Postgres version-specific.
        let query = Query::new(format!(
            r#"START_REPLICATION SLOT "{}" LOGICAL {} ("proto_version" '4', origin 'any', "publication_names" '"{}"', "binary" '{}')"#,
            self.slot.name,
            self.slot.lsn(),
            self.slot.publication,
            is_binary
        ));
        self.server().send(&vec![query.into()].into()).await?;

        let copy_both = self.server().read().await?;

        match copy_both.code() {
            'E' => return Err(ErrorResponse::from_bytes(copy_both.to_bytes())?.into()),
            'W' => (),
            c => return Err(Error::OutOfSync(c)),
        }

        debug!(
            "replication from slot \"{}\" started [{}]",
            self.slot.name, self.slot.address
        );

        Ok(())
    }

    /// Replicate from slot until finished.
    pub(crate) async fn replicate(
        &mut self,
        max_wait: Duration,
    ) -> Result<Option<ReplicationData>, Error> {
        loop {
            let message = match safe_timeout(max_wait, self.server().read()).await {
                Err(_err) => return Err(Error::ReplicationTimeout),
                Ok(message) => message?,
            };

            match message.code() {
                'd' => {
                    let copy_data = CopyData::from_bytes(message.to_bytes())?;
                    trace!("{:?} [{}]", copy_data, self.slot.address);

                    return Ok(Some(ReplicationData::CopyData(copy_data)));
                }
                'C' => (),
                'c' => return Ok(Some(ReplicationData::CopyDone)), // CopyDone.
                'Z' => {
                    debug!(
                        "slot \"{}\" drained [{}]",
                        self.slot.name, self.slot.address
                    );
                    return Ok(None);
                }
                'E' => {
                    let error = ErrorResponse::from_bytes(message.to_bytes())?;
                    replication_slot_error(&self.slot.stats(), &error);

                    return Err(error.into());
                }
                c => return Err(Error::OutOfSync(c)),
            }
        }
    }

    /// Update origin on last flushed LSN.
    pub(crate) async fn status_update(&mut self, status_update: StatusUpdate) -> Result<(), Error> {
        // Once CopyDone is sent, our copy-in half is closed and any further
        // CopyData would violate the copy protocol. The origin resends
        // unconfirmed WAL on reconnect and our applies are idempotent, so
        // dropping the confirm during teardown is safe.
        if self.stopped {
            return Ok(());
        }

        debug!(
            "confirmed {} flushed [{}]",
            status_update.last_flushed,
            self.server().addr()
        );

        let lsn = Lsn::from_i64(status_update.last_flushed);
        self.slot.advance_lsn(lsn);

        self.server()
            .send_one(&status_update.wrapped()?.into())
            .await?;
        self.server().flush().await?;

        Ok(())
    }

    /// Ask remote to close stream.
    pub(crate) async fn stop_replication(&mut self) -> Result<(), Error> {
        if self.stopped {
            return Ok(());
        }

        self.server().send_one(&CopyDone.into()).await?;
        self.server().flush().await?;
        self.stopped = true;

        Ok(())
    }

    pub(crate) fn stopped(&self) -> bool {
        self.stopped
    }

    /// Current slot LSN.
    pub(crate) fn lsn(&self) -> Lsn {
        self.slot.lsn()
    }

    pub(crate) fn name(&self) -> &str {
        &self.slot.name
    }

    pub(crate) fn addr(&self) -> &Address {
        &self.slot.address
    }
}

/// Guard for [`ReplicationSlotStream`] to track single use.
/// On drop this allows to use the slot stream again or drop the slot.
#[derive(Debug)]
pub(crate) struct ReplicationSlotGuard {
    stream: ReplicationSlotStream,
    // track permit to prevent double use of the same replication stream
    _permit: OwnedSemaphorePermit,
}

impl Deref for ReplicationSlotGuard {
    type Target = ReplicationSlotStream;

    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}

impl DerefMut for ReplicationSlotGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stream
    }
}

impl ReplicationSlotGuard {
    /// Reconnect to the remote - to make new attempts after errors.
    pub(crate) async fn reconnect(&mut self) -> Result<(), Error> {
        let stopped = self.stream.stopped;
        let address = self.stream.slot.address.clone();

        self.stream.server.terminate();
        self.stream.server = connect_replication(&address).await?;
        self.stream.start_replication().await?;

        if stopped {
            self.stream.stop_replication().await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use tokio::spawn;

    use crate::{
        backend::server::test::test_server,
        net::replication::xlog_data::{XLogData, XLogPayload},
    };

    use super::*;

    async fn slot_count(server: &mut Server, name: &str) -> i64 {
        let rows: Vec<i64> = server
            .fetch_all(format!(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{name}'"
            ))
            .await
            .unwrap();
        rows[0]
    }

    async fn wait_for_removal(server: &mut Server, name: &str) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while slot_count(server, name).await != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    impl ReplicationData {
        fn xlog_data(&self) -> Option<XLogData> {
            if let Self::CopyData(copy_data) = self {
                copy_data.xlog_data()
            } else {
                None
            }
        }
    }

    #[test]
    fn test_lsn() {
        let original = "1/12A4C"; // It's fine.
        let lsn = Lsn::from_str(original).unwrap();
        assert_eq!(lsn.high, 1);
        let lsn = lsn.to_string();
        assert_eq!(lsn, original);
    }

    #[tokio::test]
    async fn test_real_lsn() {
        let result: Vec<String> = test_server()
            .await
            .fetch_all("SELECT pg_current_wal_lsn()")
            .await
            .unwrap();
        let lsn = Lsn::from_str(&result[0]).unwrap();
        let lsn_2 = Lsn::from_i64(lsn.lsn);
        assert_eq!(lsn.to_string(), result[0]);
        assert_eq!(lsn, lsn_2);
    }

    mod permanent {
        use super::*;

        #[tokio::test]
        async fn create_registers_permanent_slot() {
            let mut server = test_server().await;
            let slot = ReplicationSlot::new_permanent("test_slot_create", server.addr(), None, 0);
            let name = slot.name().to_owned();
            slot.create().await.unwrap();

            let properties: Vec<String> = server
                .fetch_all(format!(
                    "SELECT slot_type || ':' || plugin || ':' || temporary::text \
                     FROM pg_replication_slots WHERE slot_name = '{name}'"
                ))
                .await
                .unwrap();
            assert_eq!(properties, vec!["logical:pgoutput:false"]);

            slot.drop_slot().await.unwrap();
            assert_eq!(slot_count(&mut server, &name).await, 0);
        }

        #[tokio::test]
        async fn status_registry_follows_the_slot() {
            let mut server = test_server().await;
            let slot = ReplicationSlot::new_permanent(
                "test_slot_status",
                server.addr(),
                Some("test_slot_status".into()),
                0,
            );
            let name = slot.name().to_owned();
            let _ = slot.inner.remove_slot().await;
            slot.create().await.unwrap();

            assert_ne!(slot.lsn(), Lsn::default());

            let stream = slot.get_existing().await.unwrap();
            let reported = ReplicationSlots::snapshot()
                .into_iter()
                .find(|entry| entry.name == name)
                .expect("an open slot is reported");
            assert_eq!(reported.lsn, stream.lsn());
            assert!(!reported.temporary);
            assert!(!reported.existing);

            drop(stream);
            slot.drop_slot().await.unwrap();
            assert_eq!(slot_count(&mut server, &name).await, 0);
            assert!(
                ReplicationSlots::snapshot()
                    .into_iter()
                    .all(|entry| entry.name != name)
            );
        }

        #[tokio::test]
        async fn test_slot_replication() {
            use tokio::sync::mpsc::*;
            crate::logger();

            let mut server = test_server().await;

            server
                .execute("CREATE TABLE IF NOT EXISTS public.test_slot_replication(id BIGINT)")
                .await
                .unwrap();
            let _ = server
                .execute("DROP PUBLICATION test_slot_replication")
                .await;
            server
            .execute(
                "CREATE PUBLICATION test_slot_replication FOR TABLE public.test_slot_replication",
            )
            .await
            .unwrap();

            let addr = server.addr();

            let slot = ReplicationSlot::new_permanent(
                "test_slot_replication",
                addr,
                Some("test_slot_replication".into()),
                0,
            );
            let _ = slot.inner.remove_slot().await;
            slot.create().await.unwrap();
            let mut stream = slot.get_existing().await.unwrap();

            let (tx, mut rx) = channel(16);

            let handle = spawn(async move {
                stream.start_replication().await?;
                server
                    .execute("INSERT INTO test_slot_replication (id) VALUES (1)")
                    .await?;

                loop {
                    let message = stream.replicate(Duration::MAX).await?;
                    tx.send(message.clone()).await.unwrap();

                    if let Some(message) = message {
                        match message.clone() {
                            ReplicationData::CopyData(copy_data) => {
                                if let Some(xlog_data) = copy_data.xlog_data()
                                    && let Some(XLogPayload::Commit(_)) = xlog_data.payload()
                                {
                                    stream.stop_replication().await?;
                                }
                            }
                            ReplicationData::CopyDone => (),
                        }
                    } else {
                        break;
                    }
                }

                drop(stream);
                slot.drop_slot().await?;

                Ok::<(), Error>(())
            });

            let mut got_row = false;

            while let Some(message) = rx.recv().await {
                let payload = message
                    .and_then(|message| message.xlog_data())
                    .and_then(|payload| payload.payload());
                if let Some(payload) = payload {
                    match payload {
                        XLogPayload::Relation(relation) => {
                            assert_eq!(relation.name, "test_slot_replication")
                        }
                        XLogPayload::Insert(insert) => {
                            let col = insert.tuple_data.columns.first().unwrap();
                            let id = i64::from_be_bytes(col.data[..].try_into().unwrap());
                            assert_eq!(id, 1);
                        }
                        XLogPayload::Begin(_) => (),
                        XLogPayload::Commit(_) => got_row = true,
                        _ => panic!("{:#?}", payload),
                    }
                }
            }

            assert!(got_row);

            handle.await.unwrap().unwrap();
        }

        #[tokio::test]
        async fn create_rejects_existing_slot() {
            let mut server = test_server().await;
            let slot = ReplicationSlot::new_permanent(
                "test_slot_conflict",
                server.addr(),
                Some("test_slot_conflict".into()),
                0,
            );
            let name = slot.name().to_owned();
            let _ = slot.inner.remove_slot().await;
            slot.create().await.unwrap();

            let conflicting = ReplicationSlot::new_permanent(
                "test_slot_conflict",
                server.addr(),
                Some("test_slot_conflict".into()),
                0,
            );
            assert!(matches!(
                conflicting.create().await,
                Err(Error::Backend(crate::backend::Error::ExecutionError(error)))
                    if error.code == "42710"
            ));
            assert_eq!(slot_count(&mut server, &name).await, 1);

            slot.drop_slot().await.unwrap();
            assert_eq!(slot_count(&mut server, &name).await, 0);
        }

        #[tokio::test]
        async fn verify_exists_does_not_touch_the_slot() {
            let mut server = test_server().await;
            let owner = ReplicationSlot::new_permanent(
                "test_slot_verify",
                server.addr(),
                Some("test_slot_verify".into()),
                0,
            );
            let name = owner.name().to_owned();
            let _ = owner.inner.remove_slot().await;
            owner.create().await.unwrap();

            let opened = ReplicationSlot::new_permanent(
                "test_slot_verify",
                server.addr(),
                Some("test_slot_verify".into()),
                0,
            );
            opened.verify_exists().await.unwrap();
            drop(opened);
            assert_eq!(slot_count(&mut server, &name).await, 1);

            owner.drop_slot().await.unwrap();
            assert_eq!(slot_count(&mut server, &name).await, 0);
        }

        #[tokio::test]
        async fn test_verify_exists_reports_missing_slot() {
            let server = test_server().await;
            let slot = ReplicationSlot::new_permanent("test_slot_absent", server.addr(), None, 0);
            let name = slot.name().to_owned();

            assert!(matches!(
                slot.verify_exists().await,
                Err(Error::MissingReplicationSlot(missing)) if missing == name
            ));
        }

        #[tokio::test]
        async fn reuse_or_create_creates_missing_slot() {
            let mut server = test_server().await;
            let slot = ReplicationSlot::new_permanent(
                "test_slot_reuse_missing",
                server.addr(),
                Some("test_slot_reuse_missing".into()),
                0,
            );
            let name = slot.name().to_owned();
            let _ = slot.inner.remove_slot().await;

            slot.reuse_or_create().await.unwrap();
            assert_eq!(slot_count(&mut server, &name).await, 1);

            slot.drop_slot().await.unwrap();
            assert_eq!(slot_count(&mut server, &name).await, 0);
        }

        #[tokio::test]
        async fn reuse_or_create_keeps_existing_slot() {
            let mut server = test_server().await;
            let owner = ReplicationSlot::new_permanent(
                "test_slot_reuse_existing",
                server.addr(),
                Some("test_slot_reuse_existing".into()),
                0,
            );
            let name = owner.name().to_owned();
            let _ = owner.inner.remove_slot().await;
            owner.create().await.unwrap();

            let reused = ReplicationSlot::new_permanent(
                "test_slot_reuse_existing",
                server.addr(),
                Some("test_slot_reuse_existing".into()),
                0,
            );
            reused.reuse_or_create().await.unwrap();
            drop(reused);
            assert_eq!(slot_count(&mut server, &name).await, 1);

            owner.drop_slot().await.unwrap();
            assert_eq!(slot_count(&mut server, &name).await, 0);
        }

        #[tokio::test]
        async fn reuse_or_create_succeeds_while_stream_is_open() {
            let mut server = test_server().await;
            let slot = ReplicationSlot::new_permanent(
                "test_slot_reuse_busy",
                server.addr(),
                Some("test_slot_reuse_busy".into()),
                0,
            );
            let name = slot.name().to_owned();
            let _ = slot.inner.remove_slot().await;
            slot.create().await.unwrap();
            let stream = slot.get_existing().await.unwrap();

            slot.reuse_or_create().await.unwrap();
            assert_eq!(slot_count(&mut server, &name).await, 1);

            drop(stream);
            slot.drop_slot().await.unwrap();
            assert_eq!(slot_count(&mut server, &name).await, 0);
        }

        #[tokio::test]
        async fn test_drop_slot_reports_backend_error() {
            let server = test_server().await;
            let slot =
                ReplicationSlot::new_permanent("test_slot_drop_error", server.addr(), None, 0);
            assert!(matches!(
                slot.drop_slot().await,
                Err(Error::Backend(crate::backend::Error::ExecutionError(error)))
                    if error.code == "42704"
            ));
        }

        #[tokio::test]
        async fn get_existing_does_not_create_missing_slot() {
            let mut server = test_server().await;
            let slot = ReplicationSlot::new_permanent("test_slot_missing", server.addr(), None, 0);
            let name = slot.name().to_owned();

            assert!(matches!(
                slot.get_existing().await,
                Err(Error::MissingReplicationSlot(missing)) if missing == name
            ));
            assert_eq!(slot_count(&mut server, &name).await, 0);
        }

        #[tokio::test]
        async fn get_existing_uses_backend_position() {
            let mut server = test_server().await;
            let slot = ReplicationSlot::new_permanent("test_slot_open", server.addr(), None, 0);
            slot.create().await.unwrap();
            let positions: Vec<String> = server
                .fetch_all(format!(
                    "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = '{}'",
                    slot.name()
                ))
                .await
                .unwrap();

            let stream = slot.get_existing().await.unwrap();
            assert_eq!(stream.lsn(), Lsn::from_str(&positions[0]).unwrap());
            drop(stream);
            slot.drop_slot().await.unwrap();
        }

        #[tokio::test]
        async fn simultaneous_streams_fail_without_waiting() {
            let server = test_server().await;
            let slot = ReplicationSlot::new_permanent("test_slot_busy", server.addr(), None, 0);
            slot.create().await.unwrap();
            let stream = slot.get_existing().await.unwrap();
            let cloned = slot.clone();
            let attempted = tokio::time::timeout(Duration::from_secs(1), cloned.get_existing())
                .await
                .expect("busy slot acquisition must not wait");
            assert!(matches!(
                attempted,
                Err(Error::SlotInUse(name)) if name == slot.name()
            ));
            drop(stream);
            let reopened = cloned.get_existing().await.unwrap();
            drop(reopened);
            drop(cloned);
            slot.drop_slot().await.unwrap();
        }

        #[tokio::test]
        async fn create_fails_while_stream_is_in_use() {
            let server = test_server().await;
            let slot =
                ReplicationSlot::new_permanent("test_slot_create_busy", server.addr(), None, 0);
            slot.create().await.unwrap();
            let stream = slot.get_existing().await.unwrap();
            let attempted = tokio::time::timeout(Duration::from_secs(1), slot.create())
                .await
                .expect("busy slot creation must not wait");
            assert!(matches!(
                attempted,
                Err(Error::SlotInUse(name)) if name == slot.name()
            ));
            drop(stream);
            slot.drop_slot().await.unwrap();
        }

        #[tokio::test]
        async fn drop_waits_for_guard_to_release_slot() {
            let mut server = test_server().await;
            let slot = ReplicationSlot::new_permanent(
                "test_slot_drop_busy",
                server.addr(),
                Some("test_slot_drop_busy".into()),
                0,
            );
            let name = slot.name().to_owned();
            slot.create().await.unwrap();
            let stream = slot.get_existing().await.unwrap();

            let removal = tokio::spawn(async move { slot.drop_slot().await });
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;

            assert!(!removal.is_finished());
            assert_eq!(slot_count(&mut server, &name).await, 1);

            drop(stream);
            removal.await.unwrap().unwrap();
            wait_for_removal(&mut server, &name).await;
        }

        #[tokio::test]
        async fn drop_waits_for_other_handle_to_release_slot() {
            let mut server = test_server().await;
            let slot = ReplicationSlot::new_permanent("test_slot_shared", server.addr(), None, 0);
            let name = slot.name().to_owned();
            slot.create().await.unwrap();
            let cloned = slot.clone();

            cloned.drop_slot().await.unwrap();
            assert_eq!(slot_count(&mut server, &name).await, 1);

            drop(slot);
            wait_for_removal(&mut server, &name).await;
        }
    }

    mod temporary {
        use super::*;

        #[tokio::test]
        async fn create_rejects_existing_slot() {
            let mut server = test_server().await;
            let slot = ReplicationSlot::new_temporary("test_temporary_duplicate", server.addr());
            let name = slot.name().to_owned();
            let mut existing = connect_replication(server.addr()).await.unwrap();
            existing
                .execute_checked(format!(
                    r#"CREATE_REPLICATION_SLOT "{name}" TEMPORARY LOGICAL "pgoutput" (SNAPSHOT 'nothing')"#
                ))
                .await
                .unwrap();

            assert!(matches!(
                slot.create().await,
                Err(Error::Backend(crate::backend::Error::ExecutionError(error)))
                    if error.code == "42710"
            ));
            assert_eq!(slot_count(&mut server, &name).await, 1);
            drop(existing);
            wait_for_removal(&mut server, &name).await;
        }

        #[tokio::test]
        async fn dropping_stream_removes_slot() {
            let mut server = test_server().await;
            let slot = ReplicationSlot::new_temporary("test_temporary_cleanup", server.addr());
            let name = slot.name().to_owned();
            let stream = slot.create().await.unwrap();
            assert_eq!(slot_count(&mut server, &name).await, 1);
            drop(stream);
            wait_for_removal(&mut server, &name).await;
        }

        #[tokio::test]
        async fn aborting_stream_owner_removes_slot() {
            let mut server = test_server().await;
            let slot = ReplicationSlot::new_temporary("test_temporary_abort", server.addr());
            let name = slot.name().to_owned();
            let stream = slot.create().await.unwrap();
            assert_eq!(slot_count(&mut server, &name).await, 1);

            let task = spawn(async move {
                let _stream = stream;
                std::future::pending::<()>().await;
            });
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            wait_for_removal(&mut server, &name).await;
        }

        #[tokio::test]
        async fn test_temporary_creation_retains_snapshot_connection() {
            let mut server = test_server().await;
            let slot = ReplicationSlot::new_temporary("test_temporary_snapshot", server.addr());
            let name = slot.name().to_owned();
            let mut stream = slot.create().await.unwrap();
            let temporary: Vec<String> = server
                .fetch_all(format!(
                    "SELECT temporary::text FROM pg_replication_slots WHERE slot_name = '{name}'"
                ))
                .await
                .unwrap();
            assert_eq!(temporary, vec!["true"]);
            let read_only: Vec<String> = stream
                .server()
                .fetch_all("SHOW transaction_read_only")
                .await
                .unwrap();
            assert_eq!(read_only, vec!["on"]);
            let isolation: Vec<String> = stream
                .server()
                .fetch_all("SHOW transaction_isolation")
                .await
                .unwrap();
            assert_eq!(isolation, vec!["repeatable read"]);
        }
    }
}
