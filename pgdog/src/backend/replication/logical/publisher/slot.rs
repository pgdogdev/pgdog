use super::super::status::ReplicationSlot as ReplicationSlotTracker;
use super::super::Error;
use crate::{
    backend::{self, pool::Address, ConnectReason, Server, ServerOptions},
    net::{
        replication::{StatusUpdate, XLogData},
        CopyData, CopyDone, DataRow, ErrorResponse, Format, FromBytes, Protocol, Query, ToBytes,
    },
    util::random_string,
};
use std::{fmt::Display, str::FromStr, time::Duration};
use tokio::time::timeout;
use tracing::{debug, info, trace, warn};

pub use pgdog_stats::Lsn;

#[derive(Debug, Clone, Copy)]
pub enum Snapshot {
    Export,
    Use,
    Nothing,
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum SlotKind {
    DataSync,
    Replication,
}

impl Display for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Export => write!(f, "snapshot"),
            Self::Use => write!(f, "use"),
            Self::Nothing => write!(f, "nothing"),
        }
    }
}

#[derive(Debug)]
pub struct ReplicationSlot {
    address: Address,
    publication: String,
    name: String,
    snapshot: Snapshot,
    lsn: Lsn,
    dropped: bool,
    server: Option<Server>,
    kind: SlotKind,
    server_meta: Option<Server>,
    tracker: Option<ReplicationSlotTracker>,
}

impl ReplicationSlot {
    /// Create replication slot used for streaming the WAL.
    pub fn replication(
        publication: &str,
        address: &Address,
        name: Option<String>,
        shard: usize,
    ) -> Self {
        let name = name.unwrap_or(format!("__pgdog_repl_{}", random_string(18).to_lowercase()));
        let name = format!("{}_{}", name, shard);

        Self {
            address: address.clone(),
            name: name.to_string(),
            snapshot: Snapshot::Nothing,
            lsn: Lsn::default(),
            publication: publication.to_string(),
            dropped: false,
            server: None,
            kind: SlotKind::Replication,
            server_meta: None,
            tracker: None,
        }
    }

    /// Create replication slot for data sync.
    pub fn data_sync(publication: &str, address: &Address) -> Self {
        let name = format!("__pgdog_{}", random_string(24).to_lowercase());

        Self {
            address: address.clone(),
            name,
            snapshot: Snapshot::Use,
            lsn: Lsn::default(),
            publication: publication.to_string(),
            dropped: true, // Temporary.
            server: None,
            kind: SlotKind::DataSync,
            server_meta: None,
            tracker: None,
        }
    }

    /// Connect to database using replication mode.
    pub async fn connect(&mut self) -> Result<(), Error> {
        self.server = Some(
            Server::connect(
                &self.address,
                ServerOptions::new_replication(),
                ConnectReason::Replication,
            )
            .await?,
        );

        Ok(())
    }

    /// Get or create a separate server connection for meta commands.
    pub async fn server_meta(&mut self) -> Result<&mut Server, Error> {
        if let Some(ref mut server) = self.server_meta {
            Ok(server)
        } else {
            self.server_meta = Some(
                Server::connect(
                    &self.address,
                    ServerOptions::default(),
                    ConnectReason::Replication,
                )
                .await?,
            );
            Ok(self.server_meta.as_mut().unwrap())
        }
    }

    /// Replication lag in bytes for this slot.
    pub async fn replication_lag(&mut self) -> Result<i64, Error> {
        let query = format!(
            "SELECT pg_current_wal_lsn() - confirmed_flush_lsn \
             FROM pg_replication_slots \
             WHERE slot_name = '{}'",
            self.name
        );
        let mut lag: Vec<i64> = self.server_meta().await?.fetch_all(&query).await?;

        let lag = lag
            .pop()
            .ok_or(Error::MissingReplicationSlot(self.name.clone()))?;

        if let Some(ref tracker) = self.tracker {
            tracker.update_lag(lag);
        }

        Ok(lag)
    }

    pub fn server(&mut self) -> Result<&mut Server, Error> {
        self.server.as_mut().ok_or(Error::NotConnected)
    }

    /// Create the slot.
    pub async fn create_slot(&mut self) -> Result<Lsn, Error> {
        if self.server.is_none() {
            self.connect().await?;
        }

        info!(
            "creating replication slot \"{}\" [{}]",
            self.name, self.address
        );

        if self.kind == SlotKind::DataSync {
            self.server()?
                .execute("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ")
                .await?;
        }

        let start_replication = format!(
            r#"CREATE_REPLICATION_SLOT "{}" {} LOGICAL "pgoutput" (SNAPSHOT '{}')"#,
            self.name,
            if self.kind == SlotKind::DataSync {
                "TEMPORARY"
            } else {
                ""
            },
            self.snapshot
        );

        let existing_slot = format!(
            "
            SELECT
                slot_name,
                restart_lsn,
                confirmed_flush_lsn
            FROM
                pg_replication_slots
            WHERE slot_name = '{}'",
            self.name
        );

        match self
            .server()?
            .fetch_all::<DataRow>(&start_replication)
            .await
        {
            Ok(mut result) => {
                let result = result.pop().ok_or(Error::MissingData)?;
                let lsn = result
                    .get::<String>(1, Format::Text)
                    .ok_or(Error::MissingData)?;
                let lsn = Lsn::from_str(&lsn)?;
                self.lsn = lsn;
                self.tracker = Some(ReplicationSlotTracker::new(
                    &self.name,
                    &self.lsn,
                    self.dropped,
                    &self.address,
                ));

                info!(
                    "replication slot \"{}\" at lsn {} created [{}]",
                    self.name, self.lsn, self.address,
                );

                Ok(lsn)
            }

            Err(err) => match err {
                backend::Error::ExecutionError(err) => {
                    // duplicate object.
                    if err.code == "42710" {
                        let exists: Option<DataRow> =
                            self.server()?.fetch_all(existing_slot).await?.pop();

                        if let Some(lsn) =
                            exists.and_then(|slot| slot.get::<String>(2, Format::Text))
                        {
                            let lsn = Lsn::from_str(&lsn)?;
                            self.lsn = lsn;
                            self.tracker = Some(ReplicationSlotTracker::new(
                                &self.name,
                                &self.lsn,
                                self.dropped,
                                &self.address,
                            ));

                            info!(
                                "using existing replication slot \"{}\" at lsn {} [{}]",
                                self.name, self.lsn, self.address,
                            );

                            Ok(lsn)
                        } else {
                            Err(Error::MissingReplicationSlot(self.name.clone()))
                        }
                    } else {
                        Err(backend::Error::ExecutionError(err).into())
                    }
                }

                err => Err(err.into()),
            },
        }
    }

    /// Drop the slot.
    pub async fn drop_slot(&mut self) -> Result<(), Error> {
        let drop_slot = self.drop_slot_query(true);
        self.server()?.execute(&drop_slot).await?;

        warn!(
            "replication slot \"{}\" dropped [{}]",
            self.name, self.address
        );
        self.dropped = true;
        if let Some(slot) = self.tracker.take() {
            slot.dropped()
        }

        Ok(())
    }

    fn drop_slot_query(&self, wait: bool) -> String {
        format!(
            r#"DROP_REPLICATION_SLOT "{}" {}"#,
            self.name,
            if wait { "WAIT" } else { "" }
        )
    }

    /// Start replication.
    pub async fn start_replication(&mut self) -> Result<(), Error> {
        // TODO: This is definitely Postgres version-specific.
        let query = Query::new(format!(
            r#"START_REPLICATION SLOT "{}" LOGICAL {} ("proto_version" '4', origin 'any', "publication_names" '"{}"')"#,
            self.name, self.lsn, self.publication
        ));
        self.server()?.send(&vec![query.into()].into()).await?;

        let copy_both = self.server()?.read().await?;

        match copy_both.code() {
            'E' => return Err(ErrorResponse::from_bytes(copy_both.to_bytes()?)?.into()),
            'W' => (),
            c => return Err(Error::OutOfSync(c)),
        }

        debug!(
            "replication from slot \"{}\" started [{}]",
            self.name, self.address
        );

        Ok(())
    }

    /// Replicate from slot until finished.
    pub async fn replicate(
        &mut self,
        max_wait: Duration,
    ) -> Result<Option<ReplicationData>, Error> {
        loop {
            let message = match timeout(max_wait, self.server()?.read()).await {
                Err(_err) => return Err(Error::ReplicationTimeout),
                Ok(message) => message?,
            };

            match message.code() {
                'd' => {
                    let copy_data = CopyData::from_bytes(message.to_bytes()?)?;
                    trace!("{:?} [{}]", copy_data, self.address);

                    return Ok(Some(ReplicationData::CopyData(copy_data)));
                }
                'C' => (),
                'c' => return Ok(Some(ReplicationData::CopyDone)), // CopyDone.
                'Z' => {
                    debug!("slot \"{}\" drained [{}]", self.name, self.address);
                    return Ok(None);
                }
                'E' => return Err(ErrorResponse::from_bytes(message.to_bytes()?)?.into()),
                c => return Err(Error::OutOfSync(c)),
            }
        }
    }

    /// Update origin on last flushed LSN.
    pub async fn status_update(&mut self, status_update: StatusUpdate) -> Result<(), Error> {
        debug!(
            "confirmed {} flushed [{}]",
            status_update.last_flushed,
            self.server()?.addr()
        );

        if let Some(tracker) = self.tracker.as_ref() {
            tracker.update_lsn(&Lsn::from_i64(status_update.last_flushed))
        }

        self.server()?
            .send_one(&status_update.wrapped()?.into())
            .await?;
        self.server()?.flush().await?;

        Ok(())
    }

    /// Ask remote to close stream.
    pub async fn stop_replication(&mut self) -> Result<(), Error> {
        self.server()?.send_one(&CopyDone.into()).await?;
        self.server()?.flush().await?;

        Ok(())
    }

    /// Current slot LSN.
    pub fn lsn(&self) -> Lsn {
        self.lsn
    }

    /// Slot name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone)]
pub enum ReplicationData {
    CopyData(CopyData),
    CopyDone,
}

impl ReplicationData {
    pub fn xlog_data(&self) -> Option<XLogData> {
        if let Self::CopyData(copy_data) = self {
            copy_data.xlog_data()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use tokio::spawn;

    use crate::{backend::server::test::test_server, net::replication::xlog_data::XLogPayload};

    use super::*;

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
        let _ = server
            .execute(format!(
                "SELECT pg_drop_replication_slot(test_slot_replication)"
            ))
            .await;
        server
            .execute(
                "CREATE PUBLICATION test_slot_replication FOR TABLE public.test_slot_replication",
            )
            .await
            .unwrap();

        let addr = server.addr();

        let mut slot = ReplicationSlot::replication(
            "test_slot_replication",
            addr,
            Some("test_slot_replication".into()),
            0,
        );
        let _ = slot.create_slot().await.unwrap();
        slot.connect().await.unwrap();

        let (tx, mut rx) = channel(16);

        let handle = spawn(async move {
            slot.start_replication().await?;
            server
                .execute("INSERT INTO test_slot_replication (id) VALUES (1)")
                .await?;

            loop {
                let message = slot.replicate(Duration::MAX).await?;
                tx.send(message.clone()).await.unwrap();

                if let Some(message) = message {
                    match message.clone() {
                        ReplicationData::CopyData(copy_data) => match copy_data.xlog_data() {
                            Some(xlog_data) => {
                                if let Some(XLogPayload::Commit(_)) = xlog_data.payload() {
                                    slot.stop_replication().await?;
                                }
                            }
                            _ => (),
                        },
                        ReplicationData::CopyDone => (),
                    }
                } else {
                    break;
                }
            }

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
                        assert_eq!(insert.column(0).unwrap().as_str().unwrap(), "1")
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
}
