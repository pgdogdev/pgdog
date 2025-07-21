use super::super::Error;
use crate::{
    backend::{pool::Address, Server, ServerOptions},
    net::{CopyData, DataRow, ErrorResponse, Format, FromBytes, Protocol, Query, ToBytes},
    util::random_string,
};
use std::{fmt::Display, str::FromStr};
use tokio::spawn;
use tracing::{debug, error, trace};

#[derive(Debug, Clone, Default, Copy)]
pub struct Lsn {
    pub timeline: i64,
    pub offset: i64,
}

impl FromStr for Lsn {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split("/");
        let timeline = parts.next().ok_or(Error::LsnDecode)?.parse()?;
        let offset = parts.next().ok_or(Error::LsnDecode)?;
        let offset = i64::from_str_radix(offset, 16)?;

        Ok(Self { timeline, offset })
    }
}

impl Display for Lsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{:X}", self.timeline, self.offset)
    }
}

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
}

impl ReplicationSlot {
    /// Create replication slot used for streaming the WAL.
    pub fn replication(publication: &str, address: &Address) -> Self {
        let name = format!("__pgdog_repl_{}", random_string(19).to_lowercase());

        Self {
            address: address.clone(),
            name: name.to_string(),
            snapshot: Snapshot::Nothing,
            lsn: Lsn::default(),
            publication: publication.to_string(),
            dropped: false,
            server: None,
            kind: SlotKind::Replication,
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
            dropped: false,
            server: None,
            kind: SlotKind::DataSync,
        }
    }

    /// Connect to database using replication mode.
    pub async fn connect(&mut self) -> Result<(), Error> {
        self.server = Some(Server::connect(&self.address, ServerOptions::new_replication()).await?);

        Ok(())
    }

    pub fn server(&mut self) -> Result<&mut Server, Error> {
        self.server.as_mut().ok_or(Error::NotConnected)
    }

    /// Create the slot.
    pub async fn create_slot(&mut self) -> Result<Lsn, Error> {
        if self.kind == SlotKind::DataSync {
            self.server()?
                .execute("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ")
                .await?;
        }

        let start_replication = format!(
            r#"CREATE_REPLICATION_SLOT "{}" LOGICAL "pgoutput" (SNAPSHOT '{}')"#,
            self.name, self.snapshot
        );

        let result = self
            .server()?
            .fetch_all::<DataRow>(&start_replication)
            .await?
            .pop()
            .ok_or(Error::MissingData)?;

        let lsn = result
            .get::<String>(1, Format::Text)
            .ok_or(Error::MissingData)?;

        let lsn = Lsn::from_str(&lsn)?;
        self.lsn = lsn;

        debug!(
            "replication slot \"{}\" at lsn {} created [{}]",
            self.name, self.lsn, self.address,
        );

        Ok(lsn)
    }

    /// Drop the slot.
    pub async fn drop_slot(&mut self) -> Result<(), Error> {
        let drop_slot = self.drop_slot_query(true);
        self.server()?.execute(&drop_slot).await?;

        debug!(
            "replication slot \"{}\" dropped [{}]",
            self.name, self.address
        );
        self.dropped = true;

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
        let query = Query::new(&format!(
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
    pub async fn replicate(&mut self) -> Result<Option<ReplicationData>, Error> {
        loop {
            let message = self.server()?.read().await?;
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
}

impl Drop for ReplicationSlot {
    fn drop(&mut self) {
        let server = self.server.take();

        if let Some(mut server) = server {
            if !self.dropped && self.kind == SlotKind::DataSync {
                let name = self.name.clone();
                let address = self.address.clone();
                let drop_query = self.drop_slot_query(false);
                spawn(async move {
                    if let Err(err) = server.execute(&drop_query).await {
                        error!("failed to drop slot \"{}\": {} [{}]", name, err, address);
                    }
                });
            }
        }
    }
}

#[derive(Debug)]
pub enum ReplicationData {
    CopyData(CopyData),
    CopyDone,
}
