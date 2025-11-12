use super::super::Error;
use crate::{
    backend::{pool::Address, Server, ServerOptions},
    net::{
        replication::StatusUpdate, CopyData, CopyDone, DataRow, ErrorResponse, Format, FromBytes,
        Protocol, Query, ToBytes,
    },
    util::random_string,
};
use std::{fmt::Display, str::FromStr, time::Duration};
use tokio::time::timeout;
use tracing::{debug, trace};

#[derive(Debug, Clone, Default, Copy)]
pub struct Lsn {
    pub high: i64,
    pub low: i64,
    pub lsn: i64,
}

impl Lsn {
    /// Get LSN from the 64-bit representation.
    pub fn from_i64(lsn: i64) -> Self {
        let high = ((lsn >> 32) as u32) as i64;
        let low = ((lsn & 0xFFFF_FFFF) as u32) as i64;
        Self { high, low, lsn }
    }
}

impl FromStr for Lsn {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // This is not the right formula to get the LSN number but
        // it survives (de)serialization which is all we care about.
        //
        // TODO: maybe just save it as a string?
        let mut parts = s.split("/");
        let high = parts.next().ok_or(Error::LsnDecode)?;
        let high = i64::from_str_radix(high, 16)?;

        let low = parts.next().ok_or(Error::LsnDecode)?;
        let low = i64::from_str_radix(low, 16)?;

        let lsn = (high << 32) + low;

        Ok(Self { lsn, high, low })
    }
}

impl Display for Lsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:X}/{:X}", self.high, self.low)
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
    pub fn replication(publication: &str, address: &Address, name: Option<String>) -> Self {
        let name = name.unwrap_or(format!("__pgdog_repl_{}", random_string(19).to_lowercase()));

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
            dropped: true, // Temporary.
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
        if self.server.is_none() {
            self.connect().await?;
        }

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
}

#[derive(Debug)]
pub enum ReplicationData {
    CopyData(CopyData),
    CopyDone,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_lsn() {
        let original = "1/12A4C"; // It's fine.
        let lsn = Lsn::from_str(original).unwrap();
        assert_eq!(lsn.high, 1);
        let lsn = lsn.to_string();
        assert_eq!(lsn, original);
    }
}
