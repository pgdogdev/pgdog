use super::super::Error;
use crate::{
    backend::Server,
    net::{CopyData, DataRow, Format, FromBytes, Protocol, Query, ToBytes},
    util::random_string,
};
use std::{fmt::Display, str::FromStr};
use tracing::{debug, trace};

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
    publication: String,
    name: String,
    snapshot: Snapshot,
    lsn: Lsn,
}

impl ReplicationSlot {
    /// Create replication slot used for streaming the WAL.
    pub fn replication(name: &str, publication: &str) -> Self {
        Self {
            name: name.to_string(),
            snapshot: Snapshot::Nothing,
            lsn: Lsn::default(),
            publication: publication.to_string(),
        }
    }

    /// Create replication slot for data sync.
    pub fn data_sync(publication: &str) -> Self {
        let name = format!("__pgdog_{}", random_string(24).to_lowercase());

        Self {
            name,
            snapshot: Snapshot::Use,
            lsn: Lsn::default(),
            publication: publication.to_string(),
        }
    }

    /// Create the slot.
    pub async fn create_slot(&mut self, server: &mut Server) -> Result<Lsn, Error> {
        let result = server
            .fetch_all::<DataRow>(&format!(
                r#"CREATE_REPLICATION_SLOT "{}" LOGICAL "pgoutput" (SNAPSHOT '{}')"#,
                self.name, self.snapshot
            ))
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
            self.name,
            self.lsn,
            server.addr(),
        );

        Ok(lsn)
    }

    /// Drop the slot.
    pub async fn drop_slot(&mut self, server: &mut Server) -> Result<(), Error> {
        server
            .execute(&format!(r#"DROP_REPLICATION_SLOT "{}" WAIT"#, self.name))
            .await?;

        debug!(
            "replication slot \"{}\" dropped [{}]",
            self.name,
            server.addr()
        );

        Ok(())
    }

    /// Start replication.
    pub async fn start_replication(&mut self, server: &mut Server) -> Result<(), Error> {
        // TODO: This is definitely Postgres version-specific.
        let query = Query::new(&format!(
            r#"START_REPLICATION SLOT "{}" LOGICAL {} ("proto_version" '4', origin 'any', "publication_names" '"{}"')"#,
            self.name, self.lsn, self.publication
        ));
        server.send(&vec![query.into()].into()).await?;

        let copy_both = server.read().await?;

        if copy_both.code() != 'W' {
            return Err(Error::OutOfSync(copy_both.code()));
        }

        debug!(
            "replication from slot \"{}\" started [{}]",
            self.name,
            server.addr()
        );

        Ok(())
    }

    /// Replicate from slot until finished.
    pub async fn replicate(
        &mut self,
        server: &mut Server,
    ) -> Result<Option<ReplicationData>, Error> {
        loop {
            let message = server.read().await?;
            match message.code() {
                'd' => {
                    let copy_data = CopyData::from_bytes(message.to_bytes()?)?;
                    trace!("{:?} [{}]", copy_data, server.addr());

                    return Ok(Some(ReplicationData::CopyData(copy_data)));
                }
                'C' => (),
                'c' => return Ok(Some(ReplicationData::CopyDone)), // CopyDone.
                'Z' => {
                    debug!("slot \"{}\" drained [{}]", self.name, server.addr());
                    return Ok(None);
                }
                c => return Err(Error::OutOfSync(c)),
            }
        }
    }
}

#[derive(Debug)]
pub enum ReplicationData {
    CopyData(CopyData),
    CopyDone,
}

#[cfg(test)]
mod test {
    // use crate::backend::{pool::Address, ServerOptions};

    // use super::*;

    // #[tokio::test]
    // #[ignore]
    // async fn test_replication_slot() {
    //     crate::logger();

    //     let mut server = Server::connect(&Address::new_test(), ServerOptions::new_replication())
    //         .await
    //         .unwrap();

    //     server
    //         .execute("DROP TABLE IF EXISTS test_replication_slot")
    //         .await
    //         .unwrap();
    //     server
    //         .execute("CREATE TABLE IF NOT EXISTS test_replication_slot (id bigint primary key)")
    //         .await
    //         .unwrap();
    //     server
    //         .execute("INSERT INTO test_replication_slot VALUES (1), (2), (3)")
    //         .await
    //         .unwrap();

    //     let _ = server
    //         .execute("DROP PUBLICATION test_replication_slot")
    //         .await;
    //     server
    //         .execute("CREATE PUBLICATION test_replication_slot FOR TABLE test_replication_slot")
    //         .await
    //         .unwrap();

    //     let mut repl =
    //         ReplicationSlot::replication("test_replication_slot", "test_replication_slot", server);
    //     let _ = repl.drop_slot().await;
    //     repl.create_slot().await.unwrap();
    //     repl.replicate().await.unwrap();

    //     let mut server2 = Server::connect(&Address::new_test(), ServerOptions::new_replication())
    //         .await
    //         .unwrap();
    //     server2
    //         .execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
    //         .await
    //         .unwrap();

    //     let mut repl_temp = ReplicationSlot::data_sync("test_replication_slot", server2);

    //     repl_temp.create_slot().await.unwrap();
    //     repl_temp.replicate().await.unwrap();

    //     repl_temp.drop_slot().await.unwrap();
    //     repl.drop_slot().await.unwrap();
    // }
}
