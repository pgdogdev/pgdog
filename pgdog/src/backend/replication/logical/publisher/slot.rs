use crate::{
    backend::{replication::Error, Server},
    net::{DataRow, Format, Protocol, Query},
    util::random_string,
};
use std::{fmt::Display, str::FromStr};

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
    server: Server,
}

impl ReplicationSlot {
    /// Create replication slot used for streaming the WAL.
    pub fn replication(name: &str, publication: &str, server: Server) -> Self {
        Self {
            name: name.to_string(),
            snapshot: Snapshot::Nothing,
            lsn: Lsn::default(),
            publication: publication.to_string(),
            server,
        }
    }

    /// Create replication slot for data sync.
    pub fn data_sync(publication: &str, server: Server) -> Self {
        let name = format!("__pgdog_{}", random_string(24).to_lowercase());

        Self {
            name,
            snapshot: Snapshot::Use,
            lsn: Lsn::default(),
            publication: publication.to_string(),
            server,
        }
    }

    /// Create the slot.
    pub async fn create_slot(&mut self) -> Result<Lsn, Error> {
        let result = self
            .server
            .fetch_all::<DataRow>(&format!(
                r#"CREATE_REPLICATION_SLOT "{}" LOGICAL "pgoutput" (SNAPSHOT '{}')"#,
                self.name, self.snapshot
            ))
            .await?
            .pop()
            .ok_or(Error::Protocol)?;

        let lsn = result
            .get::<String>(1, Format::Text)
            .ok_or(Error::Protocol)?;

        let lsn = Lsn::from_str(&lsn)?;
        self.lsn = lsn;
        Ok(lsn)
    }

    /// Drop the slot.
    pub async fn drop_slot(&mut self) -> Result<(), Error> {
        self.server
            .execute(&format!(r#"DROP_REPLICATION_SLOT "{}""#, self.name))
            .await?;

        Ok(())
    }

    /// Start replication.
    pub async fn replicate(&mut self) -> Result<(), Error> {
        let query = Query::new(&format!(
            r#"START_REPLICATION SLOT "{}" LOGICAL {} ("proto_version" '2', "publication_names" '{}')"#,
            self.name, self.lsn, self.publication
        ));
        self.server.send_one(&query.into()).await?;
        self.server.flush().await?;

        let copy_both = self.server.read().await?;

        if copy_both.code() != 'W' {
            return Err(Error::Protocol);
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::backend::{pool::Address, ServerOptions};

    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_replication_slot() {
        crate::logger();

        let mut server = Server::connect(&Address::new_test(), ServerOptions::new_replication())
            .await
            .unwrap();

        server
            .execute("DROP TABLE IF EXISTS test_replication_slot")
            .await
            .unwrap();
        server
            .execute("CREATE TABLE IF NOT EXISTS test_replication_slot (id bigint primary key)")
            .await
            .unwrap();
        server
            .execute("INSERT INTO test_replication_slot VALUES (1), (2), (3)")
            .await
            .unwrap();

        let _ = server
            .execute("DROP PUBLICATION test_replication_slot")
            .await;
        server
            .execute("CREATE PUBLICATION test_replication_slot FOR TABLE test_replication_slot")
            .await
            .unwrap();

        let mut repl =
            ReplicationSlot::replication("test_replication_slot", "test_replication_slot", server);
        let _ = repl.drop_slot().await;
        repl.create_slot().await.unwrap();
        repl.replicate().await.unwrap();

        let mut server2 = Server::connect(&Address::new_test(), ServerOptions::new_replication())
            .await
            .unwrap();
        server2
            .execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .unwrap();

        let mut repl_temp = ReplicationSlot::data_sync("test_replication_slot", server2);

        repl_temp.create_slot().await.unwrap();
        repl_temp.replicate().await.unwrap();

        repl_temp.drop_slot().await.unwrap();
        repl.drop_slot().await.unwrap();
    }
}
