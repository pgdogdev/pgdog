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

#[derive(Debug, Clone)]
pub struct ReplicationSlot {
    pub publication: String,
    pub name: String,
    pub snapshot: Snapshot,
    pub lsn: Lsn,
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
    pub async fn create_slot(&mut self, conn: &mut Server) -> Result<Lsn, Error> {
        let result = conn
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
    pub async fn drop_slot(&self, conn: &mut Server) -> Result<(), Error> {
        conn.execute(&format!(r#"DROP_REPLICATION_SLOT "{}""#, self.name))
            .await?;

        Ok(())
    }

    /// Start replication.
    pub async fn replicate(&self, conn: &mut Server) -> Result<(), Error> {
        let query = Query::new(&format!(
            r#"START_REPLICATION SLOT "{}" LOGICAL {} ("p roto_version" '2', "publication_names" '{}')"#,
            self.name, self.lsn, self.publication
        ));
        conn.send_one(&query.into()).await?;
        conn.flush().await?;

        let copy_both = conn.read().await?;

        if copy_both.code() != 'W' {
            return Err(Error::Protocol);
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{
        backend::{pool::Address, ServerOptions},
        net::Parameter,
    };

    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_replication_slot() {
        crate::logger();

        let mut server = Server::connect(
            &Address {
                host: "127.0.0.1".into(),
                port: 5432,
                user: "pgdog".into(),
                password: "pgdog".into(),
                database_name: "pgdog".into(),
            },
            ServerOptions {
                params: vec![Parameter {
                    name: "replication".into(),
                    value: "database".into(),
                }],
            },
        )
        .await
        .unwrap();

        // let _ = server
        //     .execute("DROP PUBLICATION test_replication_slot")
        //     .await;
        server
            .execute("CREATE PUBLICATION test_replication_slot FOR ALL TABLES")
            .await
            .unwrap();

        let mut repl =
            ReplicationSlot::replication("test_replication_slot", "test_replication_slot");
        let _ = repl.drop_slot(&mut server).await;
        repl.create_slot(&mut server).await.unwrap();

        let mut repl_temp = ReplicationSlot::data_sync("test_replication_slot");
        server
            .execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .unwrap();
        repl_temp.create_slot(&mut server).await.unwrap();
        server.execute("COMMIT").await.unwrap();
        repl_temp.replicate(&mut server).await.unwrap();
        // repl_temp.drop_slot(&mut server).await.unwrap();
        repl.drop_slot(&mut server).await.unwrap();
        let _ = server
            .execute("DROP PUBLICATION test_replication_slot")
            .await;
    }
}
