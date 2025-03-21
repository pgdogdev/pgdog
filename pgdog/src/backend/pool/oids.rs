//! OIDs used by Postgres for user-created data types.

use crate::backend::Error;
use crate::net::messages::{DataRow, Format};

use super::Guard;

#[derive(Debug, Clone, Default, Copy)]
pub struct Oids {
    vector: Option<i32>,
}

struct PgType {
    oid: i32,
    typname: String,
}

impl From<DataRow> for PgType {
    fn from(value: DataRow) -> Self {
        let oid = value.get::<i32>(0, Format::Text).unwrap_or_default();
        let typname = value.get::<String>(0, Format::Text).unwrap_or_default();

        Self { oid, typname }
    }
}

impl Oids {
    pub(super) async fn load(server: &mut Guard) -> Result<Self, Error> {
        let types: Vec<PgType> = server
            .fetch_all("SELECT oid, typname FROM pg_type WHERE typname IN ('vector')")
            .await?;

        let mut oids = Oids::default();

        for ty in types {
            if ty.typname == "vector" {
                oids.vector = Some(ty.oid);
            }
        }

        Ok(oids)
    }

    /// Get pgvector oid, if installed.
    pub fn vector(&self) -> Option<i32> {
        self.vector
    }
}
