//! Table.
use pg_query::NodeEnum;

use crate::backend::replication::Error;
use crate::backend::{Cluster, Server};
use crate::frontend::router::parser::CopyParser;
use crate::net::Query;

#[derive(Debug, Clone)]
pub struct Table {
    schema: String,
    name: String,
    columns: Vec<String>,
}

impl Table {
    pub fn new(schema: &str, name: &str, columns: &[String]) -> Self {
        Self {
            schema: schema.to_owned(),
            name: name.to_owned(),
            columns: columns.to_vec(),
        }
    }

    pub async fn copy_out(&self, source: &mut Server, destination: &Cluster) -> Result<(), Error> {
        if !source.in_transaction() {
            return Err(Error::CopyNoTransaction);
        }

        let copy_query = format!(
            r#"COPY "{}"."{}" ({}) TO STDOUT"#,
            self.schema,
            self.name,
            self.columns
                .iter()
                .map(|c| format!(r#""{}""#, c))
                .collect::<Vec<_>>()
                .join(", ")
        );

        let stmt = pg_query::parse(&copy_query)?;
        let copy = stmt
            .protobuf
            .stmts
            .first()
            .ok_or(Error::Protocol)?
            .stmt
            .as_ref()
            .ok_or(Error::Protocol)?;
        match copy.node {
            Some(NodeEnum::CopyStmt(ref stmt)) => {
                let copy = CopyParser::new(stmt, destination).unwrap();
            }

            _ => (),
        };

        source.send_one(Query::new(copy_query)).await?;
        source.flush().await?;

        Ok(())
    }
}
