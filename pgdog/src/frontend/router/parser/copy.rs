//! Parse COPY statement.

use csv::ReaderBuilder;
use pg_query::{protobuf::CopyStmt, NodeEnum};

use crate::{
    backend::Cluster,
    frontend::router::{sharding::shard_str, CopyRow},
    net::messages::CopyData,
};

use super::Error;

/// Copy information parsed from a COPY statement.
#[derive(Debug, Clone)]
pub struct CopyInfo {
    /// CSV contains headers.
    pub headers: bool,
    /// CSV delimiter.
    pub delimiter: char,
    /// Columns declared by the caller.
    pub columns: Vec<String>,
    /// Table name target for the COPY.
    pub table_name: String,
}

impl Default for CopyInfo {
    fn default() -> Self {
        Self {
            headers: true,
            delimiter: ',',
            columns: vec![],
            table_name: "".into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CopyParser {
    /// CSV contains headers.
    pub headers: bool,
    /// CSV delimiter.
    pub delimiter: char,
    /// Number of shards.
    pub shards: usize,
    /// Which column is used for sharding.
    pub sharded_column: Option<usize>,
}

impl Default for CopyParser {
    fn default() -> Self {
        Self {
            headers: true,
            delimiter: ',',
            sharded_column: None,
            shards: 1,
        }
    }
}

impl CopyParser {
    /// Create new copy parser from a COPY statement.
    pub fn new(stmt: &CopyStmt, cluster: &Cluster) -> Result<Option<Self>, Error> {
        if !stmt.is_from {
            return Ok(None);
        }

        let mut parser = Self::default();
        parser.shards = cluster.shards().len();

        if let Some(ref rel) = stmt.relation {
            // parser.table_name = rel.relname.clone();
            let mut columns = vec![];

            for column in &stmt.attlist {
                if let Some(NodeEnum::String(ref column)) = column.node {
                    columns.push(column.sval.as_str());
                }
            }

            parser.sharded_column = cluster.sharded_column(&rel.relname, &columns);

            for option in &stmt.options {
                if let Some(NodeEnum::DefElem(ref elem)) = option.node {
                    match elem.defname.to_lowercase().as_str() {
                        "format" => {
                            if let Some(ref arg) = elem.arg {
                                if let Some(NodeEnum::String(ref string)) = arg.node {
                                    if string.sval.to_lowercase().as_str() != "csv" {
                                        return Ok(None);
                                    }
                                }
                            }
                        }

                        "delimiter" => {
                            if let Some(ref arg) = elem.arg {
                                if let Some(NodeEnum::String(ref string)) = arg.node {
                                    parser.delimiter = string.sval.chars().next().unwrap_or(',');
                                }
                            }
                        }

                        "header" => {
                            parser.headers = true;
                        }

                        _ => (),
                    }
                }
            }
        }

        Ok(Some(parser))
    }

    /// Split CopyData (F) messages into multiple CopyData (F) messages
    /// with shard numbers.
    pub fn shard(&mut self, data: Vec<CopyData>) -> Result<Vec<CopyRow>, Error> {
        let mut rows = vec![];

        for row in data {
            let mut csv = ReaderBuilder::new()
                .has_headers(self.headers)
                .delimiter(self.delimiter as u8)
                .from_reader(row.data());

            if self.headers {
                let headers = csv
                    .headers()?
                    .into_iter()
                    .collect::<Vec<_>>()
                    .join(self.delimiter.to_string().as_str())
                    + "\n";
                rows.push(CopyRow::new(headers.as_bytes(), None));
            }

            for record in csv.records() {
                let record = record?;
                if let Some(sharding_column) = self.sharded_column {
                    let key = record
                        .iter()
                        .nth(sharding_column)
                        .ok_or(Error::NoShardingColumn)?;

                    let shard = shard_str(key, self.shards);

                    if let Some(shard) = shard {
                        if let Some(pos) = record.position() {
                            let start = pos.byte() as usize;
                            let end = start + record.as_slice().len() + 1; // New line.
                            let data = row.data().get(start..=end);

                            if let Some(data) = data {
                                rows.push(CopyRow::new(data, Some(shard)));
                            }
                        }
                    }
                }
            }

            self.headers = false;
        }

        Ok(rows)
    }
}
