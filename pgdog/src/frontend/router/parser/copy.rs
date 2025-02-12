//! Parse COPY statement.

use pg_query::{protobuf::CopyStmt, NodeEnum};

use crate::{
    backend::Cluster,
    frontend::router::{sharding::shard_str, CopyRow},
    net::messages::CopyData,
};

use super::{CsvStream, Error};

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
            headers: false,
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
    delimiter: Option<char>,
    /// Number of shards.
    pub shards: usize,
    /// Which column is used for sharding.
    pub sharded_column: Option<usize>,
    /// Number of columns
    pub columns: usize,
    /// This is a COPY coming from the server.
    pub is_from: bool,
    /// CSV parser that can handle incomplete records.
    csv_stream: CsvStream,
}

impl Default for CopyParser {
    fn default() -> Self {
        Self {
            headers: false,
            delimiter: None,
            sharded_column: None,
            shards: 1,
            columns: 0,
            is_from: false,
            csv_stream: CsvStream::new(',', false),
        }
    }
}

impl CopyParser {
    /// Create new copy parser from a COPY statement.
    pub fn new(stmt: &CopyStmt, cluster: &Cluster) -> Result<Option<Self>, Error> {
        let mut parser = Self {
            shards: cluster.shards().len(),
            is_from: stmt.is_from,
            ..Default::default()
        };

        if let Some(ref rel) = stmt.relation {
            let mut columns = vec![];

            for column in &stmt.attlist {
                if let Some(NodeEnum::String(ref column)) = column.node {
                    columns.push(column.sval.as_str());
                }
            }

            parser.sharded_column = cluster.sharded_column(&rel.relname, &columns);
            parser.columns = columns.len();

            for option in &stmt.options {
                if let Some(NodeEnum::DefElem(ref elem)) = option.node {
                    match elem.defname.to_lowercase().as_str() {
                        "format" => {
                            if let Some(ref arg) = elem.arg {
                                if let Some(NodeEnum::String(ref string)) = arg.node {
                                    if string.sval.to_lowercase().as_str() == "csv"
                                        && parser.delimiter.is_none()
                                    {
                                        parser.delimiter = Some(',');
                                    }
                                }
                            }
                        }

                        "delimiter" => {
                            if let Some(ref arg) = elem.arg {
                                if let Some(NodeEnum::String(ref string)) = arg.node {
                                    parser.delimiter =
                                        Some(string.sval.chars().next().unwrap_or(','));
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

        parser.csv_stream = CsvStream::new(parser.delimiter(), parser.headers);

        Ok(Some(parser))
    }

    #[inline]
    fn delimiter(&self) -> char {
        self.delimiter.unwrap_or('\t')
    }

    /// Split CopyData (F) messages into multiple CopyData (F) messages
    /// with shard numbers.
    pub fn shard(&mut self, data: Vec<CopyData>) -> Result<Vec<CopyRow>, Error> {
        let mut rows = vec![];

        for row in data {
            self.csv_stream.write(row.data());

            if self.headers && self.is_from {
                let headers = self.csv_stream.headers()?;
                if let Some(headers) = headers {
                    rows.push(CopyRow::new(headers.to_string().as_bytes(), None));
                }
                self.headers = false;
            }

            for record in self.csv_stream.records() {
                // Totally broken.
                let record = record?;

                let shard = if let Some(sharding_column) = self.sharded_column {
                    let key = record.get(sharding_column).ok_or(Error::NoShardingColumn)?;

                    shard_str(key, self.shards)
                } else {
                    None
                };

                rows.push(CopyRow::new(record.to_string().as_bytes(), shard));
            }
        }

        Ok(rows)
    }
}

#[cfg(test)]
mod test {
    use pg_query::parse;

    use super::*;

    #[test]
    fn test_copy_text() {
        let copy = "COPY sharded (id, value) FROM STDIN";
        let stmt = parse(copy).unwrap();
        let stmt = stmt.protobuf.stmts.first().unwrap();
        let copy = match stmt.stmt.clone().unwrap().node.unwrap() {
            NodeEnum::CopyStmt(copy) => copy,
            _ => panic!("not a copy"),
        };

        let mut copy = CopyParser::new(&copy, &Cluster::default())
            .unwrap()
            .unwrap();

        assert_eq!(copy.delimiter(), '\t');
        assert!(!copy.headers);

        let one = CopyData::new("5\thello world\n".as_bytes());
        let two = CopyData::new("10\thowdy mate\n".as_bytes());
        let sharded = copy.shard(vec![one, two]).unwrap();
        assert_eq!(sharded[0].message().data(), b"5\thello world\n");
        assert_eq!(sharded[1].message().data(), b"10\thowdy mate\n");
    }

    #[test]
    fn test_copy_csv() {
        let copy = "COPY sharded (id, value) FROM STDIN CSV HEADER";
        let stmt = parse(copy).unwrap();
        let stmt = stmt.protobuf.stmts.first().unwrap();
        let copy = match stmt.stmt.clone().unwrap().node.unwrap() {
            NodeEnum::CopyStmt(copy) => copy,
            _ => panic!("not a copy"),
        };

        let mut copy = CopyParser::new(&copy, &Cluster::default())
            .unwrap()
            .unwrap();
        assert!(copy.is_from);

        assert_eq!(copy.delimiter(), ',');
        assert!(copy.headers);

        let header = CopyData::new("id,value\n".as_bytes());
        let one = CopyData::new("5,hello world\n".as_bytes());
        let two = CopyData::new("10,howdy mate\n".as_bytes());
        let sharded = copy.shard(vec![header, one, two]).unwrap();

        assert_eq!(sharded[0].message().data(), b"id,value\n");
        assert_eq!(sharded[1].message().data(), b"5,hello world\n");
        assert_eq!(sharded[2].message().data(), b"10,howdy mate\n");
    }
}
