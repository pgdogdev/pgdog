//! Parse COPY statement.

use pg_query::{protobuf::CopyStmt, NodeEnum};

use crate::{
    backend::{replication::ShardedColumn, Cluster, ShardingSchema},
    frontend::router::{
        sharding::{shard_binary, shard_str},
        CopyRow,
    },
    net::messages::{CopyData, ToBytes},
};

use super::{binary::Data, BinaryStream, CsvStream, Error};

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
enum CopyStream {
    Text(CsvStream),
    Binary(BinaryStream),
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
    pub sharded_column: Option<ShardedColumn>,
    /// Number of columns
    pub columns: usize,
    /// This is a COPY coming from the server.
    pub is_from: bool,
    /// CSV parser that can handle incomplete records.
    stream: CopyStream,

    sharding_schema: ShardingSchema,
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
            stream: CopyStream::Text(CsvStream::new(',', false)),
            sharding_schema: ShardingSchema::default(),
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

        let mut binary = false;

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
                                    match string.sval.to_lowercase().as_str() {
                                        "binary" => {
                                            binary = true;
                                            parser.headers = true;
                                        }
                                        "csv" => {
                                            if parser.delimiter.is_none() {
                                                parser.delimiter = Some(',');
                                            }
                                        }
                                        _ => (),
                                    }
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

        parser.stream = if binary {
            CopyStream::Binary(BinaryStream::default())
        } else {
            CopyStream::Text(CsvStream::new(parser.delimiter(), parser.headers))
        };
        parser.sharding_schema = cluster.sharding_schema();

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
            match &mut self.stream {
                CopyStream::Binary(stream) => stream.write(row.data()),
                CopyStream::Text(stream) => stream.write(row.data()),
            }

            match &mut self.stream {
                CopyStream::Text(stream) => {
                    if self.headers && self.is_from {
                        let headers = stream.headers()?;
                        if let Some(headers) = headers {
                            rows.push(CopyRow::new(headers.to_string().as_bytes(), None));
                        }
                        self.headers = false;
                    }

                    for record in stream.records() {
                        // Totally broken.
                        let record = record?;

                        let shard = if let Some(sharding_column) = self.sharded_column {
                            let key = record
                                .get(sharding_column.position)
                                .ok_or(Error::NoShardingColumn)?;

                            shard_str(key, &self.sharding_schema)
                        } else {
                            None
                        };

                        rows.push(CopyRow::new(record.to_string().as_bytes(), shard));
                    }
                }

                CopyStream::Binary(stream) => {
                    if self.headers {
                        let header = stream.header()?;
                        rows.push(CopyRow::new(&header.to_bytes()?, None));
                        self.headers = false;
                    }

                    for tuple in stream.tuples() {
                        let tuple = tuple?;
                        if tuple.end() {
                            let terminator = (-1_i16).to_be_bytes();
                            rows.push(CopyRow::new(&terminator, None));
                            break;
                        }
                        let shard = if let Some(column) = self.sharded_column {
                            let key = tuple.get(column.position).ok_or(Error::NoShardingColumn)?;
                            if let Data::Column(key) = key {
                                shard_binary(key, &column.data_type, self.sharding_schema.shards)
                            } else {
                                None
                            }
                        } else {
                            None
                        };

                        rows.push(CopyRow::new(&tuple.to_bytes()?, shard));
                    }
                }
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

        let partial_one = CopyData::new("11,howdy partner".as_bytes());
        let partial_two = CopyData::new("\n1,2".as_bytes());
        let partial_three = CopyData::new("\n".as_bytes());

        let sharded = copy.shard(vec![partial_one]).unwrap();
        assert!(sharded.is_empty());
        let sharded = copy.shard(vec![partial_two]).unwrap();
        assert_eq!(sharded[0].message().data(), b"11,howdy partner\n");
        let sharded = copy.shard(vec![partial_three]).unwrap();
        assert_eq!(sharded[0].message().data(), b"1,2\n");
    }
}
