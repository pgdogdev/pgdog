//! Parse COPY statement.

use pg_query::{protobuf::CopyStmt, NodeEnum};

use crate::{
    backend::{Cluster, ShardingSchema},
    config::ShardedTable,
    frontend::router::{
        parser::Shard,
        sharding::{ContextBuilder, Tables},
        CopyRow,
    },
    net::messages::{CopyData, ToBytes},
};

use super::{binary::Data, BinaryStream, Column, CsvStream, Error, Table};

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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CopyFormat {
    Text,
    Csv,
    Binary,
}

#[derive(Debug, Clone)]
enum CopyStream {
    Text(Box<CsvStream>),
    Binary(BinaryStream),
}

#[derive(Debug, Clone)]
pub struct CopyParser {
    /// CSV contains headers.
    headers: bool,
    /// CSV delimiter.
    delimiter: Option<char>,
    /// Number of columns
    columns: usize,
    /// This is a COPY coming from the client.
    is_from: bool,
    /// Stream parser.
    stream: CopyStream,
    /// The sharding schema
    sharding_schema: ShardingSchema,
    /// This COPY is dealing with a sharded table.
    sharded_table: Option<ShardedTable>,
    /// The sharding column is in this position in each row.
    sharded_column: usize,
    /// Schema shard.
    schema_shard: Option<Shard>,
}

impl Default for CopyParser {
    fn default() -> Self {
        Self {
            headers: false,
            delimiter: None,
            columns: 0,
            is_from: false,
            stream: CopyStream::Text(Box::new(CsvStream::new(',', false, CopyFormat::Csv, "\\N"))),
            sharding_schema: ShardingSchema::default(),
            sharded_table: None,
            sharded_column: 0,
            schema_shard: None,
        }
    }
}

impl CopyParser {
    /// Create new copy parser from a COPY statement.
    pub fn new(stmt: &CopyStmt, cluster: &Cluster) -> Result<Self, Error> {
        let mut parser = Self {
            is_from: stmt.is_from,
            ..Default::default()
        };

        let mut format = CopyFormat::Text;
        let mut null_string = "\\N".to_owned();

        if let Some(ref rel) = stmt.relation {
            let mut columns = vec![];

            for column in &stmt.attlist {
                if let Ok(column) = Column::from_string(column) {
                    columns.push(column);
                }
            }

            let table = Table::from(rel);

            // The CopyParser is used for replicating
            // data during data-sync. This will ensure all rows
            // are sent to the right schema-based shard.
            if let Some(schema) = cluster.sharding_schema().schemas.get(table.schema()) {
                parser.schema_shard = Some(schema.shard().into());
            }

            if let Some(key) = Tables::new(&cluster.sharding_schema()).key(table, &columns) {
                parser.sharded_table = Some(key.table.clone());
                parser.sharded_column = key.position;
            }

            parser.columns = columns.len();

            for option in &stmt.options {
                if let Some(NodeEnum::DefElem(ref elem)) = option.node {
                    match elem.defname.to_lowercase().as_str() {
                        "format" => {
                            if let Some(ref arg) = elem.arg {
                                if let Some(NodeEnum::String(ref string)) = arg.node {
                                    match string.sval.to_lowercase().as_str() {
                                        "binary" => {
                                            parser.headers = true;
                                            format = CopyFormat::Binary;
                                        }
                                        "csv" => {
                                            if parser.delimiter.is_none() {
                                                parser.delimiter = Some(',');
                                            }
                                            format = CopyFormat::Csv;
                                        }
                                        _ => (),
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

                        "null" => {
                            if let Some(ref arg) = elem.arg {
                                if let Some(NodeEnum::String(ref string)) = arg.node {
                                    null_string = string.sval.clone();
                                }
                            }
                        }

                        _ => (),
                    }
                }
            }
        }

        parser.stream = if format == CopyFormat::Binary {
            CopyStream::Binary(BinaryStream::default())
        } else {
            CopyStream::Text(Box::new(CsvStream::new(
                parser.delimiter(),
                parser.headers,
                format,
                &null_string,
            )))
        };
        parser.sharding_schema = cluster.sharding_schema();

        Ok(parser)
    }

    #[inline]
    fn delimiter(&self) -> char {
        self.delimiter.unwrap_or('\t')
    }

    /// Split CopyData (F) messages into multiple CopyData (F) messages
    /// with shard numbers.
    pub fn shard(&mut self, data: &[CopyData]) -> Result<Vec<CopyRow>, Error> {
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
                            rows.push(CopyRow::new(
                                headers.to_string().as_bytes(),
                                self.schema_shard.clone().unwrap_or(Shard::All),
                            ));
                        }
                        self.headers = false;
                    }

                    for record in stream.records() {
                        // Totally broken.
                        let record = record?;

                        let shard = if let Some(table) = &self.sharded_table {
                            let key = record
                                .get(self.sharded_column)
                                .ok_or(Error::NoShardingColumn)?;

                            let ctx = ContextBuilder::new(table)
                                .data(key)
                                .shards(self.sharding_schema.shards)
                                .build()?;

                            ctx.apply()?
                        } else if let Some(schema_shard) = self.schema_shard.clone() {
                            schema_shard
                        } else {
                            Shard::All
                        };

                        rows.push(CopyRow::new(record.to_string().as_bytes(), shard));
                    }
                }

                CopyStream::Binary(stream) => {
                    if self.headers {
                        let header = stream.header()?;
                        rows.push(CopyRow::new(
                            &header.to_bytes()?,
                            self.schema_shard.clone().unwrap_or(Shard::All),
                        ));
                        self.headers = false;
                    }

                    for tuple in stream.tuples() {
                        let tuple = tuple?;
                        if tuple.end() {
                            let terminator = (-1_i16).to_be_bytes();
                            rows.push(CopyRow::new(
                                &terminator,
                                self.schema_shard.clone().unwrap_or(Shard::All),
                            ));
                            break;
                        }
                        let shard = if let Some(table) = &self.sharded_table {
                            let key = tuple
                                .get(self.sharded_column)
                                .ok_or(Error::NoShardingColumn)?;
                            if let Data::Column(key) = key {
                                let ctx = ContextBuilder::new(table)
                                    .data(&key[..])
                                    .shards(self.sharding_schema.shards)
                                    .build()?;

                                ctx.apply()?
                            } else {
                                Shard::All
                            }
                        } else if let Some(schema_shard) = self.schema_shard.clone() {
                            schema_shard
                        } else {
                            Shard::All
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

        let mut copy = CopyParser::new(&copy, &Cluster::default()).unwrap();

        assert_eq!(copy.delimiter(), '\t');
        assert!(!copy.headers);

        let one = CopyData::new("5\thello world\n".as_bytes());
        let two = CopyData::new("10\thowdy mate\n".as_bytes());
        let sharded = copy.shard(&[one, two]).unwrap();
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

        let mut copy = CopyParser::new(&copy, &Cluster::default()).unwrap();
        assert!(copy.is_from);

        assert_eq!(copy.delimiter(), ',');
        assert!(copy.headers);

        let header = CopyData::new("id,value\n".as_bytes());
        let one = CopyData::new("5,hello world\n".as_bytes());
        let two = CopyData::new("10,howdy mate\n".as_bytes());
        let sharded = copy.shard(&[header, one, two]).unwrap();

        assert_eq!(sharded[0].message().data(), b"\"id\",\"value\"\n");
        assert_eq!(sharded[1].message().data(), b"\"5\",\"hello world\"\n");
        assert_eq!(sharded[2].message().data(), b"\"10\",\"howdy mate\"\n");

        let partial_one = CopyData::new("11,howdy partner".as_bytes());
        let partial_two = CopyData::new("\n1,2".as_bytes());
        let partial_three = CopyData::new("\n".as_bytes());

        let sharded = copy.shard(&[partial_one]).unwrap();
        assert!(sharded.is_empty());
        let sharded = copy.shard(&[partial_two]).unwrap();
        assert_eq!(sharded[0].message().data(), b"\"11\",\"howdy partner\"\n");
        let sharded = copy.shard(&[partial_three]).unwrap();
        assert_eq!(sharded[0].message().data(), b"\"1\",\"2\"\n");
    }

    #[test]
    fn test_copy_csv_stream() {
        let copy_data = CopyData::new(b"id,value\n1,test\n6,test6\n");

        let copy = "COPY sharded (id, value) FROM STDIN CSV HEADER";
        let stmt = parse(copy).unwrap();
        let stmt = stmt.protobuf.stmts.first().unwrap();
        let copy = match stmt.stmt.clone().unwrap().node.unwrap() {
            NodeEnum::CopyStmt(copy) => copy,
            _ => panic!("not a copy"),
        };

        let mut copy = CopyParser::new(&copy, &Cluster::new_test()).unwrap();

        let rows = copy.shard(&[copy_data]).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].message(), CopyData::new(b"\"id\",\"value\"\n"));
        assert_eq!(rows[0].shard(), &Shard::All);
        assert_eq!(rows[1].message(), CopyData::new(b"\"1\",\"test\"\n"));
        assert_eq!(rows[2].message(), CopyData::new(b"\"6\",\"test6\"\n"));
        assert_eq!(rows[1].shard(), &Shard::Direct(0));
        assert_eq!(rows[2].shard(), &Shard::Direct(1));
    }

    #[test]
    fn test_copy_csv_custom_null() {
        let copy = "COPY sharded (id, value) FROM STDIN CSV NULL 'NULL'";
        let stmt = parse(copy).unwrap();
        let stmt = stmt.protobuf.stmts.first().unwrap();
        let copy = match stmt.stmt.clone().unwrap().node.unwrap() {
            NodeEnum::CopyStmt(copy) => copy,
            _ => panic!("not a copy"),
        };

        let mut copy = CopyParser::new(&copy, &Cluster::default()).unwrap();

        assert_eq!(copy.delimiter(), ',');
        assert!(!copy.headers);

        let data = CopyData::new("5,hello\n10,NULL\n15,world\n".as_bytes());
        let sharded = copy.shard(&[data]).unwrap();

        assert_eq!(sharded.len(), 3);
        assert_eq!(sharded[0].message().data(), b"\"5\",\"hello\"\n");
        assert_eq!(sharded[1].message().data(), b"\"10\",NULL\n");
        assert_eq!(sharded[2].message().data(), b"\"15\",\"world\"\n");
    }

    #[test]
    fn test_copy_binary() {
        let copy = "COPY sharded (id, value) FROM STDIN (FORMAT 'binary')";
        let stmt = parse(copy).unwrap();
        let stmt = stmt.protobuf.stmts.first().unwrap();
        let copy = match stmt.stmt.clone().unwrap().node.unwrap() {
            NodeEnum::CopyStmt(copy) => copy,
            _ => panic!("not a copy"),
        };

        let mut copy = CopyParser::new(&copy, &Cluster::default()).unwrap();
        assert!(copy.is_from);
        assert!(copy.headers);
        let mut data = b"PGCOPY".to_vec();
        data.push(b'\n');
        data.push(255);
        data.push(b'\r');
        data.push(b'\n');
        data.push(b'\0');
        data.extend(0_i32.to_be_bytes());
        data.extend(0_i32.to_be_bytes());
        data.extend(2_i16.to_be_bytes());
        data.extend(8_i32.to_be_bytes());
        data.extend(1234_i64.to_be_bytes());
        data.extend(3_i32.to_be_bytes());
        data.extend(b"yes");
        data.extend((-1_i16).to_be_bytes());
        let header = CopyData::new(data.as_slice());
        let sharded = copy.shard(&[header]).unwrap();
        assert_eq!(sharded.len(), 3);
        assert_eq!(sharded[0].message().data(), &data[..19]); // Header is 19 bytes long.
        assert_eq!(sharded[1].message().data().len(), 2 + 4 + 8 + 4 + 3);
        assert_eq!(sharded[2].message().data(), (-1_i16).to_be_bytes());
    }
}
