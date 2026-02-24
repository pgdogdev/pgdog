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
    /// String representing NULL values in text/CSV format.
    null_string: String,
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
            null_string: "\\N".to_owned(),
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
        parser.null_string = null_string;

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

                        // pg_dump text format uses `\.` as end-of-copy marker.
                        let is_end_marker = record.len() == 1 && record.get(0) == Some("\\.");

                        let shard = if is_end_marker {
                            Shard::All
                        } else if let Some(table) = &self.sharded_table {
                            let key = record
                                .get(self.sharded_column)
                                .ok_or(Error::NoShardingColumn)?;

                            if key == self.null_string {
                                Shard::All
                            } else {
                                let ctx = ContextBuilder::new(table)
                                    .data(key)
                                    .shards(self.sharding_schema.shards)
                                    .build()?;

                                ctx.apply()?
                            }
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
                        if let Some(header) = stream.header()? {
                            rows.push(CopyRow::new(
                                &header.to_bytes()?,
                                self.schema_shard.clone().unwrap_or(Shard::All),
                            ));
                            self.headers = false;
                        }
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

    use crate::config::config;

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

        let mut copy = CopyParser::new(&copy, &Cluster::new_test(&config())).unwrap();

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
    fn test_copy_text_pg_dump_end_marker() {
        // pg_dump generates text format COPY with `\.` as end-of-copy marker.
        // This marker should be sent to all shards without extracting a sharding key.
        let copy = "COPY sharded (id, value) FROM STDIN";
        let stmt = parse(copy).unwrap();
        let stmt = stmt.protobuf.stmts.first().unwrap();
        let copy = match stmt.stmt.clone().unwrap().node.unwrap() {
            NodeEnum::CopyStmt(copy) => copy,
            _ => panic!("not a copy"),
        };

        let mut copy = CopyParser::new(&copy, &Cluster::new_test(&config())).unwrap();

        let one = CopyData::new("1\tAlice\n".as_bytes());
        let two = CopyData::new("6\tBob\n".as_bytes());
        let end_marker = CopyData::new("\\.\n".as_bytes());

        let sharded = copy.shard(&[one, two, end_marker]).unwrap();
        assert_eq!(sharded.len(), 3);
        assert_eq!(sharded[0].message().data(), b"1\tAlice\n");
        assert_eq!(sharded[0].shard(), &Shard::Direct(0));
        assert_eq!(sharded[1].message().data(), b"6\tBob\n");
        assert_eq!(sharded[1].shard(), &Shard::Direct(1));
        assert_eq!(sharded[2].message().data(), b"\\.\n");
        assert_eq!(sharded[2].shard(), &Shard::All);
    }

    #[test]
    fn test_copy_text_null_sharding_key() {
        // pg_dump text format uses `\N` to represent NULL values.
        // When the sharding key is NULL, route to all shards.
        // When a non-sharding column is NULL, route normally based on the key.
        let copy = "COPY sharded (id, value) FROM STDIN";
        let stmt = parse(copy).unwrap();
        let stmt = stmt.protobuf.stmts.first().unwrap();
        let copy = match stmt.stmt.clone().unwrap().node.unwrap() {
            NodeEnum::CopyStmt(copy) => copy,
            _ => panic!("not a copy"),
        };

        let mut copy = CopyParser::new(&copy, &Cluster::new_test(&config())).unwrap();

        let one = CopyData::new("1\tAlice\n".as_bytes());
        let two = CopyData::new("\\N\tBob\n".as_bytes());
        let three = CopyData::new("11\tCharlie\n".as_bytes());
        let four = CopyData::new("6\t\\N\n".as_bytes());

        let sharded = copy.shard(&[one, two, three, four]).unwrap();
        assert_eq!(sharded.len(), 4);
        assert_eq!(sharded[0].message().data(), b"1\tAlice\n");
        assert_eq!(sharded[0].shard(), &Shard::Direct(0));
        assert_eq!(sharded[1].message().data(), b"\\N\tBob\n");
        assert_eq!(sharded[1].shard(), &Shard::All);
        assert_eq!(sharded[2].message().data(), b"11\tCharlie\n");
        assert_eq!(sharded[2].shard(), &Shard::Direct(1));
        assert_eq!(sharded[3].message().data(), b"6\t\\N\n");
        assert_eq!(sharded[3].shard(), &Shard::Direct(1));
    }

    #[test]
    fn test_copy_text_composite_type_sharded() {
        // Test the same composite type but with sharding enabled (using the sharded table from config)
        let copy = "COPY sharded (id, value) FROM STDIN";
        let stmt = parse(copy).unwrap();
        let stmt = stmt.protobuf.stmts.first().unwrap();
        let copy = match stmt.stmt.clone().unwrap().node.unwrap() {
            NodeEnum::CopyStmt(copy) => copy,
            _ => panic!("not a copy"),
        };

        let mut copy = CopyParser::new(&copy, &Cluster::new_test(&config())).unwrap();

        // Row where the value contains a composite type with commas and quotes
        let row = CopyData::new(b"1\t(,Annapolis,Maryland,\"United States\",)\n");
        let sharded = copy.shard(&[row]).unwrap();

        assert_eq!(sharded.len(), 1);

        // The output should preserve the quotes exactly
        assert_eq!(
            sharded[0].message().data(),
            b"1\t(,Annapolis,Maryland,\"United States\",)\n",
            "Composite type quotes should be preserved in sharded COPY"
        );
    }

    #[test]
    fn test_copy_explicit_text_format() {
        // Test with explicit FORMAT text (like during resharding)
        let copy = r#"COPY "public"."entity_values" ("id", "value_location") FROM STDIN WITH (FORMAT text)"#;
        let stmt = parse(copy).unwrap();
        let stmt = stmt.protobuf.stmts.first().unwrap();
        let copy_stmt = match stmt.stmt.clone().unwrap().node.unwrap() {
            NodeEnum::CopyStmt(copy) => copy,
            _ => panic!("not a copy"),
        };

        let mut copy = CopyParser::new(&copy_stmt, &Cluster::default()).unwrap();

        // Verify it's using tab delimiter (text format default)
        assert_eq!(
            copy.delimiter(),
            '\t',
            "Text format should use tab delimiter"
        );

        // Row with composite type
        let row = CopyData::new(b"1\t(,Annapolis,Maryland,\"United States\",)\n");
        let sharded = copy.shard(&[row]).unwrap();

        assert_eq!(sharded.len(), 1);
        assert_eq!(
            sharded[0].message().data(),
            b"1\t(,Annapolis,Maryland,\"United States\",)\n",
            "Explicit FORMAT text should preserve quotes"
        );
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
