//! Parse COPY statement.

use std::sync::Arc;

use pg_query::{protobuf::CopyStmt, NodeEnum};
use tokio::sync::Mutex;

use crate::{
    backend::{schema::FkLookup, Cluster, ShardingSchema},
    config::ShardedTable,
    frontend::router::{
        parser::Shard,
        sharding::{ContextBuilder, Data as ShardingData},
        CopyRow,
    },
    net::messages::{CopyData, ToBytes},
};

use super::{
    binary::Data as BinaryData, BinaryStream, CsvStream, Error, SchemaLookupContext,
    StatementParser, Table,
};

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
    /// The primary key column position in each row.
    primary_key_column: Option<usize>,
    /// The FK column position for FK lookup (references parent table).
    fk_column: Option<usize>,
    /// Whether this COPY targets a sharded table.
    is_sharded: bool,
    /// FK lookup for fetching sharding key when table doesn't have it directly.
    fk_lookup: Option<Arc<Mutex<FkLookup>>>,
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
            primary_key_column: None,
            fk_column: None,
            is_sharded: false,
            fk_lookup: None,
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

        let sharding_schema = cluster.sharding_schema();
        let db_schema = cluster.schema();

        if let Some(ref rel) = stmt.relation {
            let table = Table::from(rel);

            // The CopyParser is used for replicating
            // data during data-sync. This will ensure all rows
            // are sent to the right schema-based shard.
            if let Some(schema) = sharding_schema.schemas.get(table.schema()) {
                parser.schema_shard = Some(schema.shard().into());
            }

            let schema_lookup = SchemaLookupContext {
                db_schema: &db_schema,
                user: "",
                search_path: None,
            };

            let mut statement_parser = StatementParser::from_copy(stmt, &sharding_schema)
                .with_schema_lookup(schema_lookup);

            parser.is_sharded = statement_parser.is_sharded(&db_schema, "", None);

            if let Some(sharding) = statement_parser.copy_sharding_key() {
                if let (Some(position), Some(sharded_table)) =
                    (sharding.key_position, sharding.key_table)
                {
                    parser.sharded_table = Some(sharded_table.clone());
                    parser.sharded_column = position;
                }
                parser.primary_key_column = sharding.primary_key_position;
            }

            // If table is sharded but doesn't have the sharding key directly,
            // create an FK lookup to fetch sharding key via FK relationships.
            if parser.is_sharded && parser.sharded_table.is_none() {
                if let Some(join) = db_schema.table_sharded_join(table, "", None) {
                    // Find the FK column position in COPY columns
                    if let Some(fk_col_name) = join.fk_column() {
                        // Get column names from COPY statement
                        let copy_columns: Vec<String> = stmt
                            .attlist
                            .iter()
                            .filter_map(|node| {
                                if let Some(NodeEnum::String(s)) = &node.node {
                                    Some(s.sval.clone())
                                } else {
                                    None
                                }
                            })
                            .collect();

                        if let Some(pos) = copy_columns.iter().position(|c| c == fk_col_name) {
                            parser.fk_column = Some(pos);
                            parser.fk_lookup = Self::build_fk_lookup(&join, cluster);
                        }
                    }
                }
            }

            parser.columns = stmt.attlist.len();

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

    /// Build an FK lookup from a join and cluster.
    fn build_fk_lookup(
        join: &crate::backend::schema::Join,
        cluster: &Cluster,
    ) -> Option<Arc<Mutex<FkLookup>>> {
        Some(Arc::new(Mutex::new(FkLookup::new(
            join.clone(),
            cluster.clone(),
        ))))
    }

    /// Override the cluster used for FK lookups (e.g., during replication).
    /// Rebuilds the FK lookup with the new cluster.
    pub fn set_fk_lookup_cluster(&mut self, cluster: &Cluster) {
        if let Some(ref fk_lookup) = self.fk_lookup {
            let join = fk_lookup.blocking_lock().join().clone();
            self.fk_lookup = Self::build_fk_lookup(&join, cluster);
        }
    }

    /// Split CopyData (F) messages into multiple CopyData (F) messages
    /// with shard numbers.
    pub async fn shard(&mut self, data: &[CopyData]) -> Result<Vec<CopyRow>, Error> {
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
                        } else if let Some(fk_lookup) = self.fk_lookup.as_ref() {
                            if let Some(fk_col) = self.fk_column {
                                let fk_value = record.get(fk_col).ok_or(Error::NoShardingColumn)?;
                                if fk_value == self.null_string {
                                    Shard::All
                                } else {
                                    match fk_lookup
                                        .lock()
                                        .await
                                        .lookup(ShardingData::Text(fk_value))
                                        .await
                                    {
                                        Ok(shard) => shard,
                                        Err(_) => Shard::All,
                                    }
                                }
                            } else {
                                Shard::All
                            }
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
                            if let BinaryData::Column(key) = key {
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
                        } else if let Some(fk_lookup) = self.fk_lookup.as_ref() {
                            if let Some(fk_col) = self.fk_column {
                                let fk_value = tuple.get(fk_col).ok_or(Error::NoShardingColumn)?;
                                if let BinaryData::Column(fk_bytes) = fk_value {
                                    match fk_lookup
                                        .lock()
                                        .await
                                        .lookup(ShardingData::Binary(&fk_bytes[..]))
                                        .await
                                    {
                                        Ok(shard) => shard,
                                        Err(_) => Shard::All,
                                    }
                                } else {
                                    Shard::All
                                }
                            } else {
                                Shard::All
                            }
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

    #[tokio::test]
    async fn test_copy_text() {
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
        let sharded = copy.shard(&[one, two]).await.unwrap();
        assert_eq!(sharded[0].message().data(), b"5\thello world\n");
        assert_eq!(sharded[1].message().data(), b"10\thowdy mate\n");
    }

    #[tokio::test]
    async fn test_copy_csv() {
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
        let sharded = copy.shard(&[header, one, two]).await.unwrap();

        assert_eq!(sharded[0].message().data(), b"\"id\",\"value\"\n");
        assert_eq!(sharded[1].message().data(), b"\"5\",\"hello world\"\n");
        assert_eq!(sharded[2].message().data(), b"\"10\",\"howdy mate\"\n");

        let partial_one = CopyData::new("11,howdy partner".as_bytes());
        let partial_two = CopyData::new("\n1,2".as_bytes());
        let partial_three = CopyData::new("\n".as_bytes());

        let sharded = copy.shard(&[partial_one]).await.unwrap();
        assert!(sharded.is_empty());
        let sharded = copy.shard(&[partial_two]).await.unwrap();
        assert_eq!(sharded[0].message().data(), b"\"11\",\"howdy partner\"\n");
        let sharded = copy.shard(&[partial_three]).await.unwrap();
        assert_eq!(sharded[0].message().data(), b"\"1\",\"2\"\n");
    }

    #[tokio::test]
    async fn test_copy_csv_stream() {
        let copy_data = CopyData::new(b"id,value\n1,test\n6,test6\n");

        let copy = "COPY sharded (id, value) FROM STDIN CSV HEADER";
        let stmt = parse(copy).unwrap();
        let stmt = stmt.protobuf.stmts.first().unwrap();
        let copy = match stmt.stmt.clone().unwrap().node.unwrap() {
            NodeEnum::CopyStmt(copy) => copy,
            _ => panic!("not a copy"),
        };

        let mut copy = CopyParser::new(&copy, &Cluster::new_test(&config())).unwrap();

        let rows = copy.shard(&[copy_data]).await.unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].message(), CopyData::new(b"\"id\",\"value\"\n"));
        assert_eq!(rows[0].shard(), &Shard::All);
        assert_eq!(rows[1].message(), CopyData::new(b"\"1\",\"test\"\n"));
        assert_eq!(rows[2].message(), CopyData::new(b"\"6\",\"test6\"\n"));
        assert_eq!(rows[1].shard(), &Shard::Direct(0));
        assert_eq!(rows[2].shard(), &Shard::Direct(1));
    }

    #[tokio::test]
    async fn test_copy_csv_custom_null() {
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
        let sharded = copy.shard(&[data]).await.unwrap();

        assert_eq!(sharded.len(), 3);
        assert_eq!(sharded[0].message().data(), b"\"5\",\"hello\"\n");
        assert_eq!(sharded[1].message().data(), b"\"10\",NULL\n");
        assert_eq!(sharded[2].message().data(), b"\"15\",\"world\"\n");
    }

    #[tokio::test]
    async fn test_copy_text_pg_dump_end_marker() {
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

        let sharded = copy.shard(&[one, two, end_marker]).await.unwrap();
        assert_eq!(sharded.len(), 3);
        assert_eq!(sharded[0].message().data(), b"1\tAlice\n");
        assert_eq!(sharded[0].shard(), &Shard::Direct(0));
        assert_eq!(sharded[1].message().data(), b"6\tBob\n");
        assert_eq!(sharded[1].shard(), &Shard::Direct(1));
        assert_eq!(sharded[2].message().data(), b"\\.\n");
        assert_eq!(sharded[2].shard(), &Shard::All);
    }

    #[tokio::test]
    async fn test_copy_text_null_sharding_key() {
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

        let sharded = copy.shard(&[one, two, three, four]).await.unwrap();
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

    #[tokio::test]
    async fn test_copy_text_composite_type_sharded() {
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
        let sharded = copy.shard(&[row]).await.unwrap();

        assert_eq!(sharded.len(), 1);

        // The output should preserve the quotes exactly
        assert_eq!(
            sharded[0].message().data(),
            b"1\t(,Annapolis,Maryland,\"United States\",)\n",
            "Composite type quotes should be preserved in sharded COPY"
        );
    }

    #[tokio::test]
    async fn test_copy_explicit_text_format() {
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
        let sharded = copy.shard(&[row]).await.unwrap();

        assert_eq!(sharded.len(), 1);
        assert_eq!(
            sharded[0].message().data(),
            b"1\t(,Annapolis,Maryland,\"United States\",)\n",
            "Explicit FORMAT text should preserve quotes"
        );
    }

    #[tokio::test]
    async fn test_copy_binary() {
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
        let sharded = copy.shard(&[header]).await.unwrap();
        assert_eq!(sharded.len(), 3);
        assert_eq!(sharded[0].message().data(), &data[..19]); // Header is 19 bytes long.
        assert_eq!(sharded[1].message().data().len(), 2 + 4 + 8 + 4 + 3);
        assert_eq!(sharded[2].message().data(), (-1_i16).to_be_bytes());
    }

    #[test]
    fn test_copy_fk_lookup_setup() {
        use crate::backend::schema::test_helpers::prelude::*;

        // Create schema with FK relationships:
        //   users (id PK, customer_id - sharding key)
        //   orders (id PK, user_id FK -> users.id)
        //   order_items (id PK, order_id FK -> orders.id)
        let mut db_schema = schema()
            .relation(
                table("users")
                    .oid(1001)
                    .column(pk("id"))
                    .column(col("customer_id")),
            )
            .relation(
                table("orders")
                    .oid(1002)
                    .column(pk("id"))
                    .column(fk("user_id", "users", "id")),
            )
            .relation(
                table("order_items")
                    .oid(1003)
                    .column(pk("id"))
                    .column(fk("order_id", "orders", "id")),
            )
            .build();

        let sharding_schema = sharding().sharded_table("users", "customer_id").build();

        // Compute joins so tables are marked as sharded and join paths are stored
        db_schema.computed_sharded_joins(&sharding_schema);

        // Verify users has sharding key directly (no join stored, but is_sharded = true)
        let users_table = Table {
            name: "users",
            schema: Some("public"),
            alias: None,
        };
        let users = db_schema.table(users_table, "", None).unwrap();
        assert!(users.is_sharded);
        assert!(db_schema.get_sharded_join(users).is_none());

        // Verify orders has a join to users (is_sharded = true, join stored)
        let orders_table = Table {
            name: "orders",
            schema: Some("public"),
            alias: None,
        };
        let orders = db_schema.table(orders_table, "", None).unwrap();
        assert!(orders.is_sharded);
        let orders_join = db_schema.get_sharded_join(orders);
        assert!(orders_join.is_some());
        let orders_join = orders_join.unwrap();
        assert_eq!(orders_join.path.len(), 1);
        assert_eq!(orders_join.fk_column(), Some("user_id"));
        // Query should directly look up sharding key from users
        assert!(
            orders_join.query.contains("users"),
            "query should reference users table"
        );
        assert!(
            orders_join.query.contains("customer_id"),
            "query should select customer_id"
        );
        assert!(
            !orders_join.query.contains("orders"),
            "query should NOT reference orders table"
        );

        // Verify order_items has a 2-hop join to users
        let order_items_table = Table {
            name: "order_items",
            schema: Some("public"),
            alias: None,
        };
        let order_items = db_schema.table(order_items_table, "", None).unwrap();
        assert!(order_items.is_sharded);
        let order_items_join = db_schema.get_sharded_join(order_items);
        assert!(order_items_join.is_some());
        let order_items_join = order_items_join.unwrap();
        assert_eq!(order_items_join.path.len(), 2);
        assert_eq!(order_items_join.fk_column(), Some("order_id"));
        // Query should start from orders (first hop target), not order_items
        assert!(
            order_items_join.query.contains("orders"),
            "query should reference orders table"
        );
        assert!(
            order_items_join.query.contains("users"),
            "query should reference users table"
        );
        assert!(
            !order_items_join.query.contains("order_items"),
            "query should NOT reference order_items table"
        );
    }

    /// Test that CopyParser correctly sets up FK lookup when parsing COPY for a table
    /// that doesn't have the sharding key directly but has an FK path to it.
    #[tokio::test]
    async fn test_copy_parser_fk_lookup_initialization() {
        use crate::backend::pool::test::pool;
        use crate::backend::pool::Request;
        use crate::backend::schema::Schema;
        use crate::backend::Cluster;
        use crate::config::{DataType, Hasher, ShardedTable};

        let pool = pool();
        let mut conn = pool.get(&Request::default()).await.unwrap();

        // Create FK tables in the database
        conn.execute("DROP TABLE IF EXISTS pgdog.fk_copy_orders, pgdog.fk_copy_users")
            .await
            .unwrap();

        conn.execute(
            "CREATE TABLE pgdog.fk_copy_users (
                id BIGINT PRIMARY KEY,
                customer_id BIGINT NOT NULL
            )",
        )
        .await
        .unwrap();

        conn.execute(
            "CREATE TABLE pgdog.fk_copy_orders (
                id BIGINT PRIMARY KEY,
                user_id BIGINT REFERENCES pgdog.fk_copy_users(id)
            )",
        )
        .await
        .unwrap();

        // Load schema from database
        let mut db_schema = Schema::load(&mut conn).await.unwrap();

        // Create sharded tables config with customer_id as sharding key for fk_copy_users
        let sharded_tables = crate::backend::ShardedTables::new(
            vec![ShardedTable {
                database: "pgdog".into(),
                name: Some("fk_copy_users".into()),
                schema: Some("pgdog".into()),
                column: "customer_id".into(),
                primary: true,
                data_type: DataType::Bigint,
                hasher: Hasher::Postgres,
                ..Default::default()
            }],
            vec![],
            false, // omnisharded_sticky
            pgdog_config::SystemCatalogsBehavior::default(),
        );

        // Create sharding schema from the config
        let sharding_schema = crate::backend::ShardingSchema {
            shards: 2,
            tables: sharded_tables.clone(),
            schemas: crate::backend::replication::ShardedSchemas::new(vec![]),
            ..Default::default()
        };

        // Compute joins so tables are marked as sharded
        db_schema.computed_sharded_joins(&sharding_schema);

        // Verify the schema was loaded correctly with FK relationships
        let orders_table = Table {
            name: "fk_copy_orders",
            schema: Some("pgdog"),
            alias: None,
        };
        let orders = db_schema.table(orders_table, "", None);
        assert!(orders.is_some(), "fk_copy_orders should be in schema");
        assert!(
            orders.unwrap().is_sharded,
            "fk_copy_orders should be marked as sharded"
        );

        let orders_join = orders.and_then(|r| db_schema.get_sharded_join(r));
        assert!(
            orders_join.is_some(),
            "fk_copy_orders should have a join path"
        );

        // Create a test cluster with custom sharding config and schema
        let cluster = Cluster::new_test_with_sharding(sharded_tables, db_schema);
        cluster.launch();

        // Parse COPY statement for orders table (has FK, no direct sharding key)
        let copy = "COPY pgdog.fk_copy_orders (id, user_id) FROM STDIN";
        let stmt = parse(copy).unwrap();
        let stmt = stmt.protobuf.stmts.first().unwrap();
        let copy_stmt = match stmt.stmt.clone().unwrap().node.unwrap() {
            NodeEnum::CopyStmt(copy) => copy,
            _ => panic!("not a copy"),
        };

        let mut parser = CopyParser::new(&copy_stmt, &cluster).unwrap();

        // Verify FK lookup is set up
        assert!(
            parser.fk_lookup.is_some(),
            "fk_lookup should be set for table with FK path to sharding key"
        );
        assert_eq!(
            parser.fk_column,
            Some(1),
            "fk_column should be position 1 (user_id is second column)"
        );
        assert!(
            parser.is_sharded,
            "fk_copy_orders table should be marked as sharded"
        );
        assert!(
            parser.sharded_table.is_none(),
            "fk_copy_orders table doesn't have sharding key directly"
        );

        // Insert users with known customer_id values for FK lookup
        for i in 1i64..=10 {
            conn.execute(&format!(
                "INSERT INTO pgdog.fk_copy_users (id, customer_id) VALUES ({}, {})",
                i,
                i * 100 // customer_id determines shard
            ))
            .await
            .unwrap();
        }

        // Create COPY text format data - orders referencing users
        // Format: order_id<tab>user_id<newline>
        use crate::net::messages::CopyData;

        let copy_rows: String = (1i64..=10)
            .map(|i| format!("{}\t{}\n", i * 10, i)) // order_id, user_id (FK)
            .collect();

        let copy_data = CopyData::new(copy_rows.as_bytes());
        let rows = parser.shard(&[copy_data]).await.unwrap();

        // Verify each row was routed to a specific shard (not Shard::All)
        assert_eq!(rows.len(), 10, "should have 10 rows");
        for (i, row) in rows.iter().enumerate() {
            assert!(
                matches!(row.shard(), super::Shard::Direct(_)),
                "row {} should be routed to a specific shard, got {:?}",
                i,
                row.shard()
            );
        }

        // Clean up
        conn.execute("DROP TABLE IF EXISTS pgdog.fk_copy_orders, pgdog.fk_copy_users")
            .await
            .unwrap();
        cluster.shutdown();
    }

    /// Test that CopyParser does NOT set up FK lookup when table has sharding key directly.
    #[tokio::test]
    async fn test_copy_parser_direct_sharding_key() {
        use crate::backend::pool::test::pool;
        use crate::backend::pool::Request;
        use crate::backend::schema::Schema;
        use crate::backend::Cluster;
        use crate::config::{DataType, Hasher, ShardedTable};

        let pool = pool();
        let mut conn = pool.get(&Request::default()).await.unwrap();

        // Create FK tables in the database
        conn.execute("DROP TABLE IF EXISTS pgdog.fk_copy_orders, pgdog.fk_copy_users")
            .await
            .unwrap();

        conn.execute(
            "CREATE TABLE pgdog.fk_copy_users (
                id BIGINT PRIMARY KEY,
                customer_id BIGINT NOT NULL
            )",
        )
        .await
        .unwrap();

        // Load schema from database
        let mut db_schema = Schema::load(&mut conn).await.unwrap();

        // Create sharded tables config with customer_id as sharding key for fk_copy_users
        let sharded_tables = crate::backend::ShardedTables::new(
            vec![ShardedTable {
                database: "pgdog".into(),
                name: Some("fk_copy_users".into()),
                schema: Some("pgdog".into()),
                column: "customer_id".into(),
                primary: true,
                data_type: DataType::Bigint,
                hasher: Hasher::Postgres,
                ..Default::default()
            }],
            vec![],
            false, // omnisharded_sticky
            pgdog_config::SystemCatalogsBehavior::default(),
        );

        // Create sharding schema from the config
        let sharding_schema = crate::backend::ShardingSchema {
            shards: 2,
            tables: sharded_tables.clone(),
            schemas: crate::backend::replication::ShardedSchemas::new(vec![]),
            ..Default::default()
        };

        // Compute joins so tables are marked as sharded
        db_schema.computed_sharded_joins(&sharding_schema);

        // Create a test cluster with custom sharding config and schema
        let cluster = Cluster::new_test_with_sharding(sharded_tables, db_schema);

        // Parse COPY statement for users table (has sharding key directly)
        let copy = "COPY pgdog.fk_copy_users (id, customer_id) FROM STDIN";
        let stmt = parse(copy).unwrap();
        let stmt = stmt.protobuf.stmts.first().unwrap();
        let copy_stmt = match stmt.stmt.clone().unwrap().node.unwrap() {
            NodeEnum::CopyStmt(copy) => copy,
            _ => panic!("not a copy"),
        };

        let mut parser = CopyParser::new(&copy_stmt, &cluster).unwrap();

        // Verify direct sharding is used, not FK lookup
        assert!(
            parser.fk_lookup.is_none(),
            "fk_lookup should NOT be set for table with direct sharding key"
        );
        assert!(
            parser.is_sharded,
            "fk_copy_users table should be marked as sharded"
        );
        assert!(
            parser.sharded_table.is_some(),
            "fk_copy_users table has sharding key directly"
        );
        assert_eq!(parser.sharded_column, 1, "customer_id is at position 1");

        // Create COPY text format data - users with customer_id (sharding key)
        // Format: id<tab>customer_id<newline>
        use crate::net::messages::CopyData;

        let copy_rows: String = (1i64..=10)
            .map(|i| format!("{}\t{}\n", i, i * 100)) // id, customer_id (sharding key)
            .collect();

        let copy_data = CopyData::new(copy_rows.as_bytes());
        let rows = parser.shard(&[copy_data]).await.unwrap();

        // Verify each row was routed to a specific shard (not Shard::All)
        assert_eq!(rows.len(), 10, "should have 10 rows");
        for (i, row) in rows.iter().enumerate() {
            assert!(
                matches!(row.shard(), super::Shard::Direct(_)),
                "row {} should be routed to a specific shard, got {:?}",
                i,
                row.shard()
            );
        }

        // Clean up
        conn.execute("DROP TABLE IF EXISTS pgdog.fk_copy_orders, pgdog.fk_copy_users")
            .await
            .unwrap();
    }
}
