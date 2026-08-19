//! Parse COPY statement.

use pg_raw_parse::nodes;

use pgdog_config::LookupResult;
use std::sync::Arc;

use crate::{
    backend::{Cluster, ShardingSchema},
    frontend::router::{
        CopyRow,
        parser::Shard,
        sharding::{ContextBuilder, ShardedTable, Tables, Value as ShardingValue, lookup},
    },
    net::messages::{CopyData, ToBytes},
};

use super::{BinaryStream, Column, CsvStream, Error, Table, binary::Data};

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
    /// Cluster answering sharding key lookups. The serving cluster,
    /// except during resharding data sync, where the source cluster
    /// has the complete mapping while the destination is still syncing.
    lookup_cluster: Option<Cluster>,
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
            lookup_cluster: None,
        }
    }
}

impl CopyParser {
    /// Create new copy parser from a COPY statement.
    pub fn new(stmt: &nodes::CopyStmt, cluster: &Cluster) -> Result<Self, Error> {
        let mut parser = Self {
            is_from: stmt.is_from,
            ..Default::default()
        };

        let mut format = CopyFormat::Text;
        let mut null_string = "\\N".to_owned();

        if let Some(rel) = stmt.relation() {
            let columns = stmt
                .attlist()
                .iter()
                .map(|column| {
                    Column::from(column.as_str().expect("attlist is empty or all strings"))
                })
                .collect::<Vec<_>>();

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

            for elem in stmt.options() {
                match elem.defname().unwrap_or_default().to_lowercase().as_str() {
                    "format" => match elem.arg().as_str().map(|s| s.to_lowercase()).as_deref() {
                        Some("binary") => {
                            parser.headers = true;
                            format = CopyFormat::Binary;
                        }
                        Some("csv") => {
                            if parser.delimiter.is_none() {
                                parser.delimiter = Some(',');
                            }
                            format = CopyFormat::Csv;
                        }
                        _ => (),
                    },

                    "delimiter" => {
                        if let Some(string) = elem.arg().as_str() {
                            parser.delimiter = Some(string.chars().next().unwrap_or(','));
                        }
                    }

                    "header" => {
                        parser.headers = true;
                    }

                    "null" => {
                        if let Some(string) = elem.arg().as_str() {
                            null_string = string.to_owned();
                        }
                    }

                    _ => (),
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
        parser.lookup_cluster = Some(cluster.clone());
        parser.null_string = null_string;

        Ok(parser)
    }

    #[inline]
    fn delimiter(&self) -> char {
        self.delimiter.unwrap_or('\t')
    }

    /// Hash a sharding key and pick a shard.
    fn shard_for_key(
        table: &ShardedTable,
        schema: &ShardingSchema,
        key: &str,
    ) -> Result<Shard, Error> {
        Ok(ContextBuilder::new(table)
            .data(key)
            .shards(schema.shards)
            .build()?
            .apply()?)
    }

    /// Shard a translated lookup result. Shard-mode lookups never
    /// hash: the translation is the shard number.
    fn shard_for_translated(
        table: &ShardedTable,
        schema: &ShardingSchema,
        translated: &str,
    ) -> Result<Shard, Error> {
        match table.lookup_result {
            LookupResult::Shard => Ok(lookup::parse_shard_index(translated, schema.shards)?),
            LookupResult::Value => Self::shard_for_key(table, schema, translated),
        }
    }

    /// Set the cluster answering sharding key lookups, e.g. the source
    /// cluster during resharding data sync.
    pub fn set_lookup_cluster(&mut self, cluster: &Cluster) {
        self.lookup_cluster = Some(cluster.clone());
    }

    /// Translate a sharding key through the table's lookup: from the
    /// cache, or by running the lookup query. Runs per COPY row: the
    /// cache-hit path allocates nothing.
    async fn translate_key(
        cluster: &Cluster,
        schema: &ShardingSchema,
        table: &ShardedTable,
        key: &str,
    ) -> Result<Arc<str>, Error> {
        if table.lookup_query.is_none() {
            return Err(Error::NoShardingColumn);
        }

        lookup::resolve_for_table(cluster, schema.tables.lookup_cache(), table, key)
            .await
            .map_err(|err| Error::Lookup(err.message))
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
                            } else if table.lookup_query.is_some() {
                                let translated = Self::translate_key(
                                    self.lookup_cluster
                                        .as_ref()
                                        .ok_or(Error::NoShardingColumn)?,
                                    &self.sharding_schema,
                                    table,
                                    key,
                                )
                                .await?;
                                Self::shard_for_translated(
                                    table,
                                    &self.sharding_schema,
                                    translated.as_ref(),
                                )?
                            } else {
                                Self::shard_for_key(table, &self.sharding_schema, key)?
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
                    if self.headers
                        && let Some(header) = stream.header()?
                    {
                        rows.push(CopyRow::new(
                            &header.to_bytes(),
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
                                if table.lookup_query.is_some() {
                                    // Binary values are decoded to their text
                                    // form to translate through the lookup.
                                    let value = ShardingValue::new(&key[..], table.data_type);
                                    let text = value.to_text()?.ok_or(Error::LookupBinaryCopy)?;
                                    let translated = Self::translate_key(
                                        self.lookup_cluster
                                            .as_ref()
                                            .ok_or(Error::NoShardingColumn)?,
                                        &self.sharding_schema,
                                        table,
                                        &text,
                                    )
                                    .await?;
                                    Self::shard_for_translated(
                                        table,
                                        &self.sharding_schema,
                                        translated.as_ref(),
                                    )?
                                } else {
                                    let ctx = ContextBuilder::new(table)
                                        .data(&key[..])
                                        .shards(self.sharding_schema.shards)
                                        .build()?;

                                    ctx.apply()?
                                }
                            } else {
                                Shard::All
                            }
                        } else if let Some(schema_shard) = self.schema_shard.clone() {
                            schema_shard
                        } else {
                            Shard::All
                        };

                        rows.push(CopyRow::new(&tuple.to_bytes(), shard));
                    }
                }
            }
        }

        Ok(rows)
    }
}

#[cfg(test)]
mod test {
    use crate::config::config;
    use pg_raw_parse::{Node, Owned, make};

    use super::*;
    use crate::frontend::router::sharding::lookup::LookupTable;

    #[tokio::test]
    async fn test_copy_text() {
        let copy = parse("COPY sharded (id, value) FROM STDIN");
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
    async fn test_copy_lookup_translates_keys() {
        use crate::backend::ShardedTables;
        use crate::frontend::router::sharding::shard_value;
        use pgdog_config::{DataType, SystemCatalogsBehavior};

        let stmt = parse("COPY sharded_lookup (tenant_id, value) FROM STDIN CSV");
        let mut copy = CopyParser::new(&stmt, &Cluster::new_test(&config())).unwrap();

        let query = "SELECT COALESCE(parent, id) FROM tenants WHERE id = $1";
        let table = ShardedTable {
            column: "tenant_id".into(),
            name: Some("sharded_lookup".into()),
            data_type: DataType::Varchar,
            lookup_query: Some(query.into()),
            ..Default::default()
        };
        let schema = ShardingSchema {
            shards: 3,
            tables: ShardedTables::new(
                vec![table.clone()],
                vec![],
                false,
                SystemCatalogsBehavior::default(),
            ),
            ..Default::default()
        };
        copy.sharded_table = Some(table);
        copy.sharded_column = 0;
        copy.sharding_schema = schema.clone();

        let key = LookupTable {
            schema: None,
            name: Some("sharded_lookup".into()),
            column: "tenant_id".into(),
        };
        let cache = schema.tables.lookup_cache();
        cache.insert(key.clone(), "t_a".into(), "root_a".into());
        cache.insert(key, "t_b".into(), "root_b".into());

        let data = CopyData::new("t_a,hello\nt_b,world\n".as_bytes());
        let rows = copy.shard(&[data]).await.unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].message().data(), b"\"t_a\",\"hello\"\n");
        assert_eq!(
            rows[0].shard(),
            &shard_value("root_a", &DataType::Varchar, 3, &vec![], 0)
        );
        assert_eq!(rows[1].message().data(), b"\"t_b\",\"world\"\n");
        assert_eq!(
            rows[1].shard(),
            &shard_value("root_b", &DataType::Varchar, 3, &vec![], 0)
        );

        // A value that's not cached resolves against the lookup
        // cluster; the default cluster has no shards, so it errors
        // instead of routing by the untranslated value.
        let data = CopyData::new("t_cold,nope\n".as_bytes());
        assert!(copy.shard(&[data]).await.is_err());
    }

    #[tokio::test]
    async fn test_copy_csv() {
        let copy = parse("COPY sharded (id, value) FROM STDIN CSV HEADER");
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

        let copy = parse("COPY sharded (id, value) FROM STDIN CSV HEADER");
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
        let copy = parse("COPY sharded (id, value) FROM STDIN CSV NULL 'NULL'");
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
        let copy = parse("COPY sharded (id, value) FROM STDIN");
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
        let copy = parse("COPY sharded (id, value) FROM STDIN");
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
        let copy = parse("COPY sharded (id, value) FROM STDIN");
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
        let sql = r#"COPY "public"."entity_values" ("id", "value_location") FROM STDIN WITH (FORMAT text)"#;
        let copy_stmt = parse(sql);
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
        let copy = parse("COPY sharded (id, value) FROM STDIN (FORMAT 'binary')");
        let mut copy = CopyParser::new(&copy, &Cluster::new_test(&config())).unwrap();
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
        assert_eq!(sharded[0].shard(), &Shard::All);
        assert_eq!(sharded[1].message().data().len(), 2 + 4 + 8 + 4 + 3);
        assert!(matches!(sharded[1].shard(), &Shard::Direct(_)));
        assert_eq!(sharded[2].message().data(), (-1_i16).to_be_bytes());
        assert_eq!(sharded[2].shard(), &Shard::All)
    }

    #[tokio::test]
    async fn test_copy_binary_lookup_translates_keys() {
        use crate::backend::ShardedTables;
        use crate::frontend::router::sharding::shard_value;
        use pgdog_config::{DataType, SystemCatalogsBehavior};

        let stmt = parse("COPY sharded_lookup (customer_id, value) FROM STDIN (FORMAT 'binary')");
        let mut copy = CopyParser::new(&stmt, &Cluster::new_test(&config())).unwrap();

        let query = "SELECT COALESCE(parent, id) FROM customers WHERE id = $1";
        let table = ShardedTable {
            column: "customer_id".into(),
            name: Some("sharded_lookup".into()),
            data_type: DataType::Bigint,
            lookup_query: Some(query.into()),
            ..Default::default()
        };
        let schema = ShardingSchema {
            shards: 3,
            tables: ShardedTables::new(
                vec![table.clone()],
                vec![],
                false,
                SystemCatalogsBehavior::default(),
            ),
            ..Default::default()
        };
        copy.sharded_table = Some(table);
        copy.sharded_column = 0;
        copy.sharding_schema = schema.clone();

        let key = LookupTable {
            schema: None,
            name: Some("sharded_lookup".into()),
            column: "customer_id".into(),
        };
        schema
            .tables
            .lookup_cache()
            .insert(key, "42".into(), "1000".into());

        // Binary COPY header, one tuple keyed by bigint 42, terminator.
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
        data.extend(42_i64.to_be_bytes());
        data.extend(3_i32.to_be_bytes());
        data.extend(b"yes");
        data.extend((-1_i16).to_be_bytes());

        let rows = copy.shard(&[CopyData::new(data.as_slice())]).await.unwrap();

        // Header, the tuple sharded by its decoded and translated key,
        // and the terminator, in order.
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].shard(), &Shard::All);
        assert_eq!(rows[1].message().data().len(), 2 + 4 + 8 + 4 + 3);
        assert_eq!(
            rows[1].shard(),
            &shard_value("1000", &DataType::Bigint, 3, &vec![], 0)
        );
        assert_eq!(rows[2].message().data(), (-1_i16).to_be_bytes());
        assert_eq!(rows[2].shard(), &Shard::All);
    }

    pub(super) fn parse(sql: &str) -> Owned<nodes::CopyStmt> {
        let stmt = pg_raw_parse::parse(sql).unwrap();
        match stmt.stmts().next() {
            Some(Node::CopyStmt(copy)) => make::owned(|mem| mem.make_unique(copy)),
            _ => panic!("not a copy"),
        }
    }
}

#[cfg(test)]
mod lookup_result_shard_test {
    use super::*;
    use crate::backend::ShardedTables;
    use crate::config::config;
    use crate::frontend::router::sharding::lookup::LookupTable;
    use pgdog_config::{DataType, LookupResult, SystemCatalogsBehavior};

    use super::test::parse;

    fn shard_mode_copy(query: &str) -> (CopyParser, crate::backend::ShardingSchema) {
        let stmt = parse(query);
        let mut copy = CopyParser::new(&stmt, &Cluster::new_test(&config())).unwrap();

        let table = ShardedTable {
            column: "tenant_id".into(),
            name: Some("sharded_lookup".into()),
            data_type: DataType::Varchar,
            lookup_query: Some("SELECT shard_id FROM tenants WHERE id = $1".into()),
            lookup_result: LookupResult::Shard,
            ..Default::default()
        };
        let schema = ShardingSchema {
            shards: 3,
            tables: ShardedTables::new(
                vec![table.clone()],
                vec![],
                false,
                SystemCatalogsBehavior::default(),
            ),
            ..Default::default()
        };
        copy.sharded_table = Some(table);
        copy.sharded_column = 0;
        copy.sharding_schema = schema.clone();
        (copy, schema)
    }

    fn warm(schema: &crate::backend::ShardingSchema, value: &str, shard: &str) {
        schema.tables.lookup_cache().insert(
            LookupTable {
                schema: None,
                name: Some("sharded_lookup".into()),
                column: "tenant_id".into(),
            },
            value.into(),
            shard.into(),
        );
    }

    /// Shard-mode COPY rows route by the returned shard number.
    #[tokio::test]
    async fn test_copy_lookup_result_shard() {
        let (mut copy, schema) =
            shard_mode_copy("COPY sharded_lookup (tenant_id, value) FROM STDIN CSV");
        warm(&schema, "t_a", "2");
        warm(&schema, "t_b", "0");

        let rows = copy
            .shard(&[CopyData::new("t_a,hello\nt_b,world\n".as_bytes())])
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].shard(), &Shard::Direct(2));
        assert_eq!(rows[1].shard(), &Shard::Direct(0));
    }

    /// Out-of-range and non-numeric shard numbers fail the COPY.
    #[tokio::test]
    async fn test_copy_lookup_result_shard_invalid() {
        let (mut copy, schema) =
            shard_mode_copy("COPY sharded_lookup (tenant_id, value) FROM STDIN CSV");
        warm(&schema, "t_a", "7");
        let result = copy.shard(&[CopyData::new("t_a,hello\n".as_bytes())]).await;
        assert!(result.is_err());

        let (mut copy, schema) =
            shard_mode_copy("COPY sharded_lookup (tenant_id, value) FROM STDIN CSV");
        warm(&schema, "t_a", "not-a-shard");
        let result = copy.shard(&[CopyData::new("t_a,hello\n".as_bytes())]).await;
        assert!(result.is_err());
    }

    /// Binary COPY decodes the key, translates, and routes by the
    /// returned shard number.
    #[tokio::test]
    async fn test_copy_binary_lookup_result_shard() {
        let (mut copy, schema) = shard_mode_copy(
            "COPY sharded_lookup (customer_id, value) FROM STDIN (FORMAT 'binary')",
        );
        // Bigint key for the binary decode path.
        if let Some(table) = copy.sharded_table.as_mut() {
            table.data_type = DataType::Bigint;
            table.column = "customer_id".into();
        }
        schema.tables.lookup_cache().insert(
            LookupTable {
                schema: None,
                name: Some("sharded_lookup".into()),
                column: "customer_id".into(),
            },
            "42".into(),
            "1".into(),
        );

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
        data.extend(42_i64.to_be_bytes());
        data.extend(3_i32.to_be_bytes());
        data.extend(b"yes");
        data.extend((-1_i16).to_be_bytes());

        let rows = copy.shard(&[CopyData::new(data.as_slice())]).await.unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].shard(), &Shard::All);
        assert_eq!(rows[1].shard(), &Shard::Direct(1));
        assert_eq!(rows[2].shard(), &Shard::All);
    }
}
