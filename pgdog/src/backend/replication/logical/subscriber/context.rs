use lazy_static::lazy_static;

use super::super::Error;
use crate::{
    backend::Cluster,
    frontend::{
        client::Sticky,
        router::parser::{AstContext, Cache, Shard},
        BufferedQuery, ClientRequest, Command, PreparedStatements, Router, RouterContext,
    },
    net::{replication::TupleData, Bind, Parameters, Parse},
};

#[derive(Debug)]
pub struct StreamContext<'a> {
    request: ClientRequest,
    cluster: &'a Cluster,
    bind: Bind,
    parse: Parse,
}

impl<'a> StreamContext<'a> {
    /// Construct new stream context.
    pub fn new(cluster: &'a Cluster, tuple: &TupleData, stmt: &Parse) -> Self {
        let bind = tuple.to_bind(stmt.name());
        let parse = stmt.clone();
        let request = ClientRequest::from(vec![parse.clone().into(), bind.clone().into()]);
        Self {
            request,
            cluster,
            bind,
            parse,
        }
    }

    pub fn shard(&'a mut self) -> Result<Shard, Error> {
        let router_context = self.router_context()?;
        let mut router = Router::new();
        let route = router.query(router_context)?;

        if let Command::Query(route) = route {
            Ok(route.shard().clone())
        } else {
            Err(Error::IncorrectCommand)
        }
    }

    /// Get Bind message.
    pub fn bind(&self) -> &Bind {
        &self.bind
    }

    /// Construct router context.
    pub fn router_context(&'a mut self) -> Result<RouterContext<'a>, Error> {
        lazy_static! {
            static ref PARAMS: Parameters = Parameters::default();
        }

        let ast_context = AstContext::from_cluster(self.cluster, &PARAMS);

        let ast = Cache::get().query(
            &BufferedQuery::Prepared(self.parse.clone()),
            &ast_context,
            &mut PreparedStatements::default(),
        )?;
        self.request.ast = Some(ast);

        Ok(RouterContext::new(
            &self.request,
            self.cluster,
            &PARAMS,
            None,
            Sticky::new(),
        )?)
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;

    use crate::{
        backend::replication::logical::publisher::{
            NonIdentityColumnsPresence, PublicationTable, PublicationTableColumn, ReplicaIdentity,
            Table,
        },
        config::config,
        net::replication::logical::tuple_data::{
            text_col, toasted_col, Column, Identifier, TupleData,
        },
    };
    use pgdog_config::QueryParserEngine;
    use pgdog_postgres_types::{Format, Oid};

    use super::*;

    fn make_table(columns: Vec<(&str, bool)>) -> Table {
        Table {
            publication: "test".to_string(),
            table: PublicationTable {
                schema: "public".to_string(),
                name: "test_table".to_string(),
                attributes: "".to_string(),
                parent_schema: "".to_string(),
                parent_name: "".to_string(),
            },
            identity: ReplicaIdentity {
                oid: Oid(1),
                identity: "".to_string(),
                kind: "".to_string(),
            },
            columns: columns
                .into_iter()
                .map(|(name, identity)| PublicationTableColumn {
                    oid: 1,
                    name: name.to_string(),
                    type_oid: Oid(23),
                    identity,
                })
                .collect(),
            lsn: Default::default(),
            query_parser_engine: QueryParserEngine::default(),
        }
    }

    #[test]
    fn test_stream_context() {
        let cluster = Cluster::new_test(&config());
        let tuple = TupleData {
            columns: vec![Column {
                identifier: Identifier::Format(Format::Text),
                len: 1,
                data: Bytes::from("1"),
            }],
        };
        let parse = Parse::new_anonymous("INSERT INTO sharded (customer_id) VALUES ($1)");

        let shard = StreamContext::new(&cluster, &tuple, &parse)
            .shard()
            .unwrap();
        assert!(matches!(shard, Shard::Direct(_)));
    }

    // Verify that $N in the generated SQL matches the bind slot to_bind() places
    // the corresponding value into, for each DML operation and tuple shape.

    #[test]
    fn test_to_bind_delete_trailing_identity() {
        // Columns: name | value | id(pk). DELETE bind = key_non_null(): identity only, reindexed to $1.
        let table = make_table(vec![("name", false), ("value", false), ("id", true)]);

        let key = TupleData {
            // key_non_null() output: [id_val]
            columns: vec![Column {
                identifier: Identifier::Format(Format::Text),
                len: 2,
                data: Bytes::from("99"),
            }],
        };
        let bind = key.to_bind("__pgdog_1");

        assert_eq!(bind.parameter(0).unwrap().unwrap().text(), Some("99")); // id → $1
        assert_eq!(
            table.delete(),
            r#"DELETE FROM "public"."test_table" WHERE "id" = $1"#
        );
    }

    #[test]
    fn test_to_bind_delete_composite_key() {
        // Columns: id(pk) | name | version(pk). key_non_null() strips name; id+version → $1, $2.
        let table = make_table(vec![("id", true), ("name", false), ("version", true)]);

        let key = TupleData {
            // key_non_null() output: [id_val, version_val]
            columns: vec![
                Column {
                    identifier: Identifier::Format(Format::Text),
                    len: 1,
                    data: Bytes::from("5"),
                },
                Column {
                    identifier: Identifier::Format(Format::Text),
                    len: 1,
                    data: Bytes::from("3"),
                },
            ],
        };
        let bind = key.to_bind("__pgdog_1");

        assert_eq!(bind.parameter(0).unwrap().unwrap().text(), Some("5")); // id → $1
        assert_eq!(bind.parameter(1).unwrap().unwrap().text(), Some("3")); // version → $2
        assert_eq!(
            table.delete(),
            r#"DELETE FROM "public"."test_table" WHERE "id" = $1 AND "version" = $2"#,
        );
    }

    #[test]
    fn test_to_bind_insert() {
        // Full row in table order; all_columns().placeholders() → $N = column N.
        let table = make_table(vec![("name", false), ("value", false), ("id", true)]);
        let tuple = TupleData {
            columns: vec![
                Column {
                    identifier: Identifier::Format(Format::Text),
                    len: 5,
                    data: Bytes::from("alice"),
                },
                Column {
                    identifier: Identifier::Format(Format::Text),
                    len: 2,
                    data: Bytes::from("42"),
                },
                Column {
                    identifier: Identifier::Format(Format::Text),
                    len: 2,
                    data: Bytes::from("99"),
                },
            ],
        };
        let bind = tuple.to_bind("__pgdog_1");

        assert_eq!(bind.parameter(0).unwrap().unwrap().text(), Some("alice")); // name → $1
        assert_eq!(bind.parameter(1).unwrap().unwrap().text(), Some("42")); // value → $2
        assert_eq!(bind.parameter(2).unwrap().unwrap().text(), Some("99")); // id → $3
        assert_eq!(
            table.insert(),
            r#"INSERT INTO "public"."test_table" ("name", "value", "id") VALUES ($1, $2, $3)"#,
        );
    }

    #[test]
    fn test_to_bind_upsert() {
        // Same bind as insert; DO UPDATE SET reuses the same $N — no reindex after VALUES.
        let table = make_table(vec![("name", false), ("value", false), ("id", true)]);
        let tuple = TupleData {
            columns: vec![
                Column {
                    identifier: Identifier::Format(Format::Text),
                    len: 5,
                    data: Bytes::from("alice"),
                },
                Column {
                    identifier: Identifier::Format(Format::Text),
                    len: 2,
                    data: Bytes::from("42"),
                },
                Column {
                    identifier: Identifier::Format(Format::Text),
                    len: 2,
                    data: Bytes::from("99"),
                },
            ],
        };
        let bind = tuple.to_bind("__pgdog_1");

        assert_eq!(bind.parameter(0).unwrap().unwrap().text(), Some("alice")); // name → $1
        assert_eq!(bind.parameter(1).unwrap().unwrap().text(), Some("42")); // value → $2
        assert_eq!(bind.parameter(2).unwrap().unwrap().text(), Some("99")); // id → $3
        assert_eq!(
            table.upsert(),
            r#"INSERT INTO "public"."test_table" ("name", "value", "id") VALUES ($1, $2, $3) ON CONFLICT ("id") DO UPDATE SET "name" = $1, "value" = $2"#,
        );
    }

    #[test]
    fn test_to_bind_update() {
        // Full new-row tuple; original positions preserved for both SET and WHERE.
        let table = make_table(vec![("name", false), ("id", true)]);
        let tuple = TupleData {
            columns: vec![
                Column {
                    identifier: Identifier::Format(Format::Text),
                    len: 5,
                    data: Bytes::from("alice"),
                },
                Column {
                    identifier: Identifier::Format(Format::Text),
                    len: 2,
                    data: Bytes::from("99"),
                },
            ],
        };
        let bind = tuple.to_bind("__pgdog_1");

        assert_eq!(bind.parameter(0).unwrap().unwrap().text(), Some("alice")); // name → $1
        assert_eq!(bind.parameter(1).unwrap().unwrap().text(), Some("99")); // id → $2
        assert_eq!(
            table.update(),
            r#"UPDATE "public"."test_table" SET "name" = $1 WHERE "id" = $2"#,
        );
    }

    #[test]
    fn test_to_bind_update_partial() {
        // Columns: id(pk) | a | b | c. b is toasted — dropped from both bind and query.
        let table = make_table(vec![("id", true), ("a", false), ("b", false), ("c", false)]);
        let new_tuple = TupleData {
            columns: vec![text_col("7"), text_col("aa"), toasted_col(), text_col("cc")],
        };
        let present = NonIdentityColumnsPresence::from_tuple(&new_tuple, &table).unwrap();

        let partial = TupleData {
            // mirrors partial_new(): filter toasted → [id, a, c]
            columns: new_tuple
                .columns
                .iter()
                .filter(|c| c.identifier != Identifier::Toasted)
                .cloned()
                .collect(),
        };
        let bind = partial.to_bind("__pgdog_1");

        assert_eq!(bind.parameter(0).unwrap().unwrap().text(), Some("7")); // id → $1
        assert_eq!(bind.parameter(1).unwrap().unwrap().text(), Some("aa")); // a → $2
        assert_eq!(bind.parameter(2).unwrap().unwrap().text(), Some("cc")); // c → $3
        assert_eq!(
            table.update_partial(&present),
            r#"UPDATE "public"."test_table" SET "a" = $2, "c" = $3 WHERE "id" = $1"#,
        );
    }
}
