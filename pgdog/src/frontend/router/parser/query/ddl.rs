use pg_query::parse;

use super::*;

impl QueryParser {
    /// Handle DDL, e.g. CREATE, DROP, ALTER, etc.
    pub(super) fn ddl(
        &mut self,
        node: &Option<NodeEnum>,
        context: &mut QueryParserContext<'_>,
    ) -> Result<Command, Error> {
        let command = Self::shard_ddl(
            node,
            &context.sharding_schema,
            &mut context.shards_calculator,
        )?;

        Ok(command)
    }

    pub(super) fn shard_ddl(
        node: &Option<NodeEnum>,
        schema: &ShardingSchema,
        calculator: &mut ShardsWithPriority,
    ) -> Result<Command, Error> {
        let mut shard = Shard::All;
        let mut schema_changed = false;

        match node {
            Some(NodeEnum::CreateStmt(stmt)) => {
                schema_changed = true;
                shard = Self::shard_ddl_table(&stmt.relation, schema)?.unwrap_or(Shard::All);
            }

            Some(NodeEnum::CreateSeqStmt(stmt)) => {
                shard = Self::shard_ddl_table(&stmt.sequence, schema)?.unwrap_or(Shard::All);
            }

            Some(NodeEnum::DropStmt(stmt)) => match stmt.remove_type() {
                ObjectType::ObjectTable
                | ObjectType::ObjectIndex
                | ObjectType::ObjectView
                | ObjectType::ObjectSequence => {
                    let table = Table::try_from(&stmt.objects).ok();
                    if let Some(table) = table {
                        if let Some(schema) = schema.schemas.get(table.schema()) {
                            shard = schema.shard().into();
                        }
                    }
                    schema_changed = true;
                }

                ObjectType::ObjectSchema => {
                    if let Some(Node {
                        node: Some(NodeEnum::String(string)),
                    }) = stmt.objects.first()
                    {
                        if let Some(schema) = schema.schemas.get(Some(string.sval.as_str().into()))
                        {
                            shard = schema.shard().into();
                        }
                    }
                }

                _ => (),
            },

            Some(NodeEnum::CreateSchemaStmt(stmt)) => {
                if let Some(schema) = schema.schemas.get(Some(stmt.schemaname.as_str().into())) {
                    shard = schema.shard().into();
                }
            }

            Some(NodeEnum::IndexStmt(stmt)) => {
                shard = Self::shard_ddl_table(&stmt.relation, schema)?.unwrap_or(Shard::All);
            }

            Some(NodeEnum::ViewStmt(stmt)) => {
                schema_changed = true;
                shard = Self::shard_ddl_table(&stmt.view, schema)?.unwrap_or(Shard::All);
            }

            Some(NodeEnum::CreateTableAsStmt(stmt)) => {
                schema_changed = true;
                if let Some(into) = &stmt.into {
                    shard = Self::shard_ddl_table(&into.rel, schema)?.unwrap_or(Shard::All);
                }
            }

            Some(NodeEnum::CreateFunctionStmt(stmt)) => {
                let table = Table::try_from(&stmt.funcname).ok();
                if let Some(table) = table {
                    shard = schema
                        .schemas
                        .get(table.schema())
                        .map(|schema| schema.shard().into())
                        .unwrap_or(Shard::All);
                }
            }

            Some(NodeEnum::CreateEnumStmt(stmt)) => {
                let table = Table::try_from(&stmt.type_name).ok();
                if let Some(table) = table {
                    shard = schema
                        .schemas
                        .get(table.schema())
                        .map(|schema| schema.shard().into())
                        .unwrap_or(Shard::All);
                }
            }

            Some(NodeEnum::AlterOwnerStmt(stmt)) => {
                shard = Self::shard_ddl_table(&stmt.relation, schema)?.unwrap_or(Shard::All);
            }

            Some(NodeEnum::RenameStmt(stmt)) => {
                shard = Self::shard_ddl_table(&stmt.relation, schema)?.unwrap_or(Shard::All);
            }

            Some(NodeEnum::AlterTableStmt(stmt)) => {
                schema_changed = true;
                shard = Self::shard_ddl_table(&stmt.relation, schema)?.unwrap_or(Shard::All);
            }

            Some(NodeEnum::AlterSeqStmt(stmt)) => {
                shard = Self::shard_ddl_table(&stmt.sequence, schema)?.unwrap_or(Shard::All);
            }

            Some(NodeEnum::LockStmt(stmt)) => {
                if let Some(node) = stmt.relations.first() {
                    if let Some(NodeEnum::RangeVar(ref table)) = node.node {
                        let table = Table::from(table);
                        shard = schema
                            .schemas
                            .get(table.schema())
                            .map(|schema| schema.shard().into())
                            .unwrap_or(Shard::All);
                    }
                }
            }

            Some(NodeEnum::VacuumStmt(stmt)) => {
                for rel in &stmt.rels {
                    if let Some(NodeEnum::VacuumRelation(ref stmt)) = rel.node {
                        shard =
                            Self::shard_ddl_table(&stmt.relation, schema)?.unwrap_or(Shard::All);
                    }
                }
            }

            Some(NodeEnum::VacuumRelation(stmt)) => {
                shard = Self::shard_ddl_table(&stmt.relation, schema)?.unwrap_or(Shard::All);
            }

            // DO $$ BEGIN ... END
            Some(NodeEnum::DoStmt(stmt)) => {
                if let Some(inner) = stmt.args.first() {
                    if let Some(NodeEnum::DefElem(ref elem)) = inner.node {
                        if let Some(ref arg) = elem.arg {
                            if let Some(NodeEnum::String(ref string)) = arg.node {
                                // Parse each statement individually.
                                // The first DDL statement to return a direct shard will be used.
                                // TODO: handle non-DDL statements in here as well,
                                // need a full recursive call back to QueryParser::query basically, but that requires a refactor.
                                for stmt in string.sval.lines() {
                                    if let Ok(stmt) = parse(stmt) {
                                        if let Some(node) = stmt
                                            .protobuf
                                            .stmts
                                            .first()
                                            .map(|stmt| &stmt.stmt)
                                            .cloned()
                                            .flatten()
                                        {
                                            // Use a fresh calculator for each inner statement
                                            // to avoid pollution from statements that don't match
                                            // any DDL pattern (like BEGIN, END, etc.)
                                            let mut inner_calculator =
                                                ShardsWithPriority::default();
                                            let command = Self::shard_ddl(
                                                &node.node,
                                                schema,
                                                &mut inner_calculator,
                                            )?;
                                            if let Command::Query(query) = command {
                                                if !query.is_cross_shard() {
                                                    shard = query.shard().clone();
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            Some(NodeEnum::TruncateStmt(stmt)) => {
                let mut shards = HashSet::new();
                for relation in &stmt.relations {
                    if let Some(NodeEnum::RangeVar(ref relation)) = relation.node {
                        shards.insert(
                            Self::shard_ddl_table(&Some(relation.clone()), schema)?
                                .unwrap_or(Shard::All),
                        );
                    }
                }

                match shards.len() {
                    0 => (),
                    1 => {
                        shard = shards.iter().next().unwrap().clone();
                    }
                    _ => return Err(Error::CrossShardTruncateSchemaSharding),
                }
            }

            // All others are not handled.
            // They are sent to all shards concurrently.
            _ => (),
        };

        calculator.push(ShardWithPriority::new_table(shard));

        Ok(Command::Query(
            Route::write(calculator.shard())
                .with_schema_changed(schema_changed)
                .with_ddl(true),
        ))
    }

    pub(super) fn shard_ddl_table(
        range_var: &Option<RangeVar>,
        schema: &ShardingSchema,
    ) -> Result<Option<Shard>, Error> {
        let table = range_var.as_ref().map(Table::from);
        if let Some(table) = table {
            if let Some(sharded_schema) = schema.schemas.get(table.schema()) {
                return Ok(Some(sharded_schema.shard().into()));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use pg_query::{parse, NodeEnum};
    use pgdog_config::ShardedSchema;

    use crate::{
        backend::{replication::ShardedSchemas, ShardingSchema},
        frontend::router::{
            parser::{Shard, ShardsWithPriority},
            QueryParser,
        },
    };

    fn test_schema() -> ShardingSchema {
        ShardingSchema {
            shards: 2,
            schemas: ShardedSchemas::new(vec![
                ShardedSchema {
                    name: Some("shard_0".into()),
                    shard: 0,
                    ..Default::default()
                },
                ShardedSchema {
                    name: Some("shard_1".into()),
                    shard: 1,
                    ..Default::default()
                },
            ]),
            ..Default::default()
        }
    }

    fn parse_stmt(query: &str) -> Option<NodeEnum> {
        let root = parse(query)
            .unwrap()
            .protobuf
            .stmts
            .first()
            .unwrap()
            .clone()
            .stmt
            .unwrap()
            .node;
        root
    }

    #[test]
    fn test_create_table_sharded_schema() {
        let root = parse_stmt("CREATE TABLE shard_0.test (id BIGINT)");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(0));
        assert!(command.route().is_schema_changed());
    }

    #[test]
    fn test_create_table_unsharded_schema() {
        let root = parse_stmt("CREATE TABLE unsharded.test (id BIGINT)");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::All);
        assert!(command.route().is_schema_changed());
    }

    #[test]
    fn test_create_table_no_schema() {
        let root = parse_stmt("CREATE TABLE test (id BIGINT)");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::All);
        assert!(command.route().is_schema_changed());
    }

    #[test]
    fn test_create_sequence_sharded() {
        let root = parse_stmt("CREATE SEQUENCE shard_1.test_seq");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(1));
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_create_sequence_unsharded() {
        let root = parse_stmt("CREATE SEQUENCE public.test_seq");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::All);
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_drop_table_sharded() {
        let root = parse_stmt("DROP TABLE shard_0.test");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(0));
        assert!(command.route().is_schema_changed());
    }

    #[test]
    fn test_drop_table_unsharded() {
        let root = parse_stmt("DROP TABLE public.test");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::All);
        assert!(command.route().is_schema_changed());
    }

    #[test]
    fn test_drop_index_sharded() {
        let root = parse_stmt("DROP INDEX shard_1.test_idx");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(1));
        assert!(command.route().is_schema_changed());
    }

    #[test]
    fn test_drop_view_sharded() {
        let root = parse_stmt("DROP VIEW shard_0.test_view");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(0));
        assert!(command.route().is_schema_changed());
    }

    #[test]
    fn test_drop_sequence_sharded() {
        let root = parse_stmt("DROP SEQUENCE shard_1.test_seq");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(1));
        assert!(command.route().is_schema_changed());
    }

    #[test]
    fn test_drop_schema_sharded() {
        let root = parse_stmt("DROP SCHEMA shard_0");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(0));
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_drop_schema_unsharded() {
        let root = parse_stmt("DROP SCHEMA public");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::All);
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_create_schema_sharded() {
        let root = parse_stmt("CREATE SCHEMA shard_0");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(0));
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_create_schema_unsharded() {
        let root = parse_stmt("CREATE SCHEMA new_schema");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::All);
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_create_index_sharded() {
        let root = parse_stmt("CREATE INDEX test_idx ON shard_1.test (id)");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(1));
        assert!(!command.route().is_schema_changed());

        let root = parse_stmt("CREATE UNIQUE INDEX test_idx ON shard_1.test (id)");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(1));
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_do_begin() {
        let root = parse_stmt(
            r#"DO $$ BEGIN
         ALTER TABLE "shard_1"."foo" ADD CONSTRAINT "foo_id_foo2_id_fk" FOREIGN KEY ("id") REFERENCES "shard_1"."foo2"("id") ON DELETE cascade ON UPDATE cascade;
        EXCEPTION
         WHEN duplicate_object THEN null;
        END $$;"#,
        );
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(1));
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_create_index_unsharded() {
        let root = parse_stmt("CREATE INDEX test_idx ON public.test (id)");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::All);
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_create_view_sharded() {
        let root = parse_stmt("CREATE VIEW shard_0.test_view AS SELECT 1");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(0));
        assert!(command.route().is_schema_changed());
    }

    #[test]
    fn test_create_view_unsharded() {
        let root = parse_stmt("CREATE VIEW public.test_view AS SELECT 1");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::All);
        assert!(command.route().is_schema_changed());
    }

    #[test]
    fn test_create_table_as_sharded() {
        let root = parse_stmt("CREATE TABLE shard_1.new_table AS SELECT * FROM other");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(1));
        assert!(command.route().is_schema_changed());
    }

    #[test]
    fn test_create_table_as_unsharded() {
        let root = parse_stmt("CREATE TABLE public.new_table AS SELECT * FROM other");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::All);
        assert!(command.route().is_schema_changed());
    }

    #[test]
    fn test_lock_table() {
        let root = parse_stmt(r#"LOCK TABLE "shard_1"."__migrations_table""#);
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(1));
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_create_function_sharded() {
        let root = parse_stmt(
            "CREATE FUNCTION shard_0.test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql",
        );
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(0));
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_create_function_unsharded() {
        let root = parse_stmt(
            "CREATE FUNCTION public.test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql",
        );
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::All);
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_create_enum_sharded() {
        let root = parse_stmt("CREATE TYPE shard_1.mood AS ENUM ('sad', 'ok', 'happy')");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(1));
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_create_enum_unsharded() {
        let root = parse_stmt("CREATE TYPE public.mood AS ENUM ('sad', 'ok', 'happy')");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::All);
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_alter_owner_sharded() {
        // Note: ALTER TABLE ... OWNER TO is parsed as AlterTableStmt, not AlterOwnerStmt
        let root = parse_stmt("ALTER TABLE shard_0.test OWNER TO new_owner");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(0));
        assert!(command.route().is_schema_changed());
    }

    #[test]
    fn test_alter_owner_unsharded() {
        // Note: ALTER TABLE ... OWNER TO is parsed as AlterTableStmt, not AlterOwnerStmt
        let root = parse_stmt("ALTER TABLE public.test OWNER TO new_owner");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::All);
        assert!(command.route().is_schema_changed());
    }

    #[test]
    fn test_rename_table_sharded() {
        let root = parse_stmt("ALTER TABLE shard_1.test RENAME TO new_test");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(1));
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_rename_table_unsharded() {
        let root = parse_stmt("ALTER TABLE public.test RENAME TO new_test");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::All);
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_alter_table_sharded() {
        let root = parse_stmt("ALTER TABLE shard_0.test ADD COLUMN new_col INT");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(0));
        assert!(command.route().is_schema_changed());
    }

    #[test]
    fn test_alter_table_unsharded() {
        let root = parse_stmt("ALTER TABLE public.test ADD COLUMN new_col INT");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::All);
        assert!(command.route().is_schema_changed());
    }

    #[test]
    fn test_alter_sequence_sharded() {
        let root = parse_stmt("ALTER SEQUENCE shard_1.test_seq RESTART WITH 100");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(1));
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_alter_sequence_unsharded() {
        let root = parse_stmt("ALTER SEQUENCE public.test_seq RESTART WITH 100");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::All);
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_vacuum_sharded() {
        let root = parse_stmt("VACUUM shard_0.test");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(0));
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_vacuum_unsharded() {
        let root = parse_stmt("VACUUM public.test");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::All);
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_vacuum_no_table() {
        let root = parse_stmt("VACUUM");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::All);
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_truncate_single_table_sharded() {
        let root = parse_stmt("TRUNCATE shard_0.test");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(0));
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_truncate_single_table_unsharded() {
        let root = parse_stmt("TRUNCATE public.test");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::All);
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_truncate_multiple_tables_same_shard() {
        let root = parse_stmt("TRUNCATE shard_0.test1, shard_0.test2");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::Direct(0));
        assert!(!command.route().is_schema_changed());
    }

    #[test]
    fn test_truncate_cross_shard_error() {
        let root = parse_stmt("TRUNCATE shard_0.test1, shard_1.test2");
        let mut calculator = ShardsWithPriority::default();
        let result = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator);
        assert!(result.is_err());
    }

    #[test]
    fn test_unhandled_ddl_defaults_to_all() {
        let root = parse_stmt("COMMENT ON TABLE public.test IS 'test comment'");
        let mut calculator = ShardsWithPriority::default();
        let command = QueryParser::shard_ddl(&root, &test_schema(), &mut calculator).unwrap();
        assert_eq!(command.route().shard(), &Shard::All);
        assert!(!command.route().is_schema_changed());
    }
}
