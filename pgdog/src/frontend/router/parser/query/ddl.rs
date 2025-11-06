use super::*;

impl QueryParser {
    /// Handle DDL, e.g. CREATE, DROP, ALTER, etc.
    pub(super) fn ddl(
        &mut self,
        node: &Option<NodeEnum>,
        context: &mut QueryParserContext<'_>,
    ) -> Result<Command, Error> {
        let mut shard = Shard::All;

        match node {
            Some(NodeEnum::CreateStmt(stmt)) => {
                shard = self
                    .schema_shard_from_table(&stmt.relation, context)?
                    .unwrap_or(Shard::All);
            }
            Some(NodeEnum::DropStmt(stmt)) => match stmt.remove_type() {
                ObjectType::ObjectTable | ObjectType::ObjectIndex => {
                    let table = Table::try_from(&stmt.objects).ok();
                    if let Some(table) = table {
                        if let Some(table_schema) = table.schema {
                            if let Some(schema) = context.sharding_schema.schemas.get(table_schema)
                            {
                                shard = Shard::Direct(schema.shard);
                            }
                        }
                    }
                }

                ObjectType::ObjectSchema => {
                    if let Some(Node {
                        node: Some(NodeEnum::String(string)),
                    }) = stmt.objects.first()
                    {
                        if let Some(schema) = context.sharding_schema.schemas.get(&string.sval) {
                            shard = Shard::Direct(schema.shard);
                        }
                    }
                }

                _ => (),
            },
            Some(NodeEnum::CreateSchemaStmt(stmt)) => {
                if let Some(schema) = context.sharding_schema.schemas.get(&stmt.schemaname) {
                    shard = Shard::Direct(schema.shard);
                }
            }

            Some(NodeEnum::RenameStmt(stmt)) => {
                shard = self
                    .schema_shard_from_table(&stmt.relation, context)?
                    .unwrap_or(Shard::All);
            }

            Some(NodeEnum::AlterTableStmt(stmt)) => {
                shard = self
                    .schema_shard_from_table(&stmt.relation, context)?
                    .unwrap_or(Shard::All);
            }

            Some(NodeEnum::VacuumStmt(stmt)) => {
                for rel in &stmt.rels {
                    if let Some(NodeEnum::VacuumRelation(ref stmt)) = rel.node {
                        shard = self
                            .schema_shard_from_table(&stmt.relation, context)?
                            .unwrap_or(Shard::All);
                    }
                }
            }

            Some(NodeEnum::VacuumRelation(stmt)) => {
                shard = self
                    .schema_shard_from_table(&stmt.relation, context)?
                    .unwrap_or(Shard::All);
            }

            // All others are not handled.
            // They are sent to all shards concurrently.
            _ => (),
        };

        Ok(Command::Query(Route::write(shard)))
    }
}
