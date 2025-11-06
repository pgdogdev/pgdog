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
                        if let Some(schema) = context.sharding_schema.schemas.get(table.schema()) {
                            shard = schema.shard().into();
                        }
                    }
                }

                ObjectType::ObjectSchema => {
                    if let Some(Node {
                        node: Some(NodeEnum::String(string)),
                    }) = stmt.objects.first()
                    {
                        if let Some(schema) = context
                            .sharding_schema
                            .schemas
                            .get(Some(string.sval.as_str().into()))
                        {
                            shard = schema.shard().into();
                        }
                    }
                }

                _ => (),
            },
            Some(NodeEnum::CreateSchemaStmt(stmt)) => {
                if let Some(schema) = context
                    .sharding_schema
                    .schemas
                    .get(Some(stmt.schemaname.as_str().into()))
                {
                    shard = schema.shard().into();
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

            Some(NodeEnum::TruncateStmt(stmt)) => {
                let mut shards = HashSet::new();
                for relation in &stmt.relations {
                    if let Some(NodeEnum::RangeVar(ref relation)) = relation.node {
                        shards.insert(
                            self.schema_shard_from_table(&Some(relation.clone()), context)?
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

        Ok(Command::Query(Route::write(shard)))
    }
}
