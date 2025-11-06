use super::*;

impl QueryParser {
    /// Get schema shard, if configured.
    pub(super) fn schema_shard_from_table(
        &self,
        range_var: &Option<RangeVar>,
        context: &QueryParserContext<'_>,
    ) -> Result<Option<Shard>, Error> {
        let table = range_var.as_ref().map(Table::from);
        if let Some(table) = table {
            if let Some(schema) = table.schema {
                if let Some(sharded_schema) = context.sharding_schema.schemas.get(schema) {
                    return Ok(Some(Shard::Direct(sharded_schema.shard)));
                }
            }
        }

        Ok(None)
    }
}
