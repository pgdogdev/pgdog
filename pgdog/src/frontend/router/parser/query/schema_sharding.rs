use crate::frontend::router::sharding::SchemaSharder;

use super::*;

impl QueryParser {
    pub(super) fn check_search_path_for_shard(
        &mut self,
        context: &QueryParserContext<'_>,
    ) -> Result<Option<Shard>, Error> {
        // Shortcut.
        if context.sharding_schema.schemas.is_empty() {
            return Ok(None);
        }

        // Check search_path for schema.
        let search_path = context.router_context.params.get("search_path");
        let mut schema_sharder = SchemaSharder::default();

        match search_path {
            Some(ParameterValue::String(search_path)) => {
                let schema = Schema::from(search_path.as_str());
                schema_sharder.resolve(Some(schema), &context.sharding_schema.schemas);
                if let Some((shard, schema)) = schema_sharder.get() {
                    if let Some(recorder) = self.recorder_mut() {
                        recorder.clear();
                        recorder.record_entry(
                            Some(shard.clone()),
                            format!("matched schema {} in search_path", schema),
                        );
                    }
                    return Ok(Some(shard));
                }
            }

            Some(ParameterValue::Tuple(search_paths)) => {
                for schema in search_paths {
                    let schema = Schema::from(schema.as_str());
                    schema_sharder.resolve(Some(schema), &context.sharding_schema.schemas);
                }

                if let Some((shard, schema)) = schema_sharder.get() {
                    if let Some(recorder) = self.recorder_mut() {
                        recorder.clear();
                        recorder.record_entry(
                            Some(shard.clone()),
                            format!("matched schema {} in search_path", schema),
                        );
                    }
                    return Ok(Some(shard));
                }
            }

            None => (),
        }

        Ok(None)
    }
}
