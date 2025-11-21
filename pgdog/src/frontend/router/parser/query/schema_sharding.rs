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

        // Quick inline function to shard query
        // based on schema in search_path.
        fn shard_from_search_path(
            search_path: &str,
            context: &QueryParserContext<'_>,
            query_parser: &mut QueryParser,
        ) -> Option<Shard> {
            let mut result: Option<Shard> = None;

            let schema = Schema::from(search_path);

            if let Some(schema) = context.sharding_schema.schemas.get(Some(schema)) {
                let shard: Shard = schema.shard().into();

                if let Some(recorder) = query_parser.recorder_mut() {
                    // This will override all other decisions.
                    recorder.clear();
                    recorder.record_entry(
                        Some(shard.clone()),
                        format!("matched schema {} in search_path", schema.name()),
                    );
                }

                result = Some(shard);
            }

            result
        }

        match search_path {
            Some(ParameterValue::String(search_path)) => {
                return Ok(shard_from_search_path(search_path, context, self));
            }

            Some(ParameterValue::Tuple(search_paths)) => {
                for schema in search_paths {
                    if let Some(shard) = shard_from_search_path(schema, context, self) {
                        return Ok(Some(shard));
                    }
                }
            }

            None => (),
        }

        Ok(None)
    }
}
