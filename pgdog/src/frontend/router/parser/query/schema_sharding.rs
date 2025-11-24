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

        match search_path {
            Some(ParameterValue::String(search_path)) => {
                let schema = Schema::from(search_path.as_str());
                if let Some(schema) = context.sharding_schema.schemas.get(Some(schema)) {
                    let shard: Shard = schema.shard().into();
                    if let Some(recorder) = self.recorder_mut() {
                        recorder.record_entry(
                            Some(shard.clone()),
                            format!("matched schema \"{}\" in search_path", schema.name()),
                        );
                    }
                    return Ok(Some(shard));
                }
            }

            Some(ParameterValue::Tuple(search_paths)) => {
                let mut candidates = vec![];

                for (idx, schema) in search_paths.iter().enumerate() {
                    let schema = Schema::from(schema.as_str());
                    if let Some(schema) = context.sharding_schema.schemas.get(Some(schema)) {
                        let shard: Shard = schema.shard().into();
                        let catch_all = schema.is_default();
                        candidates.push((shard, catch_all, idx));
                    }
                }

                // false < true
                // Catch-all schemas go first, more qualified ones go last.
                candidates.sort_by_key(|cand| !cand.1);
                if let Some(candidate) = candidates.pop() {
                    if let Some(schema) = search_paths.get(candidate.2) {
                        if let Some(recorder) = self.recorder_mut() {
                            recorder.record_entry(
                                Some(candidate.0.clone()),
                                format!("matched schema mult \"{}\" in search_path", schema),
                            );
                        }
                    }
                    return Ok(Some(candidate.0));
                }
            }

            None => (),
        }

        Ok(None)
    }
}
