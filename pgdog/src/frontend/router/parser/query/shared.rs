use super::{explain_trace::ExplainRecorder, *};
use std::string::String as StdString;

#[derive(Debug, Clone, Default, Copy, PartialEq)]
pub(super) enum ConvergeAlgorithm {
    // Take the first direct shard we find
    FirstDirect,
    // If All is present, make it cross-shard.
    // If multiple shards are present, make it multi.
    // Else, make it direct.
    #[default]
    AllFirstElseMulti,
}

impl QueryParser {
    /// Converge to a single route given multiple shards.
    pub(super) fn converge(shards: HashSet<Shard>, algorithm: ConvergeAlgorithm) -> Shard {
        let shard = if shards.len() == 1 {
            shards.iter().next().cloned().unwrap()
        } else {
            let mut multi = vec![];
            let mut all = false;
            for shard in &shards {
                match shard {
                    Shard::All => {
                        all = true;
                        break;
                    }
                    Shard::Direct(v) => multi.push(*v),
                    Shard::Multi(m) => multi.extend(m),
                };
            }

            if algorithm == ConvergeAlgorithm::FirstDirect {
                let direct = shards.iter().find(|shard| shard.is_direct());
                if let Some(direct) = direct {
                    return direct.clone();
                }
            }

            if all || shards.is_empty() {
                Shard::All
            } else {
                Shard::Multi(multi)
            }
        };

        shard
    }

    /// Handle WHERRE clause in SELECT, UPDATE an DELETE statements.
    pub(super) fn where_clause(
        sharding_schema: &ShardingSchema,
        where_clause: &WhereClause,
        params: Option<&Bind>,
        recorder: &mut Option<ExplainRecorder>,
    ) -> Result<HashSet<Shard>, Error> {
        let mut shards = HashSet::new();
        // Complexity: O(number of sharded tables * number of columns in the query)
        for table in sharding_schema.tables().tables() {
            let table_name = table.name.as_deref();
            let keys = where_clause.keys(table_name, &table.column);
            for key in keys {
                match key {
                    Key::Constant { value, array } => {
                        if array {
                            shards.insert(Shard::All);
                            record_column(
                                recorder,
                                Some(Shard::All),
                                table_name,
                                &table.column,
                                |col| format!("array value on {} forced broadcast", col),
                            );
                            break;
                        }

                        let ctx = ContextBuilder::new(table)
                            .data(value.as_str())
                            .shards(sharding_schema.shards)
                            .build()?;
                        let shard = ctx.apply()?;
                        record_column(
                            recorder,
                            Some(shard.clone()),
                            table_name,
                            &table.column,
                            |col| format!("matched sharding key {} using constant", col),
                        );
                        shards.insert(shard);
                    }

                    Key::Parameter { pos, array } => {
                        // Don't hash individual values yet.
                        // The odds are high this will go to all shards anyway.
                        if array {
                            shards.insert(Shard::All);
                            record_column(
                                recorder,
                                Some(Shard::All),
                                table_name,
                                &table.column,
                                |col| format!("array parameter for {} forced broadcast", col),
                            );
                            break;
                        } else if let Some(params) = params {
                            if let Some(param) = params.parameter(pos)? {
                                let value = ShardingValue::from_param(&param, table.data_type)?;
                                let ctx = ContextBuilder::new(table)
                                    .value(value)
                                    .shards(sharding_schema.shards)
                                    .build()?;
                                let shard = ctx.apply()?;
                                record_column(
                                    recorder,
                                    Some(shard.clone()),
                                    table_name,
                                    &table.column,
                                    |col| {
                                        format!(
                                            "matched sharding key {} using parameter ${}",
                                            col,
                                            pos + 1
                                        )
                                    },
                                );
                                shards.insert(shard);
                            }
                        }
                    }

                    // Null doesn't help.
                    Key::Null => (),
                }
            }
        }

        Ok(shards)
    }
}

fn format_column(table: Option<&str>, column: &str) -> StdString {
    match table {
        Some(table) if !table.is_empty() => format!("{}.{}", table, column),
        _ => column.to_string(),
    }
}

fn record_column<F>(
    recorder: &mut Option<ExplainRecorder>,
    shard: Option<Shard>,
    table: Option<&str>,
    column: &str,
    message: F,
) where
    F: FnOnce(StdString) -> StdString,
{
    if let Some(recorder) = recorder.as_mut() {
        let column: StdString = format_column(table, column);
        let description: StdString = message(column);
        recorder.record_entry(shard, description);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn single_direct_returns_itself() {
        let shards = HashSet::from([Shard::Direct(5)]);

        let result = QueryParser::converge(shards.clone(), ConvergeAlgorithm::AllFirstElseMulti);
        assert_eq!(result, Shard::Direct(5));

        let result = QueryParser::converge(shards, ConvergeAlgorithm::FirstDirect);
        assert_eq!(result, Shard::Direct(5));
    }

    #[test]
    fn single_all_returns_itself() {
        let shards = HashSet::from([Shard::All]);

        let result = QueryParser::converge(shards.clone(), ConvergeAlgorithm::AllFirstElseMulti);
        assert_eq!(result, Shard::All);

        let result = QueryParser::converge(shards, ConvergeAlgorithm::FirstDirect);
        assert_eq!(result, Shard::All);
    }

    #[test]
    fn single_multi_returns_itself() {
        let shards = HashSet::from([Shard::Multi(vec![1, 2, 3])]);

        let result = QueryParser::converge(shards.clone(), ConvergeAlgorithm::AllFirstElseMulti);
        assert_eq!(result, Shard::Multi(vec![1, 2, 3]));

        let result = QueryParser::converge(shards, ConvergeAlgorithm::FirstDirect);
        assert_eq!(result, Shard::Multi(vec![1, 2, 3]));
    }

    #[test]
    fn multiple_direct_all_first_else_multi_returns_multi() {
        let shards = HashSet::from([Shard::Direct(1), Shard::Direct(2)]);

        let result = QueryParser::converge(shards, ConvergeAlgorithm::AllFirstElseMulti);
        match result {
            Shard::Multi(mut v) => {
                v.sort();
                assert_eq!(v, vec![1, 2]);
            }
            other => panic!("expected Multi, got {:?}", other),
        }
    }

    #[test]
    fn multiple_direct_first_direct_returns_one_direct() {
        let shards = HashSet::from([Shard::Direct(1), Shard::Direct(2)]);

        let result = QueryParser::converge(shards, ConvergeAlgorithm::FirstDirect);
        assert!(
            matches!(result, Shard::Direct(1) | Shard::Direct(2)),
            "expected Direct(1) or Direct(2), got {:?}",
            result
        );
    }

    #[test]
    fn all_present_all_first_else_multi_returns_all() {
        let shards = HashSet::from([Shard::All, Shard::Direct(1)]);

        let result = QueryParser::converge(shards, ConvergeAlgorithm::AllFirstElseMulti);
        assert_eq!(result, Shard::All);
    }

    #[test]
    fn all_present_first_direct_returns_direct() {
        let shards = HashSet::from([Shard::All, Shard::Direct(1)]);

        let result = QueryParser::converge(shards, ConvergeAlgorithm::FirstDirect);
        assert_eq!(result, Shard::Direct(1));
    }

    #[test]
    fn empty_set_returns_all() {
        let shards = HashSet::new();

        let result = QueryParser::converge(shards.clone(), ConvergeAlgorithm::AllFirstElseMulti);
        assert_eq!(result, Shard::All);

        let result = QueryParser::converge(shards, ConvergeAlgorithm::FirstDirect);
        assert_eq!(result, Shard::All);
    }

    #[test]
    fn multi_and_direct_merge_into_multi() {
        let shards = HashSet::from([Shard::Multi(vec![1, 2]), Shard::Direct(3)]);

        let result = QueryParser::converge(shards, ConvergeAlgorithm::AllFirstElseMulti);
        match result {
            Shard::Multi(mut v) => {
                v.sort();
                assert_eq!(v, vec![1, 2, 3]);
            }
            other => panic!("expected Multi, got {:?}", other),
        }
    }

    #[test]
    fn multi_and_direct_first_direct_returns_direct() {
        let shards = HashSet::from([Shard::Multi(vec![1, 2]), Shard::Direct(3)]);

        let result = QueryParser::converge(shards, ConvergeAlgorithm::FirstDirect);
        assert_eq!(result, Shard::Direct(3));
    }

    #[test]
    fn all_with_multi_first_direct_no_direct_returns_all() {
        let shards = HashSet::from([Shard::All, Shard::Multi(vec![1, 2])]);

        let result = QueryParser::converge(shards, ConvergeAlgorithm::FirstDirect);
        assert_eq!(result, Shard::All);
    }
}
