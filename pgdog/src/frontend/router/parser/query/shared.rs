use super::*;

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
    pub(super) fn converge(shards: &HashSet<Shard>, algorithm: ConvergeAlgorithm) -> Shard {
        let shard = if shards.len() == 1 {
            shards.iter().next().cloned().unwrap()
        } else {
            let mut multi = HashSet::new();
            let mut all = false;
            for shard in shards {
                match shard {
                    Shard::All => {
                        all = true;
                        break;
                    }
                    Shard::Direct(v) => {
                        multi.insert(*v);
                    }
                    Shard::Multi(m) => multi.extend(m.iter()),
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
                Shard::Multi(multi.into_iter().collect())
            }
        };

        shard
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn single_direct_returns_itself() {
        let shards = HashSet::from([Shard::Direct(5)]);

        let result = QueryParser::converge(&shards, ConvergeAlgorithm::AllFirstElseMulti);
        assert_eq!(result, Shard::Direct(5));

        let result = QueryParser::converge(&shards, ConvergeAlgorithm::FirstDirect);
        assert_eq!(result, Shard::Direct(5));
    }

    #[test]
    fn single_all_returns_itself() {
        let shards = HashSet::from([Shard::All]);

        let result = QueryParser::converge(&shards, ConvergeAlgorithm::AllFirstElseMulti);
        assert_eq!(result, Shard::All);

        let result = QueryParser::converge(&shards, ConvergeAlgorithm::FirstDirect);
        assert_eq!(result, Shard::All);
    }

    #[test]
    fn single_multi_returns_itself() {
        let shards = HashSet::from([Shard::Multi(vec![1, 2, 3])]);

        let result = QueryParser::converge(&shards, ConvergeAlgorithm::AllFirstElseMulti);
        assert_eq!(result, Shard::Multi(vec![1, 2, 3]));

        let result = QueryParser::converge(&shards, ConvergeAlgorithm::FirstDirect);
        assert_eq!(result, Shard::Multi(vec![1, 2, 3]));
    }

    #[test]
    fn multiple_direct_all_first_else_multi_returns_multi() {
        let shards = HashSet::from([Shard::Direct(1), Shard::Direct(2)]);

        let result = QueryParser::converge(&shards, ConvergeAlgorithm::AllFirstElseMulti);
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

        let result = QueryParser::converge(&shards, ConvergeAlgorithm::FirstDirect);
        assert!(
            matches!(result, Shard::Direct(1) | Shard::Direct(2)),
            "expected Direct(1) or Direct(2), got {:?}",
            result
        );
    }

    #[test]
    fn all_present_all_first_else_multi_returns_all() {
        let shards = HashSet::from([Shard::All, Shard::Direct(1)]);

        let result = QueryParser::converge(&shards, ConvergeAlgorithm::AllFirstElseMulti);
        assert_eq!(result, Shard::All);
    }

    #[test]
    fn all_present_first_direct_returns_direct() {
        let shards = HashSet::from([Shard::All, Shard::Direct(1)]);

        let result = QueryParser::converge(&shards, ConvergeAlgorithm::FirstDirect);
        assert_eq!(result, Shard::Direct(1));
    }

    #[test]
    fn empty_set_returns_all() {
        let shards = HashSet::new();

        let result = QueryParser::converge(&shards, ConvergeAlgorithm::AllFirstElseMulti);
        assert_eq!(result, Shard::All);

        let result = QueryParser::converge(&shards, ConvergeAlgorithm::FirstDirect);
        assert_eq!(result, Shard::All);
    }

    #[test]
    fn multi_and_direct_merge_into_multi() {
        let shards = HashSet::from([Shard::Multi(vec![1, 2]), Shard::Direct(3)]);

        let result = QueryParser::converge(&shards, ConvergeAlgorithm::AllFirstElseMulti);
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

        let result = QueryParser::converge(&shards, ConvergeAlgorithm::FirstDirect);
        assert_eq!(result, Shard::Direct(3));
    }

    #[test]
    fn all_with_multi_first_direct_no_direct_returns_all() {
        let shards = HashSet::from([Shard::All, Shard::Multi(vec![1, 2])]);

        let result = QueryParser::converge(&shards, ConvergeAlgorithm::FirstDirect);
        assert_eq!(result, Shard::All);
    }
}
