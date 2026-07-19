//! Config-time validation for sharded table mappings.

use std::cmp::Ordering;

use derive_more::Display;
use pgdog_config::{
    DataType, FlexibleType, FlexibleTypeRef, ShardedMappingConfig, ShardedMappingRange,
};

use crate::frontend::router::sharding::mapping::compare_flexible_type;

/// A single validation problem detected in a sharded table mapping configuration.
#[derive(Debug, Display)]
pub enum ValidationError {
    /// A shard number in a mapping entry exceeds the available shard count.
    #[display("shard {shard} is out of range (num_shards={num_shards})")]
    ShardOutOfRange { shard: usize, num_shards: usize },

    /// A range entry has neither `start` nor `end` defined.
    #[display("range for shard {shard} has neither start nor end defined")]
    RangeNoBounds { shard: usize },

    /// A range entry's `start` bound is greater than its `end` bound.
    #[display("range for shard {shard} is inverted: start {start:?} > end {end:?}")]
    RangeInverted {
        shard: usize,
        start: FlexibleType,
        end: FlexibleType,
    },

    /// Two or more range entries overlap.
    #[display("ranges: {range1:?} & {range2:?} overlap")]
    RangesOverlap {
        range1: ShardedMappingRange,
        range2: ShardedMappingRange,
    },

    /// A value in a list entry or range bound does not match the column's `data_type`.
    #[display("value {value:?} is incompatible with data_type {data_type:?}")]
    IncompatibleType {
        value: FlexibleType,
        data_type: DataType,
    },
}

/// Collect all validation errors for a mapping configuration.
///
/// Errors are ordered by entry position: all problems with entry N are reported
/// before any problem with entry N+1. Range overlap is checked last as it
/// requires the full slice.
pub fn validate(
    configs: &[ShardedMappingConfig],
    data_type: DataType,
    num_shards: usize,
) -> Vec<ValidationError> {
    let mut errors = vec![];
    for config in configs {
        errors.extend(check_shard_range(config, num_shards));
        errors.extend(check_range_bounds(config));
        errors.extend(check_type_compatibility(config, data_type));
    }
    errors.extend(check_range_overlap(configs));
    errors
}

/// Check that the shard number in `config` is within `[0, num_shards)`.
pub fn check_shard_range(
    config: &ShardedMappingConfig,
    num_shards: usize,
) -> Option<ValidationError> {
    let shard = match config {
        ShardedMappingConfig::Default { shard } => *shard,
        ShardedMappingConfig::List(l) => l.shard,
        ShardedMappingConfig::Range(r) => r.shard,
    };
    (shard >= num_shards).then_some(ValidationError::ShardOutOfRange { shard, num_shards })
}

/// Check that `config`, if it is a range entry, has well-formed bounds: at least
/// one of `start`/`end` is defined, and `start <= end` when both are present.
pub fn check_range_bounds(config: &ShardedMappingConfig) -> Option<ValidationError> {
    let ShardedMappingConfig::Range(r) = config else {
        return None;
    };
    match (r.start.as_ref(), r.end.as_ref()) {
        (None, None) => Some(ValidationError::RangeNoBounds { shard: r.shard }),
        (Some(start), Some(end))
            if compare_flexible_type(&FlexibleTypeRef::from(start), end)
                == Some(Ordering::Greater) =>
        {
            Some(ValidationError::RangeInverted {
                shard: r.shard,
                start: start.clone(),
                end: end.clone(),
            })
        }
        _ => None,
    }
}

/// Check that all values in `config` (list values or range bounds) match `data_type`.
pub fn check_type_compatibility(
    config: &ShardedMappingConfig,
    data_type: DataType,
) -> Vec<ValidationError> {
    let values: Vec<&FlexibleType> = match config {
        ShardedMappingConfig::List(list) => list.values.iter().collect(),
        ShardedMappingConfig::Range(range) => [range.start.as_ref(), range.end.as_ref()]
            .into_iter()
            .flatten()
            .collect(),
        ShardedMappingConfig::Default { .. } => return vec![],
    };
    values
        .into_iter()
        .filter(|v| !type_compatible(v, data_type))
        .map(|v| ValidationError::IncompatibleType {
            value: v.clone(),
            data_type,
        })
        .collect()
}

/// Check that no two range entries in `configs` overlap.
///
/// Compares every pair of ranges as half-open intervals `[start, end)`, with an
/// absent bound treated as -inf (`start`) or +inf (`end`). Returns
/// [`ValidationError::RangesOverlap`] naming the first conflicting pair.
pub fn check_range_overlap(configs: &[ShardedMappingConfig]) -> Vec<ValidationError> {
    let ranges: Vec<_> = configs
        .iter()
        .filter_map(|c| match c {
            ShardedMappingConfig::Range(r) => Some(r),
            _ => None,
        })
        .collect();

    let mut overlaps = Vec::new();

    for (i, &a) in ranges.iter().enumerate() {
        for &b in &ranges[i + 1..] {
            if ranges_overlap(a, b) {
                overlaps.push(ValidationError::RangesOverlap {
                    range1: a.clone(),
                    range2: b.clone(),
                });
            }
        }
    }

    overlaps
}

/// Whether two half-open ranges `[start, end)` overlap, treating an absent
/// bound as -inf (`start`) or +inf (`end`).
fn ranges_overlap(a: &ShardedMappingRange, b: &ShardedMappingRange) -> bool {
    start_before_end(&a.start, &b.end) && start_before_end(&b.start, &a.end)
}

/// Whether `start < end`, treating `None` as -inf (`start`) / +inf (`end`).
fn start_before_end(start: &Option<FlexibleType>, end: &Option<FlexibleType>) -> bool {
    match (start, end) {
        (Some(s), Some(e)) => {
            compare_flexible_type(&FlexibleTypeRef::from(s), e) == Some(Ordering::Less)
        }
        _ => true,
    }
}

/// Returns `true` if `value`'s variant matches the expected `data_type`.
fn type_compatible(value: &FlexibleType, data_type: DataType) -> bool {
    matches!(
        (value, data_type),
        (FlexibleType::Integer(_), DataType::Bigint)
            | (FlexibleType::Uuid(_), DataType::Uuid)
            | (FlexibleType::String(_), DataType::Varchar)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgdog_config::{FlexibleType, ShardedMappingList, ShardedMappingRange};
    use std::assert_matches;

    fn range(shard: usize, start: Option<i64>, end: Option<i64>) -> ShardedMappingConfig {
        ShardedMappingConfig::Range(ShardedMappingRange {
            shard,
            start: start.map(FlexibleType::Integer),
            end: end.map(FlexibleType::Integer),
        })
    }

    fn list(shard: usize, values: Vec<FlexibleType>) -> ShardedMappingConfig {
        ShardedMappingConfig::List(ShardedMappingList { shard, values })
    }

    fn default_shard(shard: usize) -> ShardedMappingConfig {
        ShardedMappingConfig::Default { shard }
    }

    mod check_shard_range {
        use super::*;

        #[test]
        fn ok() {
            assert!(check_shard_range(&range(0, Some(0), Some(100)), 2).is_none());
            assert!(check_shard_range(&range(1, Some(100), Some(200)), 2).is_none());
            assert!(check_shard_range(&list(0, vec![FlexibleType::Integer(1)]), 2).is_none());
            assert!(check_shard_range(&list(1, vec![FlexibleType::Integer(2)]), 2).is_none());
            assert!(check_shard_range(&default_shard(0), 2).is_none());
            assert!(check_shard_range(&default_shard(1), 2).is_none());
        }

        #[test]
        fn out_of_bounds() {
            // shard == num_shards is out of range (0-indexed); all three variants
            assert_matches!(
                check_shard_range(&range(5, Some(0), Some(100)), 3),
                Some(ValidationError::ShardOutOfRange {
                    shard: 5,
                    num_shards: 3
                })
            );
            assert_matches!(
                check_shard_range(&list(10, vec![FlexibleType::Integer(1)]), 3),
                Some(ValidationError::ShardOutOfRange {
                    shard: 10,
                    num_shards: 3
                })
            );
            assert_matches!(
                check_shard_range(&default_shard(3), 3),
                Some(ValidationError::ShardOutOfRange {
                    shard: 3,
                    num_shards: 3
                })
            );
        }
    }

    mod check_range_bounds {
        use super::*;

        #[test]
        fn ok() {
            assert!(check_range_bounds(&range(0, Some(0), Some(100))).is_none());
            assert!(check_range_bounds(&range(0, Some(0), None)).is_none());
            assert!(check_range_bounds(&range(0, None, Some(100))).is_none());
        }

        #[test]
        fn both_none() {
            assert_matches!(
                check_range_bounds(&range(0, None, None)),
                Some(ValidationError::RangeNoBounds { shard: 0 })
            );
        }

        #[test]
        fn inverted() {
            assert_matches!(
                check_range_bounds(&range(0, Some(200), Some(100))),
                Some(ValidationError::RangeInverted { shard: 0, .. })
            );
            // start == end is a degenerate empty range, but not inverted.
            assert!(check_range_bounds(&range(0, Some(100), Some(100))).is_none());
        }

        #[test]
        fn list_and_default_ignored() {
            assert!(check_range_bounds(&list(0, vec![])).is_none());
            assert!(check_range_bounds(&default_shard(1)).is_none());
        }
    }

    mod check_type_compatibility {
        use super::*;

        #[test]
        fn ok() {
            let uuid = uuid::Uuid::nil();
            assert!(
                check_type_compatibility(
                    &list(0, vec![FlexibleType::Integer(42)]),
                    DataType::Bigint
                )
                .is_empty()
            );
            assert!(
                check_type_compatibility(&range(0, Some(0), Some(100)), DataType::Bigint)
                    .is_empty()
            );
            assert!(
                check_type_compatibility(&list(0, vec![FlexibleType::Uuid(uuid)]), DataType::Uuid)
                    .is_empty()
            );
            assert!(
                check_type_compatibility(
                    &list(0, vec![FlexibleType::String("foo".into())]),
                    DataType::Varchar
                )
                .is_empty()
            );
        }

        #[test]
        fn list_mismatch() {
            let errors = check_type_compatibility(
                &list(0, vec![FlexibleType::String("foo".into())]),
                DataType::Bigint,
            );
            assert_eq!(errors.len(), 1);
            assert_matches!(errors[0], ValidationError::IncompatibleType { .. });
        }

        #[test]
        fn range_both_bounds_mismatch() {
            // Integer bounds but data_type = Uuid → both start and end are wrong.
            let errors = check_type_compatibility(&range(0, Some(0), Some(100)), DataType::Uuid);
            assert_eq!(errors.len(), 2);
        }

        #[test]
        fn range_one_bound_mismatch() {
            let config = ShardedMappingConfig::Range(ShardedMappingRange {
                shard: 0,
                start: None,
                end: Some(FlexibleType::Integer(100)),
            });
            let errors = check_type_compatibility(&config, DataType::Varchar);
            assert_eq!(errors.len(), 1);
        }

        #[test]
        fn default_ignored() {
            assert!(check_type_compatibility(&default_shard(0), DataType::Bigint).is_empty());
        }
    }

    mod check_range_overlap {
        use super::*;

        #[test]
        fn none() {
            assert!(check_range_overlap(&[range(0, Some(0), Some(100))]).is_empty());
            assert!(
                check_range_overlap(&[
                    range(0, Some(0), Some(100)),
                    range(1, Some(100), Some(200))
                ])
                .is_empty()
            );
        }

        #[test]
        fn detected() {
            let configs = vec![range(0, Some(0), Some(150)), range(1, Some(100), Some(200))];
            assert_matches!(
                check_range_overlap(&configs).as_slice(),
                [ValidationError::RangesOverlap { .. }]
            );
        }

        #[test]
        fn unbounded_below_overlap() {
            // `(-inf, 50)` and `(-inf, 100)` overlap on `(-inf, 50)` yet share no
            // explicit bound that falls strictly inside the other.
            let configs = vec![range(0, None, Some(50)), range(1, None, Some(100))];
            assert_matches!(
                check_range_overlap(&configs).as_slice(),
                [ValidationError::RangesOverlap { .. }]
            );
        }

        #[test]
        fn unbounded_above_overlap() {
            let configs = vec![range(0, Some(0), None), range(1, Some(100), None)];
            assert_matches!(
                check_range_overlap(&configs).as_slice(),
                [ValidationError::RangesOverlap { .. }]
            );
        }

        #[test]
        fn unbounded_below_then_adjacent_bounded_ok() {
            // `(-inf, 50)` and `[50, 100)` are adjacent, not overlapping.
            let configs = vec![range(0, None, Some(50)), range(1, Some(50), Some(100))];
            assert!(check_range_overlap(&configs).is_empty());
        }

        #[test]
        fn list_and_default_ignored() {
            let configs = vec![list(0, vec![FlexibleType::Integer(5)]), default_shard(1)];
            assert!(check_range_overlap(&configs).is_empty());
        }
    }

    mod validate {
        use super::*;

        #[test]
        fn error_order_is_per_entry() {
            // Entry 0: shard out of range + type mismatch (string in bigint list)
            // Entry 1: range with no bounds
            // Expected order: entry-0 errors, then entry-1 errors, then overlap check (none here)
            let configs = vec![
                list(9, vec![FlexibleType::String("x".into())]),
                range(0, None, None),
            ];
            let errors = validate(&configs, DataType::Bigint, 3);
            assert_matches!(errors[0], ValidationError::ShardOutOfRange { .. });
            assert_matches!(errors[1], ValidationError::IncompatibleType { .. });
            assert_matches!(errors[2], ValidationError::RangeNoBounds { .. });
        }
    }
}
