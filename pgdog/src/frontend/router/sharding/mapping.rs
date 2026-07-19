use std::cmp::Ordering;

use indexmap::IndexMap;
use pgdog_config::{FlexibleType, FlexibleTypeRef, ShardedMappingConfig, ShardedMappingRange};

use crate::frontend::router::parser::Shard;
use crate::frontend::router::sharding::{Error, Value};

#[derive(Debug, Clone, Eq, PartialEq)]
struct ListShards {
    mapping: IndexMap<FlexibleType, usize>,
}

impl ListShards {
    fn shard(&self, value: &FlexibleTypeRef<'_>) -> Option<usize> {
        self.mapping.get(value).copied()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct RangeShards {
    mapping: Vec<ShardedMappingRange>,
}

impl RangeShards {
    fn shard(&self, value: &FlexibleTypeRef<'_>) -> Option<usize> {
        self.mapping
            .iter()
            .find_map(|range| range_shard(range, value))
    }
}

pub(crate) fn compare_flexible_type(
    value: &FlexibleTypeRef<'_>,
    ty: &FlexibleType,
) -> Option<Ordering> {
    match (value, ty) {
        (FlexibleTypeRef::Integer(a), FlexibleType::Integer(b)) => Some(a.cmp(b)),
        (FlexibleTypeRef::Uuid(a), FlexibleType::Uuid(b)) => Some(a.cmp(&b)),
        (FlexibleTypeRef::String(a), FlexibleType::String(b)) => Some(a.cmp(&b.as_str())),
        _ => None,
    }
}

pub(crate) fn range_shard(
    range: &ShardedMappingRange,
    value: &FlexibleTypeRef<'_>,
) -> Option<usize> {
    let after_start = range
        .start
        .as_ref()
        .map(|start| {
            matches!(
                compare_flexible_type(value, start),
                Some(Ordering::Greater) | Some(Ordering::Equal)
            )
        })
        .unwrap_or(true);

    let before_end = range
        .end
        .as_ref()
        .map(|end| matches!(compare_flexible_type(value, end), Some(Ordering::Less)))
        .unwrap_or(true);

    if after_start && before_end {
        Some(range.shard)
    } else {
        None
    }
}

/// Runtime mapping of explicit column values or ranges to shard numbers.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Mapping {
    list: ListShards,
    range: RangeShards,
    pub default: Option<usize>,
}

impl Mapping {
    pub fn new(mappings: Vec<ShardedMappingConfig>) -> Option<Self> {
        let mut list = IndexMap::new();
        let mut range = Vec::new();
        let mut default = None;

        for mapping in mappings {
            match mapping {
                ShardedMappingConfig::Default { shard } => {
                    default = Some(shard);
                }
                ShardedMappingConfig::List(l) => {
                    list.extend(l.values.into_iter().map(|v| (v, l.shard)));
                }
                ShardedMappingConfig::Range(r) => {
                    range.push(r);
                }
            }
        }

        if !list.is_empty() || !range.is_empty() || default.is_some() {
            Some(Self {
                list: ListShards { mapping: list },
                range: RangeShards { mapping: range },
                default,
            })
        } else {
            None
        }
    }

    pub fn shard(&self, value: &FlexibleTypeRef<'_>) -> Option<usize> {
        self.list
            .shard(value)
            .or_else(|| self.range.shard(value))
            .or(self.default)
    }
}

#[derive(Debug)]
pub struct MappingResolver<'a> {
    mapping: &'a Mapping,
}

impl<'a> MappingResolver<'a> {
    pub fn new(mapping: &'a Option<Mapping>) -> Option<Self> {
        mapping.as_ref().map(|mapping| Self { mapping })
    }

    pub fn shard(&self, value: &Value) -> Result<Shard, Error> {
        if let Some(value) = value.integer()? {
            Ok(self.mapping.shard(&FlexibleTypeRef::Integer(value)).into())
        } else if let Some(value) = value.uuid()? {
            Ok(self.mapping.shard(&FlexibleTypeRef::Uuid(&value)).into())
        } else if let Some(value) = value.varchar()? {
            Ok(self.mapping.shard(&FlexibleTypeRef::String(value)).into())
        } else {
            Ok(Shard::All)
        }
    }
}

#[cfg(test)]
mod tests {
    use pgdog_config::{
        FlexibleType, FlexibleTypeRef, ShardedMappingConfig, ShardedMappingList,
        ShardedMappingRange,
    };
    use uuid::Uuid;

    use super::*;

    // ── helpers ──────────────────────────────────────────────────────────────

    fn list(values: Vec<i64>, shard: usize) -> ShardedMappingConfig {
        ShardedMappingConfig::List(ShardedMappingList {
            values: values.into_iter().map(FlexibleType::Integer).collect(),
            shard,
        })
    }

    fn str_list(values: Vec<&str>, shard: usize) -> ShardedMappingConfig {
        ShardedMappingConfig::List(ShardedMappingList {
            values: values
                .into_iter()
                .map(|s| FlexibleType::String(s.into()))
                .collect(),
            shard,
        })
    }

    fn uuid_list(values: Vec<Uuid>, shard: usize) -> ShardedMappingConfig {
        ShardedMappingConfig::List(ShardedMappingList {
            values: values.into_iter().map(FlexibleType::Uuid).collect(),
            shard,
        })
    }

    fn range(start: Option<i64>, end: Option<i64>, shard: usize) -> ShardedMappingConfig {
        ShardedMappingConfig::Range(ShardedMappingRange {
            start: start.map(FlexibleType::Integer),
            end: end.map(FlexibleType::Integer),
            shard,
        })
    }

    fn str_range(start: Option<&str>, end: Option<&str>, shard: usize) -> ShardedMappingConfig {
        ShardedMappingConfig::Range(ShardedMappingRange {
            start: start.map(|s| FlexibleType::String(s.into())),
            end: end.map(|s| FlexibleType::String(s.into())),
            shard,
        })
    }

    fn uuid_range(start: Option<Uuid>, end: Option<Uuid>, shard: usize) -> ShardedMappingConfig {
        ShardedMappingConfig::Range(ShardedMappingRange {
            start: start.map(FlexibleType::Uuid),
            end: end.map(FlexibleType::Uuid),
            shard,
        })
    }

    fn default(shard: usize) -> ShardedMappingConfig {
        ShardedMappingConfig::Default { shard }
    }

    fn shard_int(m: &Mapping, v: i64) -> Option<usize> {
        m.shard(&FlexibleTypeRef::Integer(v))
    }

    fn shard_str(m: &Mapping, v: &str) -> Option<usize> {
        m.shard(&FlexibleTypeRef::String(v))
    }

    fn shard_uuid(m: &Mapping, v: Uuid) -> Option<usize> {
        m.shard(&FlexibleTypeRef::Uuid(&v))
    }

    mod compare_fn {
        use super::*;

        #[test]
        fn integer_ordering() {
            assert_eq!(
                compare_flexible_type(&FlexibleTypeRef::Integer(5), &FlexibleType::Integer(5)),
                Some(Ordering::Equal)
            );
            assert_eq!(
                compare_flexible_type(&FlexibleTypeRef::Integer(6), &FlexibleType::Integer(5)),
                Some(Ordering::Greater)
            );
            assert_eq!(
                compare_flexible_type(&FlexibleTypeRef::Integer(4), &FlexibleType::Integer(5)),
                Some(Ordering::Less)
            );
        }

        #[test]
        fn string_ordering() {
            assert_eq!(
                compare_flexible_type(
                    &FlexibleTypeRef::String("b"),
                    &FlexibleType::String("b".into())
                ),
                Some(Ordering::Equal)
            );
            assert_eq!(
                compare_flexible_type(
                    &FlexibleTypeRef::String("c"),
                    &FlexibleType::String("b".into())
                ),
                Some(Ordering::Greater)
            );
            assert_eq!(
                compare_flexible_type(
                    &FlexibleTypeRef::String("a"),
                    &FlexibleType::String("b".into())
                ),
                Some(Ordering::Less)
            );
        }

        #[test]
        fn uuid_ordering() {
            let lo = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
            let hi = Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap();
            assert_eq!(
                compare_flexible_type(&FlexibleTypeRef::Uuid(&lo), &FlexibleType::Uuid(lo)),
                Some(Ordering::Equal)
            );
            assert_eq!(
                compare_flexible_type(&FlexibleTypeRef::Uuid(&hi), &FlexibleType::Uuid(lo)),
                Some(Ordering::Greater)
            );
            assert_eq!(
                compare_flexible_type(&FlexibleTypeRef::Uuid(&lo), &FlexibleType::Uuid(hi)),
                Some(Ordering::Less)
            );
        }

        #[test]
        fn type_mismatch_returns_none() {
            let u = Uuid::nil();
            assert_eq!(
                compare_flexible_type(
                    &FlexibleTypeRef::Integer(1),
                    &FlexibleType::String("1".into())
                ),
                None
            );
            assert_eq!(
                compare_flexible_type(&FlexibleTypeRef::String("a"), &FlexibleType::Integer(1)),
                None
            );
            assert_eq!(
                compare_flexible_type(&FlexibleTypeRef::Uuid(&u), &FlexibleType::Integer(0)),
                None
            );
            assert_eq!(
                compare_flexible_type(&FlexibleTypeRef::Integer(0), &FlexibleType::Uuid(u)),
                None
            );
        }
    }

    mod range_shard_fn {
        use super::*;

        fn r(start: Option<i64>, end: Option<i64>) -> ShardedMappingRange {
            ShardedMappingRange {
                start: start.map(FlexibleType::Integer),
                end: end.map(FlexibleType::Integer),
                shard: 7,
            }
        }

        fn rs(start: Option<&str>, end: Option<&str>) -> ShardedMappingRange {
            ShardedMappingRange {
                start: start.map(|s| FlexibleType::String(s.into())),
                end: end.map(|s| FlexibleType::String(s.into())),
                shard: 7,
            }
        }

        fn hit(rng: &ShardedMappingRange, v: &FlexibleTypeRef<'_>) {
            assert_eq!(range_shard(rng, v), Some(7));
        }
        fn miss(rng: &ShardedMappingRange, v: &FlexibleTypeRef<'_>) {
            assert_eq!(range_shard(rng, v), None);
        }

        #[test]
        fn closed_range() {
            let rng = r(Some(10), Some(20));
            hit(&rng, &FlexibleTypeRef::Integer(10));
            hit(&rng, &FlexibleTypeRef::Integer(19));
            miss(&rng, &FlexibleTypeRef::Integer(20));
            miss(&rng, &FlexibleTypeRef::Integer(9));
        }

        #[test]
        fn open_start() {
            let rng = r(None, Some(50));
            hit(&rng, &FlexibleTypeRef::Integer(i64::MIN));
            hit(&rng, &FlexibleTypeRef::Integer(49));
            miss(&rng, &FlexibleTypeRef::Integer(50));
        }

        #[test]
        fn open_end() {
            let rng = r(Some(100), None);
            hit(&rng, &FlexibleTypeRef::Integer(100));
            hit(&rng, &FlexibleTypeRef::Integer(i64::MAX));
            miss(&rng, &FlexibleTypeRef::Integer(99));
        }

        #[test]
        fn fully_open() {
            let rng = r(None, None);
            hit(&rng, &FlexibleTypeRef::Integer(i64::MIN));
            hit(&rng, &FlexibleTypeRef::Integer(i64::MAX));
        }

        #[test]
        fn string_range() {
            let rng = rs(Some("g"), Some("p"));
            hit(&rng, &FlexibleTypeRef::String("g"));
            hit(&rng, &FlexibleTypeRef::String("m"));
            miss(&rng, &FlexibleTypeRef::String("p"));
            miss(&rng, &FlexibleTypeRef::String("a"));
        }

        #[test]
        fn type_mismatch_misses() {
            let rng = r(Some(0), Some(100));
            miss(&rng, &FlexibleTypeRef::String("50"));
            let u = Uuid::nil();
            miss(&rng, &FlexibleTypeRef::Uuid(&u));
        }
    }

    // ── construction ─────────────────────────────────────────────────────────

    mod construction {
        use super::*;

        #[test]
        fn empty_returns_none() {
            assert_eq!(Mapping::new(vec![]), None);
        }

        #[test]
        fn default_only_is_valid() {
            let m = Mapping::new(vec![default(3)]).unwrap();
            assert_eq!(m.default, Some(3));
        }
    }

    // ── list mapping ─────────────────────────────────────────────────────────

    mod list {
        use super::*;

        #[test]
        fn exact_integer_match() {
            let m = Mapping::new(vec![list(vec![10, 20], 0), list(vec![30], 1)]).unwrap();
            assert_eq!(shard_int(&m, 10), Some(0));
            assert_eq!(shard_int(&m, 20), Some(0));
            assert_eq!(shard_int(&m, 30), Some(1));
        }

        #[test]
        fn miss_without_default_returns_none() {
            let m = Mapping::new(vec![list(vec![1, 2], 0)]).unwrap();
            assert_eq!(shard_int(&m, 99), None);
        }

        #[test]
        fn string_exact_match() {
            let m = Mapping::new(vec![
                str_list(vec!["alice", "bob"], 0),
                str_list(vec!["carol"], 1),
            ])
            .unwrap();
            assert_eq!(shard_str(&m, "alice"), Some(0));
            assert_eq!(shard_str(&m, "bob"), Some(0));
            assert_eq!(shard_str(&m, "carol"), Some(1));
            assert_eq!(shard_str(&m, "dave"), None);
        }

        #[test]
        fn uuid_exact_match() {
            let u0 = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
            let u1 = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
            let m = Mapping::new(vec![uuid_list(vec![u0], 0), uuid_list(vec![u1], 1)]).unwrap();
            assert_eq!(shard_uuid(&m, u0), Some(0));
            assert_eq!(shard_uuid(&m, u1), Some(1));
            assert_eq!(
                shard_uuid(
                    &m,
                    Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap()
                ),
                None
            );
        }
    }

    // ── range mapping ─────────────────────────────────────────────────────────

    mod range {
        use super::*;

        #[test]
        fn inclusive_start_exclusive_end() {
            // [0, 100)
            let m = Mapping::new(vec![range(Some(0), Some(100), 0)]).unwrap();
            assert_eq!(shard_int(&m, 0), Some(0)); // at start — included
            assert_eq!(shard_int(&m, 50), Some(0));
            assert_eq!(shard_int(&m, 99), Some(0)); // last included
            assert_eq!(shard_int(&m, 100), None); // at end — excluded
            assert_eq!(shard_int(&m, -1), None); // below start
        }

        #[test]
        fn open_start() {
            // (−∞, 50)
            let m = Mapping::new(vec![range(None, Some(50), 0)]).unwrap();
            assert_eq!(shard_int(&m, i64::MIN), Some(0));
            assert_eq!(shard_int(&m, 49), Some(0));
            assert_eq!(shard_int(&m, 50), None); // end is exclusive
        }
        #[test]
        fn open_end() {
            // [100, +∞)
            let m = Mapping::new(vec![range(Some(100), None, 0)]).unwrap();
            assert_eq!(shard_int(&m, 100), Some(0));
            assert_eq!(shard_int(&m, i64::MAX), Some(0));
            assert_eq!(shard_int(&m, 99), None);
        }

        #[test]
        fn multiple_ranges_first_match_wins() {
            let m = Mapping::new(vec![
                range(Some(0), Some(33), 0),
                range(Some(33), Some(66), 1),
                range(Some(66), Some(99), 2),
            ])
            .unwrap();
            assert_eq!(shard_int(&m, 0), Some(0));
            assert_eq!(shard_int(&m, 32), Some(0));
            assert_eq!(shard_int(&m, 33), Some(1));
            assert_eq!(shard_int(&m, 65), Some(1));
            assert_eq!(shard_int(&m, 66), Some(2));
            assert_eq!(shard_int(&m, 98), Some(2));
            assert_eq!(shard_int(&m, 99), None); // gap after last range
        }

        #[test]
        fn gap_between_ranges_returns_none() {
            // [0, 10) then [20, 30) — value 15 falls in the gap
            let m = Mapping::new(vec![
                range(Some(0), Some(10), 0),
                range(Some(20), Some(30), 1),
            ])
            .unwrap();
            assert_eq!(shard_int(&m, 15), None);
        }

        #[test]
        fn string_bounds() {
            // ["a", "m") → shard 0, ["m", "z") → shard 1
            let m = Mapping::new(vec![
                str_range(Some("a"), Some("m"), 0),
                str_range(Some("m"), Some("z"), 1),
            ])
            .unwrap();
            assert_eq!(shard_str(&m, "a"), Some(0));
            assert_eq!(shard_str(&m, "l"), Some(0));
            assert_eq!(shard_str(&m, "m"), Some(1));
            assert_eq!(shard_str(&m, "y"), Some(1));
            assert_eq!(shard_str(&m, "z"), None);
        }

        #[test]
        fn uuid_bounds() {
            let lo = Uuid::parse_str("00000000-0000-0000-0000-000000000000").unwrap();
            let mid = Uuid::parse_str("80000000-0000-0000-0000-000000000000").unwrap();
            let hi = Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap();
            let m = Mapping::new(vec![
                uuid_range(Some(lo), Some(mid), 0),
                uuid_range(Some(mid), None, 1),
            ])
            .unwrap();
            assert_eq!(shard_uuid(&m, lo), Some(0));
            assert_eq!(shard_uuid(&m, mid), Some(1)); // mid is end of first → exclusive
            assert_eq!(shard_uuid(&m, hi), Some(1));
        }
    }

    // ── resolution order ─────────────────────────────────────────────────────
    //
    // One test per meaningful combination; each exercises multiple values so a
    // single function demonstrates the full routing decision tree for that setup.

    mod resolution_order {
        use super::*;

        /// list + range + default: all three layers active.
        /// Expected priority: list first, then range, then default.
        #[test]
        fn list_range_default() {
            let m = Mapping::new(vec![
                list(vec![1, 2], 0),
                list(vec![3, 103], 1),
                range(Some(100), Some(200), 2),
                range(Some(200), Some(300), 3),
                default(9),
            ])
            .unwrap();

            // list hits
            assert_eq!(shard_int(&m, 1), Some(0));
            assert_eq!(shard_int(&m, 2), Some(0));
            assert_eq!(shard_int(&m, 3), Some(1));
            assert_eq!(shard_int(&m, 103), Some(1));

            // range hits (list misses)
            assert_eq!(shard_int(&m, 100), Some(2)); // range start inclusive
            assert_eq!(shard_int(&m, 150), Some(2));
            assert_eq!(shard_int(&m, 199), Some(2)); // last value in first range
            assert_eq!(shard_int(&m, 200), Some(3)); // boundary: end of first, start of second
            assert_eq!(shard_int(&m, 299), Some(3)); // last value in second range

            // default (list miss, range miss)
            assert_eq!(shard_int(&m, 0), Some(9)); // below range
            assert_eq!(shard_int(&m, 300), Some(9)); // range end exclusive → default
            assert_eq!(shard_int(&m, 999), Some(9));
            assert_eq!(shard_int(&m, -1), Some(9));
        }

        /// list + default: no range configured.
        /// Values either hit the list exactly or fall to the default.
        #[test]
        fn list_default() {
            let m = Mapping::new(vec![
                list(vec![10, 20, 30], 0),
                list(vec![40, 50], 1),
                default(7),
            ])
            .unwrap();

            // list hits
            assert_eq!(shard_int(&m, 10), Some(0));
            assert_eq!(shard_int(&m, 20), Some(0));
            assert_eq!(shard_int(&m, 30), Some(0));
            assert_eq!(shard_int(&m, 40), Some(1));
            assert_eq!(shard_int(&m, 50), Some(1));

            // default (not in any list)
            assert_eq!(shard_int(&m, 0), Some(7));
            assert_eq!(shard_int(&m, 11), Some(7)); // near-miss
            assert_eq!(shard_int(&m, 99), Some(7));
            assert_eq!(shard_int(&m, -1), Some(7));
        }

        /// list + range: no default.
        /// Values hit list or range; anything outside both yields None.
        #[test]
        fn list_range() {
            let m = Mapping::new(vec![
                list(vec![5, 6, 15], 0),
                range(Some(10), Some(20), 1),
                range(Some(50), Some(60), 2),
            ])
            .unwrap();

            // list hits
            assert_eq!(shard_int(&m, 5), Some(0));
            assert_eq!(shard_int(&m, 6), Some(0));
            assert_eq!(shard_int(&m, 15), Some(0));

            // range hits
            assert_eq!(shard_int(&m, 10), Some(1)); // range start inclusive
            assert_eq!(shard_int(&m, 19), Some(1)); // last in range
            assert_eq!(shard_int(&m, 50), Some(2));
            assert_eq!(shard_int(&m, 59), Some(2));

            // misses → None
            assert_eq!(shard_int(&m, 20), None); // range end exclusive
            assert_eq!(shard_int(&m, 60), None); // second range end exclusive
            assert_eq!(shard_int(&m, 7), None); // gap between list and first range
            assert_eq!(shard_int(&m, 30), None); // gap between ranges
            assert_eq!(shard_int(&m, 99), None); // beyond all ranges
        }

        /// range + default: no list configured.
        /// Values either fall in a range or hit the default.
        #[test]
        fn range_default() {
            let m = Mapping::new(vec![
                range(Some(0), Some(100), 0),
                range(Some(100), Some(200), 1),
                default(9),
            ])
            .unwrap();

            // range hits
            assert_eq!(shard_int(&m, 0), Some(0)); // start inclusive
            assert_eq!(shard_int(&m, 50), Some(0));
            assert_eq!(shard_int(&m, 99), Some(0)); // last in first range
            assert_eq!(shard_int(&m, 100), Some(1)); // boundary: end of first, start of second
            assert_eq!(shard_int(&m, 150), Some(1));
            assert_eq!(shard_int(&m, 199), Some(1)); // last in second range

            // default (range end exclusive, or beyond all ranges)
            assert_eq!(shard_int(&m, 200), Some(9)); // end of last range → default
            assert_eq!(shard_int(&m, 999), Some(9));
            assert_eq!(shard_int(&m, -1), Some(9)); // below first range start → default
        }
    }
}
