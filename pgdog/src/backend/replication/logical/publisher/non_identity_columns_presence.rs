//! Bitmask recording which non-identity columns are present in a replication tuple.

use bit_vec::BitVec;

use super::super::Error;
use crate::backend::replication::logical::publisher::table::Table;
use crate::net::messages::replication::logical::tuple_data::Identifier;

/// Presence flags for the non-identity columns of a replication UPDATE tuple.
///
/// Bit `i` is **set** when the `i`-th non-identity column in table order is
/// present (not `Identifier::Toasted`). Identity columns are not represented
/// here — they are always present by construction.
///
/// Uses a heap-allocated [`BitVec`], so there is no hard limit on the number
/// of non-identity columns a table may have.
///
/// Implements [`Hash`] and [`Eq`] for direct use as a `HashMap` key, e.g.
/// caching per-shape prepared statements in the subscriber.
/// [`count_present`] is O(1) (cached at construction).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NonIdentityColumnsPresence {
    mask: BitVec,
}

impl NonIdentityColumnsPresence {
    /// Build from an UPDATE tuple.
    ///
    /// Returns [`Error::MissingData`] when tuple and table column counts differ.
    pub fn from_tuple(
        tuple: &crate::net::replication::TupleData,
        table: &Table,
    ) -> Result<Self, Error> {
        if tuple.columns.len() != table.columns.len() {
            return Err(Error::MissingData);
        }
        let non_id_count = table.columns.iter().filter(|c| !c.identity).count();
        let mut mask = BitVec::from_elem(non_id_count, false);
        let mut idx = 0usize;
        for (tcol, col) in tuple.columns.iter().zip(table.columns.iter()) {
            if col.identity {
                continue;
            }
            if tcol.identifier != Identifier::Toasted {
                mask.set(idx, true);
            }
            idx += 1;
        }
        Ok(Self { mask })
    }

    /// Whether the `idx`-th non-identity column is present.
    pub fn is_set(&self, idx: usize) -> bool {
        self.mask.get(idx).unwrap_or(false)
    }

    /// Whether no non-identity column is present (all are `'u'`).
    pub fn no_non_identity_present(&self) -> bool {
        self.mask.none()
    }
}

#[cfg(test)]
impl NonIdentityColumnsPresence {
    /// Every non-identity column marked present. Test helper.
    ///
    /// Uses the same allocation size as [`Self::from_tuple`] (non-identity
    /// count only) so that two presences with identical logical shapes compare
    /// equal under `Eq`/`Hash` and can be used as cache keys.
    pub(crate) fn all(table: &Table) -> Self {
        let non_id_count = table.columns.iter().filter(|c| !c.identity).count();
        Self {
            mask: BitVec::from_elem(non_id_count, true),
        }
    }

    /// Whether every non-identity column is present.
    pub(crate) fn is_all_present(&self) -> bool {
        self.mask.all()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::backend::replication::logical::publisher::{
        queries::{PublicationTable, PublicationTableColumn, ReplicaIdentity},
        table::Table,
    };
    use crate::backend::replication::publisher::Lsn;
    use crate::net::messages::replication::logical::tuple_data::{
        text_col, toasted_col, TupleData,
    };
    use pgdog_config::QueryParserEngine;
    use pgdog_postgres_types::Oid;

    fn make_table(columns: Vec<(&str, bool)>) -> Table {
        Table {
            publication: "test".to_string(),
            table: PublicationTable {
                schema: "public".to_string(),
                name: "test_table".to_string(),
                attributes: "".to_string(),
                parent_schema: "".to_string(),
                parent_name: "".to_string(),
            },
            identity: ReplicaIdentity {
                oid: Oid(1),
                identity: "".to_string(),
                kind: "".to_string(),
            },
            columns: columns
                .into_iter()
                .map(|(name, identity)| PublicationTableColumn {
                    oid: 1,
                    name: name.to_string(),
                    type_oid: Oid(23),
                    identity,
                })
                .collect(),
            lsn: Lsn::default(),
            query_parser_engine: QueryParserEngine::default(),
        }
    }

    #[test]
    fn from_tuple_partial_presence() {
        let table = make_table(vec![("id", true), ("a", false), ("b", false), ("c", false)]);
        let t = TupleData {
            columns: vec![text_col("1"), text_col("x"), toasted_col(), text_col("z")],
        };
        let p = NonIdentityColumnsPresence::from_tuple(&t, &table).unwrap();
        // non-identity indices: a=0, b=1, c=2
        assert!(p.is_set(0)); // a present
        assert!(!p.is_set(1)); // b toasted
        assert!(p.is_set(2)); // c present
        assert!(!p.is_all_present());
        assert!(!p.no_non_identity_present());
    }

    #[test]
    fn all_marks_every_col() {
        let table = make_table(vec![("id", true), ("a", false), ("b", false)]);
        let p = NonIdentityColumnsPresence::all(&table);
        assert!(p.is_all_present());
    }

    #[test]
    fn no_non_identity_present_when_all_toasted() {
        let table = make_table(vec![("id", true), ("a", false), ("b", false)]);
        let t = TupleData {
            columns: vec![text_col("1"), toasted_col(), toasted_col()],
        };
        let p = NonIdentityColumnsPresence::from_tuple(&t, &table).unwrap();
        assert!(p.no_non_identity_present());
    }

    #[test]
    fn equal_shapes_hash_consistently() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let table = make_table(vec![("id", true), ("a", false), ("b", false)]);
        let p1 = NonIdentityColumnsPresence::all(&table);
        let p2 = NonIdentityColumnsPresence::all(&table);
        assert_eq!(p1, p2);

        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        p1.hash(&mut h1);
        p2.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }

    #[test]
    fn large_table_no_limit() {
        let mut cols: Vec<(&str, bool)> = vec![("id", true)];
        let names: Vec<String> = (0..300).map(|i| format!("c{}", i)).collect();
        for n in &names {
            cols.push((n.as_str(), false));
        }
        let table = make_table(cols);
        let p = NonIdentityColumnsPresence::all(&table);
        assert!(p.is_all_present());
    }
}
