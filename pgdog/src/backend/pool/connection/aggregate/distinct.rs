//! `count(DISTINCT x)` over values that more than one shard can have.

use std::collections::HashSet;

use super::{Error, TypeError, von::cast_count};
use crate::net::messages::Datum;
use pgdog_postgres_types::DataType;

/// Each shard returns its distinct values in a hidden `array_agg(DISTINCT x)`
/// column. The count is the size of their union.
#[derive(Debug)]
pub(super) struct CountDistinct {
    pub(super) column: usize,
    pub(super) values_column: usize,
    values: HashSet<Datum>,
    data_type: DataType,
}

impl CountDistinct {
    pub(super) fn new(column: usize, values_column: usize) -> Self {
        Self {
            column,
            values_column,
            values: HashSet::new(),
            data_type: DataType::Bigint,
        }
    }

    /// Add one shard's count, kept only for its type, and its distinct values.
    pub(super) fn accumulate(&mut self, count: Datum, values: Datum) -> Result<(), Error> {
        if !count.is_null() {
            self.data_type = count.data_type();
        }

        let values = match values {
            // No row of this group is on the shard.
            Datum::Null => return Ok(()),
            Datum::Array(values) => values.into_elements(),
            _ => return Err(unsupported_type()),
        };
        for value in values {
            match value {
                // `count` skips NULL.
                Datum::Null => {}
                // Equal values of a type PgDog doesn't decode can differ in bytes.
                Datum::Unknown(_) => return Err(unsupported_type()),
                value => {
                    self.values.insert(value);
                }
            }
        }

        Ok(())
    }

    pub(super) fn finalize(self) -> Result<Datum, TypeError> {
        let total = i64::try_from(self.values.len())
            .map_err(|_| TypeError::NumericOutOfRange(DataType::Bigint))?;
        cast_count(total, self.data_type)
    }
}

fn unsupported_type() -> Error {
    Error::UnsupportedAggregation {
        function: String::from("count"),
        reason: String::from("count(DISTINCT ...) is not yet supported for this type"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgdog_postgres_types::Array;

    fn integers(values: &[Option<i32>]) -> Datum {
        Datum::Array(Array::new(
            values
                .iter()
                .map(|value| value.map_or(Datum::Null, Datum::Integer))
                .collect(),
            23,
        ))
    }

    #[test]
    fn counts_the_union_of_shard_values() {
        let mut state = CountDistinct::new(0, 1);
        state
            .accumulate(Datum::Bigint(2), integers(&[Some(1), Some(2)]))
            .unwrap();
        state
            .accumulate(Datum::Bigint(2), integers(&[Some(2), Some(3), None]))
            .unwrap();
        // No row of the group on this shard.
        state.accumulate(Datum::Bigint(0), Datum::Null).unwrap();

        assert_eq!(state.finalize().unwrap(), Datum::Bigint(3));
    }

    #[test]
    fn keeps_the_type_of_the_shard_count() {
        let mut state = CountDistinct::new(0, 1);
        state
            .accumulate(Datum::Integer(1), integers(&[Some(7)]))
            .unwrap();

        assert_eq!(state.finalize().unwrap(), Datum::Integer(1));
    }

    #[test]
    fn refuses_values_it_cannot_compare() {
        for values in [
            Datum::Unknown("{a,b}".into()),
            Datum::Array(Array::new(vec![Datum::Unknown("a".into())], 0)),
        ] {
            let mut state = CountDistinct::new(0, 1);
            assert!(matches!(
                state.accumulate(Datum::Bigint(1), values),
                Err(Error::UnsupportedAggregation { .. })
            ));
        }
    }
}
