//! `count`, `sum` and `avg` over `DISTINCT x`, where more than one shard can
//! have the same `x`.

use std::cmp::Ordering;
use std::collections::HashSet;

use super::{Error, TypeError, avg, checked_add_assign, von::cast_count};
use crate::net::messages::Datum;
use pgdog_postgres_types::{DataType, Numeric};

/// What to compute from the distinct values.
#[derive(Debug, Clone, Copy)]
enum Function {
    Count,
    Sum,
    Avg,
}

/// Each shard returns its distinct values in a hidden `array_agg(DISTINCT x)`
/// column. The aggregate is computed over their union.
#[derive(Debug)]
pub(super) struct DistinctValues {
    pub(super) column: usize,
    pub(super) values_column: usize,
    function: Function,
    values: HashSet<Datum>,
    /// Type the shards return the aggregate in, which the result keeps.
    data_type: DataType,
}

impl DistinctValues {
    pub(super) fn count(column: usize, values_column: usize) -> Self {
        Self::new(Function::Count, column, values_column)
    }

    pub(super) fn sum(column: usize, values_column: usize) -> Self {
        Self::new(Function::Sum, column, values_column)
    }

    pub(super) fn avg(column: usize, values_column: usize) -> Self {
        Self::new(Function::Avg, column, values_column)
    }

    fn new(function: Function, column: usize, values_column: usize) -> Self {
        Self {
            column,
            values_column,
            function,
            values: HashSet::new(),
            data_type: DataType::Bigint,
        }
    }

    /// Add one shard's aggregate, kept only for its type, and its distinct values.
    pub(super) fn accumulate(&mut self, result: Datum, values: Datum) -> Result<(), Error> {
        if !result.is_null() {
            self.data_type = result.data_type();
        }

        let values = match values {
            // No row of this group is on the shard.
            Datum::Null => return Ok(()),
            Datum::Array(values) => values.into_elements(),
            _ => return Err(self.unsupported_type()),
        };
        self.values.reserve(values.len());
        for value in values {
            match value {
                // Aggregates skip NULL.
                Datum::Null => {}
                // Equal values of a type PgDog doesn't decode can differ in bytes.
                Datum::Unknown(_) => return Err(self.unsupported_type()),
                value => {
                    self.values.insert(value);
                }
            }
        }

        Ok(())
    }

    pub(super) fn finalize(self) -> Result<Datum, Error> {
        let count = i64::try_from(self.values.len())
            .map_err(|_| TypeError::NumericOutOfRange(DataType::Bigint))?;
        match self.function {
            Function::Count => Ok(cast_count(count, self.data_type)?),
            Function::Sum => Ok(sum(self.values, self.data_type)?),
            Function::Avg => avg::divide(sum(self.values, self.data_type)?, count),
        }
    }

    fn unsupported_type(&self) -> Error {
        let function = match self.function {
            Function::Count => "count",
            Function::Sum => "sum",
            Function::Avg => "avg",
        };
        Error::UnsupportedAggregation {
            function: String::from(function),
            reason: format!("{function}(DISTINCT ...) is not yet supported for this type"),
        }
    }
}

/// Sum of the values in `data_type`, the type Postgres adds them up in:
/// wider than the values for integers, so they don't overflow.
fn sum(values: HashSet<Datum>, data_type: DataType) -> Result<Datum, TypeError> {
    let mut sum = Datum::Null;
    let mut add = |value| checked_add_assign(&mut sum, widen(value, data_type)?);

    if matches!(data_type, DataType::Real | DataType::DoublePrecision) {
        // Float addition depends on the order. Postgres adds DISTINCT values
        // in ascending order.
        let mut values = Vec::from_iter(values);
        values.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        values.into_iter().try_for_each(&mut add)?;
    } else {
        values.into_iter().try_for_each(&mut add)?;
    }

    Ok(sum)
}

/// Convert a value to the type its sum is computed in.
fn widen(value: Datum, to: DataType) -> Result<Datum, TypeError> {
    match (value, to) {
        (value @ (Datum::SmallInt(_) | Datum::Integer(_) | Datum::Bigint(_)), DataType::Bigint) => {
            Ok(Datum::Bigint(value.as_i64()?))
        }
        (
            value @ (Datum::SmallInt(_) | Datum::Integer(_) | Datum::Bigint(_)),
            DataType::Numeric,
        ) => Ok(Datum::Numeric(Numeric::from(value.as_i64()?))),
        (Datum::Float(value), DataType::DoublePrecision) => Ok(Datum::from(f64::from(value.0))),
        (value, to) if value.data_type() == to => Ok(value),
        (value, to) => Err(TypeError::InvalidCast {
            from: value.data_type(),
            to,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgdog_postgres_types::Array;
    use rust_decimal::Decimal;

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
        let mut state = DistinctValues::count(0, 1);
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
        let mut state = DistinctValues::count(0, 1);
        state
            .accumulate(Datum::Integer(1), integers(&[Some(7)]))
            .unwrap();

        assert_eq!(state.finalize().unwrap(), Datum::Integer(1));
    }

    #[test]
    fn sums_and_averages_each_value_once() {
        // Both shards have 20. Their sums, 30 and 50, are in the result's type.
        let shards = [[Some(10), Some(20)], [Some(20), Some(30)]];

        let mut state = DistinctValues::sum(0, 1);
        for (sum, values) in [30, 50].into_iter().zip(shards) {
            state
                .accumulate(Datum::Bigint(sum), integers(&values))
                .unwrap();
        }
        assert_eq!(state.finalize().unwrap(), Datum::Bigint(60));

        let mut state = DistinctValues::avg(0, 1);
        for (avg, values) in [15, 25].into_iter().zip(shards) {
            let avg = Decimal::new(avg * 10_000, 4);
            state
                .accumulate(Datum::Numeric(avg.into()), integers(&values))
                .unwrap();
        }
        let Datum::Numeric(avg) = state.finalize().unwrap() else {
            panic!("avg of integers is numeric");
        };
        let avg = avg.as_decimal().copied().expect("a number");
        assert_eq!(avg, Decimal::from(20));
        // Postgres returns 20.0000000000000000.
        assert_eq!(avg.scale(), 16);
    }

    #[test]
    fn sums_floats_in_ascending_order() {
        // Adding 1e16 first would lose both ones.
        let values = [1e16, 1.0, 1.0000000000000002].map(Datum::from);
        let mut state = DistinctValues::sum(0, 1);
        state
            .accumulate(
                Datum::from(0.0),
                Datum::Array(Array::new(values.into(), 701)),
            )
            .unwrap();

        assert_eq!(state.finalize().unwrap(), Datum::from(1e16 + 2.0));
    }

    #[test]
    fn nothing_to_sum_is_null() {
        for mut state in [DistinctValues::sum(0, 1), DistinctValues::avg(0, 1)] {
            state.accumulate(Datum::Null, integers(&[None])).unwrap();
            state.accumulate(Datum::Null, Datum::Null).unwrap();
            assert_eq!(state.finalize().unwrap(), Datum::Null);
        }
    }

    #[test]
    fn refuses_values_it_cannot_compare() {
        for values in [
            Datum::Unknown("{a,b}".into()),
            Datum::Array(Array::new(vec![Datum::Unknown("a".into())], 0)),
        ] {
            let mut state = DistinctValues::count(0, 1);
            assert!(matches!(
                state.accumulate(Datum::Bigint(1), values),
                Err(Error::UnsupportedAggregation { .. })
            ));
        }
    }
}
