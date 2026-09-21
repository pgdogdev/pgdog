use super::{TypeError, checked_add_assign};
use crate::net::messages::Datum;
use pgdog_postgres_types::DataType;

#[derive(Debug)]
enum SumValue {
    SmallInt(i128),
    Integer(i128),
    Bigint(i128),
    Other(Datum),
}

impl From<Datum> for SumValue {
    fn from(value: Datum) -> Self {
        match value {
            Datum::SmallInt(value) => Self::SmallInt(i128::from(value)),
            Datum::Integer(value) => Self::Integer(i128::from(value)),
            Datum::Bigint(value) => Self::Bigint(i128::from(value)),
            value => Self::Other(value),
        }
    }
}

impl SumValue {
    fn data_type(&self) -> DataType {
        match self {
            Self::SmallInt(_) => DataType::SmallInt,
            Self::Integer(_) => DataType::Integer,
            Self::Bigint(_) => DataType::Bigint,
            Self::Other(value) => value.data_type(),
        }
    }
}

#[derive(Debug)]
pub(super) struct Sum {
    pub(super) column: usize,
    sum: SumValue,
}

impl Sum {
    pub(super) fn new(column: usize) -> Self {
        Self {
            column,
            sum: Datum::Null.into(),
        }
    }

    pub(super) fn accumulate(&mut self, value: Datum) -> Result<(), TypeError> {
        if value.is_null() {
            return Ok(());
        }
        let data_type = self.sum.data_type();
        match (&mut self.sum, value) {
            (SumValue::Other(Datum::Null), value) => self.sum = value.into(),
            (SumValue::SmallInt(sum), Datum::SmallInt(value)) => {
                add_integer(sum, i128::from(value), data_type)?;
            }
            (SumValue::Integer(sum), Datum::Integer(value)) => {
                add_integer(sum, i128::from(value), data_type)?;
            }
            (SumValue::Bigint(sum), Datum::Bigint(value)) => {
                add_integer(sum, i128::from(value), data_type)?;
            }
            (SumValue::Other(sum), value) => checked_add_assign(sum, value)?,
            (_, value) => return Err(TypeError::IncompatibleTypes(data_type, value.data_type())),
        }
        Ok(())
    }

    pub(super) fn finalize(self) -> Result<Datum, TypeError> {
        let data_type = self.sum.data_type();
        // A later shard can cancel an intermediate overflow. Narrow only once,
        // after all partial sums have been combined.
        let result = match self.sum {
            SumValue::SmallInt(sum) => i16::try_from(sum).map(Datum::SmallInt),
            SumValue::Integer(sum) => i32::try_from(sum).map(Datum::Integer),
            SumValue::Bigint(sum) => i64::try_from(sum).map(Datum::Bigint),
            SumValue::Other(sum) => return Ok(sum),
        };
        result.map_err(|_| TypeError::NumericOutOfRange(data_type))
    }
}

fn add_integer(sum: &mut i128, value: i128, data_type: DataType) -> Result<(), TypeError> {
    *sum = sum
        .checked_add(value)
        .ok_or(TypeError::NumericOutOfRange(data_type))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_matches;

    #[test]
    fn test_sum() {
        let mut state = Sum::new(0);
        state.accumulate(Datum::Null).unwrap();
        state.accumulate(1i64.into()).unwrap();
        state.accumulate(Datum::Null).unwrap();
        state.accumulate(3i64.into()).unwrap();
        state.accumulate(2i64.into()).unwrap();
        assert_eq!(state.finalize().expect("valid sum"), Datum::from(6i64));
    }

    #[test]
    fn mixed_types_produces_error() {
        let mut state = Sum::new(0);
        state.accumulate(1i64.into()).unwrap();
        assert_matches!(state.accumulate(1f64.into()), Err(_));
    }

    #[test]
    fn empty_sum_returns_null() {
        let mut state = Sum::new(0);
        state.accumulate(Datum::Null).unwrap();
        state.accumulate(Datum::Null).unwrap();
        assert_eq!(state.finalize().expect("empty sum"), Datum::Null);
    }

    fn integer_cases() -> [(DataType, Datum, Datum, Datum, Datum); 3] {
        [
            (
                DataType::SmallInt,
                Datum::SmallInt(i16::MAX),
                Datum::SmallInt(i16::MIN),
                Datum::SmallInt(1),
                Datum::SmallInt(-1),
            ),
            (
                DataType::Integer,
                Datum::Integer(i32::MAX),
                Datum::Integer(i32::MIN),
                Datum::Integer(1),
                Datum::Integer(-1),
            ),
            (
                DataType::Bigint,
                Datum::Bigint(i64::MAX),
                Datum::Bigint(i64::MIN),
                Datum::Bigint(1),
                Datum::Bigint(-1),
            ),
        ]
    }

    #[test]
    fn integer_sum_checks_final_bounds() {
        for (data_type, max, min, one, negative_one) in integer_cases() {
            for values in [[max, one], [min, negative_one]] {
                let mut state = Sum::new(0);
                for value in values {
                    state.accumulate(value).expect("wide accumulation");
                }
                assert!(
                    matches!(state.finalize(), Err(TypeError::NumericOutOfRange(ty)) if ty == data_type)
                );
            }
        }
    }

    #[test]
    fn integer_sum_allows_intermediate_overflow_to_cancel() {
        for (_, max, min, one, negative_one) in integer_cases() {
            for (boundary, delta, inverse) in [
                (max, one.clone(), negative_one.clone()),
                (min, negative_one, one),
            ] {
                let mut state = Sum::new(0);
                state.accumulate(boundary.clone()).expect("boundary");
                state.accumulate(delta).expect("wide intermediate sum");
                state.accumulate(Datum::Null).expect("ignore NULL");
                state.accumulate(inverse).expect("cancellation");
                assert_eq!(state.finalize().expect("representable final sum"), boundary);
            }
        }
    }

    #[test]
    fn integer_sum_rejects_mixed_widths_without_changing_state() {
        let mut state = Sum::new(0);
        state.accumulate(Datum::SmallInt(12)).expect("smallint");
        assert!(matches!(
            state.accumulate(Datum::Bigint(30)),
            Err(TypeError::IncompatibleTypes(
                DataType::SmallInt,
                DataType::Bigint
            ))
        ));
        assert_eq!(
            state.finalize().expect("unchanged sum"),
            Datum::SmallInt(12)
        );
    }
}
