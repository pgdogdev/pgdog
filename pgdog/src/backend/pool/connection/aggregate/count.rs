use super::TypeError;
use crate::net::messages::Datum;
use pgdog_postgres_types::{DataType, Float};

#[derive(Debug)]
pub(super) struct Count {
    pub(super) column: usize,
    total: i64,
    data_type: DataType,
}

impl Count {
    pub(super) fn new(column: usize) -> Self {
        Self {
            column,
            total: 0,
            data_type: DataType::Bigint,
        }
    }

    pub(super) fn accumulate(&mut self, value: Datum) -> Result<(), TypeError> {
        if value.is_null() {
            return Ok(());
        }

        self.total = self
            .total
            .checked_add(value.as_i64()?)
            .ok_or(TypeError::NumericOutOfRange(DataType::Bigint))?;
        self.data_type = value.data_type();
        Ok(())
    }

    pub(super) fn finalize(self) -> Result<Datum, TypeError> {
        match self.data_type {
            DataType::SmallInt => i16::try_from(self.total)
                .map(Datum::SmallInt)
                .map_err(|_| TypeError::NumericOutOfRange(self.data_type)),
            DataType::Integer => i32::try_from(self.total)
                .map(Datum::Integer)
                .map_err(|_| TypeError::NumericOutOfRange(self.data_type)),
            DataType::Bigint => Ok(self.total.into()),
            DataType::Real => Ok(Datum::Float(Float(self.total as f32))),
            DataType::DoublePrecision => Ok(Datum::from(self.total as f64)),
            to => Err(TypeError::InvalidCast {
                from: DataType::Bigint,
                to,
            }),
        }
    }

    pub(super) fn finalize_i64(self) -> i64 {
        self.total
    }
}

#[test]
fn count_with_null() {
    let mut state = Count::new(0);
    state.accumulate(Datum::Null).unwrap();
    state.accumulate(1i64.into()).unwrap();
    state.accumulate(Datum::Null).unwrap();
    state.accumulate(2i64.into()).unwrap();
    assert_eq!(state.finalize().expect("valid count"), 3i64.into());
}

#[test]
fn count_with_only_null() {
    let mut state = Count::new(0);
    state.accumulate(Datum::Null).unwrap();
    state.accumulate(Datum::Null).unwrap();
    assert_eq!(state.finalize().expect("valid count"), 0i64.into());
}

#[test]
fn count_cast_rejects_out_of_range_results() {
    for value in [Datum::SmallInt(i16::MAX), Datum::Integer(i32::MAX)] {
        let data_type = value.data_type();
        let mut state = Count::new(0);
        state
            .accumulate(value.clone())
            .expect("first partial count");
        state.accumulate(value).expect("second partial count");
        assert!(matches!(
            state.finalize(),
            Err(TypeError::NumericOutOfRange(ty)) if ty == data_type
        ));
    }

    let mut state = Count::new(0);
    state.accumulate(i64::MAX.into()).expect("maximum count");
    assert!(matches!(
        state.accumulate(1i64.into()),
        Err(TypeError::NumericOutOfRange(DataType::Bigint))
    ));
}
