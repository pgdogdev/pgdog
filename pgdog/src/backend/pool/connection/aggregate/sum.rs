use super::{TypeError, checked_add_assign};
use crate::net::messages::Datum;

#[derive(Debug)]
pub(super) struct Sum {
    pub(super) column: usize,
    sum: Datum,
}

impl Sum {
    pub(super) fn new(column: usize) -> Self {
        Self {
            column,
            sum: Datum::Null,
        }
    }

    pub(super) fn accumulate(&mut self, value: Datum) -> Result<(), TypeError> {
        if self.sum.is_null() {
            self.sum = value;
            Ok(())
        } else if !value.is_null() {
            checked_add_assign(&mut self.sum, value)
        } else {
            Ok(())
        }
    }

    pub(super) fn finalize(self) -> Datum {
        self.sum
    }
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
        assert_eq!(state.finalize(), Datum::from(6i64));
    }

    #[test]
    fn mixed_types_produces_error() {
        let mut state = Sum::new(0);
        state.accumulate(1i64.into()).unwrap();
        assert_matches!(state.accumulate(1f64.into()), Err(_));
    }
}
