use super::TypeError;
use crate::net::messages::Datum;
use std::cmp::{Ordering, PartialOrd};

#[derive(Debug)]
pub(super) struct Cmp {
    pub(super) column: usize,
    ordering: Ordering,
    value: Datum,
}

impl Cmp {
    pub(super) fn max(column: usize) -> Self {
        Self {
            column,
            ordering: Ordering::Less,
            value: Datum::Null,
        }
    }

    pub(super) fn min(column: usize) -> Self {
        Self {
            column,
            ordering: Ordering::Greater,
            value: Datum::Null,
        }
    }

    pub(super) fn accumulate(&mut self, value: Datum) -> Result<(), TypeError> {
        if self.value.is_null() {
            self.value = value;
            Ok(())
        } else if !value.is_null() {
            match self.value.partial_cmp(&value) {
                Some(ord) if ord == self.ordering => Ok(self.value = value),
                Some(_) => Ok(()),
                None => Err(TypeError::IncompatibleTypes(
                    self.value.data_type(),
                    value.data_type(),
                )),
            }
        } else {
            Ok(())
        }
    }

    pub(super) fn finalize(self) -> Datum {
        self.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_matches;

    #[test]
    fn test_max() {
        let mut state = Cmp::max(0);
        state.accumulate(Datum::Null).unwrap();
        state.accumulate(1i64.into()).unwrap();
        state.accumulate(Datum::Null).unwrap();
        state.accumulate(3i64.into()).unwrap();
        state.accumulate(2i64.into()).unwrap();
        assert_eq!(state.finalize(), Datum::from(3i64));
    }

    #[test]
    fn test_min() {
        let mut state = Cmp::min(0);
        state.accumulate(Datum::Null).unwrap();
        state.accumulate(1i64.into()).unwrap();
        state.accumulate(Datum::Null).unwrap();
        state.accumulate(3i64.into()).unwrap();
        state.accumulate(2i64.into()).unwrap();
        assert_eq!(state.finalize(), Datum::from(1i64));
    }

    #[test]
    fn mixed_types_produces_error() {
        let mut state = Cmp::max(0);
        state.accumulate(1i64.into()).unwrap();
        assert_matches!(state.accumulate(1f64.into()), Err(_));
    }
}
