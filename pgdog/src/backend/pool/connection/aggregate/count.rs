use super::Error;
use crate::net::messages::Datum;

#[derive(Debug)]
pub(super) struct Count {
    pub(super) column: usize,
    total: Option<i64>,
}

impl Count {
    pub(super) fn new(column: usize) -> Self {
        Self {
            column,
            total: None,
        }
    }

    pub(super) fn accumulate(&mut self, value: Datum) -> Result<(), Error> {
        if value.is_null() {
            return Ok(());
        }

        *self.total.get_or_insert(0) += value.as_i64()?;
        Ok(())
    }

    pub(super) fn finalize(self) -> Datum {
        self.total.into()
    }
}

#[test]
fn count_with_null() {
    let mut state = Count::new(0);
    state.accumulate(Datum::Null).unwrap();
    state.accumulate(1i64.into()).unwrap();
    state.accumulate(Datum::Null).unwrap();
    state.accumulate(2i64.into()).unwrap();
    assert_eq!(state.finalize(), 3i64.into());
}

#[test]
fn count_with_only_null() {
    let mut state = Count::new(0);
    state.accumulate(Datum::Null).unwrap();
    state.accumulate(Datum::Null).unwrap();
    assert_eq!(state.finalize(), Datum::Null);
}
