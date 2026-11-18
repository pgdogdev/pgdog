use super::Error;
use crate::net::messages::Datum;

#[derive(Debug, Clone, Copy)]
enum Operator {
    And,
    Or,
}

impl Operator {
    fn name(self) -> &'static str {
        match self {
            Self::And => "bit_and",
            Self::Or => "bit_or",
        }
    }
}

/// `bit_and` and `bit_or`: both are associative, so the result
/// of each shard is combined with the same operator.
#[derive(Debug)]
pub(super) struct Bit {
    pub(super) column: usize,
    operator: Operator,
    value: Datum,
}

impl Bit {
    pub(super) fn and(column: usize) -> Self {
        Self::new(column, Operator::And)
    }

    pub(super) fn or(column: usize) -> Self {
        Self::new(column, Operator::Or)
    }

    fn new(column: usize, operator: Operator) -> Self {
        Self {
            column,
            operator,
            value: Datum::Null,
        }
    }

    pub(super) fn accumulate(&mut self, value: Datum) -> Result<(), Error> {
        use Datum::*;

        macro_rules! apply {
            ($a:expr, $b:expr) => {
                match self.operator {
                    Operator::And => *$a &= $b,
                    Operator::Or => *$a |= $b,
                }
            };
        }

        match (&mut self.value, value) {
            (_, Null) => (),
            (state @ Null, value @ (Bigint(_) | Integer(_) | SmallInt(_))) => *state = value,
            (Bigint(a), Bigint(b)) => apply!(a, b),
            (Integer(a), Integer(b)) => apply!(a, b),
            (SmallInt(a), SmallInt(b)) => apply!(a, b),
            (_, value) => {
                let function = self.operator.name();
                return Err(Error::UnsupportedAggregation {
                    function: function.to_owned(),
                    reason: format!(
                        "{function}({}) is not yet supported, only smallint, integer and bigint are",
                        value.data_type()
                    ),
                });
            }
        }

        Ok(())
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
    fn bit_and_merges_shards() {
        let mut state = Bit::and(0);
        state.accumulate(Datum::Null).unwrap();
        state.accumulate(Datum::Integer(0b1110)).unwrap();
        state.accumulate(Datum::Null).unwrap();
        state.accumulate(Datum::Integer(0b0111)).unwrap();
        assert_eq!(state.finalize(), Datum::Integer(0b0110));
    }

    #[test]
    fn bit_or_merges_shards() {
        let mut state = Bit::or(0);
        state.accumulate(0b1000i64.into()).unwrap();
        state.accumulate(Datum::Null).unwrap();
        state.accumulate(0b0001i64.into()).unwrap();
        assert_eq!(state.finalize(), Datum::from(0b1001i64));
    }

    #[test]
    fn keeps_smallint_type() {
        let mut state = Bit::or(0);
        state.accumulate(Datum::SmallInt(1)).unwrap();
        state.accumulate(Datum::SmallInt(2)).unwrap();
        assert_eq!(state.finalize(), Datum::SmallInt(3));
    }

    #[test]
    fn all_null_returns_null() {
        let mut state = Bit::and(0);
        state.accumulate(Datum::Null).unwrap();
        state.accumulate(Datum::Null).unwrap();
        assert_eq!(state.finalize(), Datum::Null);
    }

    #[test]
    fn non_integer_types_produce_error() {
        let mut state = Bit::and(0);
        assert_matches!(
            state.accumulate(Datum::Text("101".into())),
            Err(Error::UnsupportedAggregation { .. })
        );

        let mut state = Bit::and(0);
        state.accumulate(1i64.into()).unwrap();
        assert_matches!(
            state.accumulate(Datum::Integer(1)),
            Err(Error::UnsupportedAggregation { .. })
        );
    }
}
