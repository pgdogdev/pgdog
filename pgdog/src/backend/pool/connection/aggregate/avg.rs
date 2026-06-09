use super::{Error, checked_add};
use crate::net::messages::Datum;
use rust_decimal::prelude::*;

#[derive(Debug)]
pub(crate) struct Avg {
    pub(crate) count_helper: usize,
    values_and_weights: Vec<(Datum, i64)>,
}

impl Avg {
    pub(crate) fn new(_column: usize, count_helper: usize) -> Self {
        Self {
            count_helper,
            values_and_weights: Vec::new(),
        }
    }

    pub(crate) fn accumulate(&mut self, value: Datum, count: i64) {
        self.values_and_weights.push((value, count));
    }

    pub(crate) fn finalize(self) -> Result<Datum, Error> {
        let total_count: i64 = self.values_and_weights.iter().map(|i| i.1).sum();
        let total_count = Decimal::from(total_count);

        // Ensure we limit numeric results to the number of significant digits
        // that PG would have provided
        let numeric_scale = self
            .values_and_weights
            .iter()
            .filter_map(|(value, _)| match value {
                Datum::Numeric(n) => n.as_decimal().map(|d| d.normalize().scale()),
                _ => None,
            })
            .max();

        let mut result =
            self.values_and_weights
                .into_iter()
                .fold(Ok(Datum::Null), |acc, (value, weight)| {
                    checked_mul_add(value, Decimal::from(weight) / total_count, acc?)
                });
        if let Ok(Datum::Numeric(n)) = &mut result
            && let Some(d) = n.as_decimal_mut()
            && let Some(scale) = numeric_scale
        {
            d.rescale(scale)
        }
        result
    }
}

fn checked_mul_add(lhs: Datum, mul: Decimal, rhs: Datum) -> Result<Datum, Error> {
    match (lhs, rhs) {
        (Datum::Double(lhs), Datum::Double(rhs)) => {
            Ok(Datum::from(lhs.0.mul_add(mul.as_f64(), rhs.0)))
        }
        (lhs, rhs) => checked_add(checked_mul(lhs, mul)?, rhs).map_err(Into::into),
    }
}

fn checked_mul(lhs: Datum, rhs: Decimal) -> Result<Datum, Error> {
    match lhs {
        Datum::Numeric(i) => Ok(Datum::Numeric(i * rhs)),
        Datum::Double(f) => Ok(Datum::Double(f * rhs.as_f64())),
        Datum::Null if rhs.is_zero() => Ok(Datum::Null),
        Datum::Null => Err(Error::UnsupportedAggregation {
            function: String::from("avg"),
            reason: String::from(
                "Received NULL as a result for avg from a shard with a count \
                other than 0. This should be impossible",
            ),
        }),
        Datum::Interval(..) => Err(Error::UnsupportedAggregation {
            function: String::from("avg"),
            reason: String::from("avg(interval) is not yet supported"),
        }),
        Datum::Unknown(..) => Err(Error::UnsupportedAggregation {
            function: String::from("avg"),
            reason: String::from("avg for extension types is not supported"),
        }),
        datum => Err(Error::UnsupportedAggregation {
            function: String::from("avg"),
            reason: format!(
                "Received type {}, which is not a type postgresql should \
                return from avg. This is likely a bug in pgdog.",
                datum.data_type()
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgdog_postgres_types::Numeric;
    use rust_decimal::dec;

    // Average number of downloads each version of the top 10 crates on
    // crates.io gets per day over the last 90 days, if crates.io were
    // sharded by crate_id
    const TEST_DATA: &[(f64, i64)] = &[
        (96724.170212765957, 4089),
        (186386.974947807933, 1916),
        (33537.298768879299, 7879),
        (97637.501614987080, 3096),
        (33534.787317691820, 7885),
        (104947.738952164009, 4390),
        (60095.478251906823, 4851),
        (14353.621700556566, 18147),
        (11857.550375469337, 28764),
        (42577.351057747284, 6996),
    ];

    #[test]
    fn avg_with_numeric_data() {
        let mut state = Avg::new(0, 0);
        for &(avg, count) in TEST_DATA {
            state.accumulate(Decimal::from_f64(avg).unwrap().into(), count);
        }
        let expected: Datum = Decimal::from_f64(36758.559474168589).unwrap().into();
        assert_eq!(state.finalize().unwrap(), expected);
    }

    #[test]
    fn avg_with_double_data() {
        let mut state = Avg::new(0, 0);
        for &(avg, count) in TEST_DATA {
            state.accumulate(avg.into(), count);
        }
        assert_eq!(state.finalize().unwrap(), Datum::from(36758.559474168589));
    }

    #[test]
    fn avg_with_very_large_decimal() {
        let mut state = Avg::new(0, 0);
        state.accumulate(Decimal::MAX.into(), 2);
        state.accumulate(Decimal::MAX.into(), 3);
        assert_eq!(state.finalize().unwrap(), Datum::from(Decimal::MAX));
    }

    #[test]
    fn avg_with_very_large_double() {
        let mut state = Avg::new(0, 0);
        state.accumulate(f64::MAX.into(), 2);
        state.accumulate(f64::MAX.into(), 3);
        assert_eq!(state.finalize().unwrap(), Datum::from(f64::MAX));
    }

    #[test]
    fn avg_with_nulls() {
        let mut state = Avg::new(0, 0);
        state.accumulate(1.0.into(), 1);
        state.accumulate(Datum::Null, 0);
        assert_eq!(state.finalize().unwrap(), Datum::from(1.0));
    }

    #[test]
    fn integer_numerics_have_trailing_zeroes_removed() {
        let mut state = Avg::new(0, 0);
        state.accumulate(dec!(850.0000000000000000).into(), 6);
        state.accumulate(dec!(48343436988849539).into(), 9);
        assert_eq!(state.finalize().unwrap(), dec!(29006062193310063).into())
    }

    #[test]
    fn avg_numeric_nan_returns_nan() {
        let mut state = Avg::new(0, 0);
        state.accumulate(Datum::Numeric(Numeric::nan()), 1);
        state.accumulate(dec!(1).into(), 1_000_000);
        assert_eq!(state.finalize().unwrap(), Datum::Numeric(Numeric::nan()));
    }

    #[test]
    fn avg_double_nan_returns_nan() {
        let mut state = Avg::new(0, 0);
        state.accumulate(f64::NAN.into(), 1);
        state.accumulate(1.0.into(), 1_000_000);
        assert_eq!(state.finalize().unwrap(), Datum::from(f64::NAN));
    }
}
