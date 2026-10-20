use super::{Error, checked_add};
use crate::net::messages::Datum;
use rust_decimal::prelude::*;

#[derive(Debug)]
pub(super) struct Avg {
    pub(super) column: usize,
    pub(super) count_helper: usize,
    values_and_weights: Vec<(Datum, i64)>,
}

impl Avg {
    pub(super) fn new(column: usize, count_helper: usize) -> Self {
        Self {
            column,
            count_helper,
            values_and_weights: Vec::new(),
        }
    }

    pub(super) fn accumulate(&mut self, value: Datum, count: i64) {
        self.values_and_weights.push((value, count));
    }

    pub(super) fn finalize(self) -> Result<Datum, Error> {
        let total_count: i64 = self.values_and_weights.iter().map(|i| i.1).sum();
        let total_count = Decimal::from(total_count);

        if total_count.is_zero() {
            debug_assert!(self.values_and_weights.iter().all(|(v, _)| v.is_null()));
            return Ok(Datum::Null);
        }

        // A shard average's scale includes both the input scale and extra
        // precision selected from that shard's result magnitude. The minimum
        // observed scale is therefore the best available input-scale hint.
        let numeric_scale_hint = self
            .values_and_weights
            .iter()
            .filter_map(|(value, _)| match value {
                Datum::Numeric(n) => n.as_decimal().map(|d| d.scale()),
                _ => None,
            })
            .min();

        let mut result = self
            .values_and_weights
            .into_iter()
            .try_fold(Datum::Null, |acc, (value, weight)| {
                checked_mul_add(value, Decimal::from(weight) / total_count, acc)
            });
        if let Ok(Datum::Numeric(n)) = &mut result
            && let Some(d) = n.as_decimal_mut()
            && let Some(scale_hint) = numeric_scale_hint
        {
            d.rescale(scale_hint.max(numeric_avg_scale(d)))
        }
        result
    }
}

/// Approximate PostgreSQL's numeric division scale from the quotient.
///
/// PostgreSQL keeps at least 16 significant digits and stores numeric digits
/// in base 10,000, so display scale changes in groups of four. The exact
/// algorithm uses the numerator and denominator, which aren't available when
/// merging shard averages.
fn numeric_avg_scale(value: &Decimal) -> u32 {
    const MIN_SIGNIFICANT_DIGITS: i32 = 16;
    const NUMERIC_BASE_DIGITS: i32 = 4;
    const MAX_DECIMAL_SCALE: i32 = 28;

    if value.is_zero() {
        return 20;
    }

    let mut mantissa = value.mantissa().unsigned_abs();
    let mut digits = 1_i32;
    while mantissa >= 10 {
        mantissa /= 10;
        digits += 1;
    }

    let decimal_weight = digits - value.scale() as i32 - 1;
    let numeric_weight = decimal_weight.div_euclid(NUMERIC_BASE_DIGITS);
    (MIN_SIGNIFICANT_DIGITS - numeric_weight * NUMERIC_BASE_DIGITS).clamp(0, MAX_DECIMAL_SCALE)
        as u32
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
        (96_724.170_212_765_95, 4089),
        (186_386.974_947_807_93, 1916),
        (33_537.298_768_879_3, 7879),
        (97_637.501_614_987_08, 3096),
        (33_534.787_317_691_82, 7885),
        (104_947.738_952_164_01, 4390),
        (60_095.478_251_906_823, 4851),
        (14_353.621_700_556_566, 18147),
        (11_857.550_375_469_337, 28764),
        (42_577.351_057_747_284, 6996),
    ];

    #[test]
    fn avg_with_numeric_data() {
        let mut state = Avg::new(0, 0);
        for &(avg, count) in TEST_DATA {
            state.accumulate(Decimal::from_f64(avg).unwrap().into(), count);
        }
        let expected: Datum = dec!(36_758.559_474_168_591).into();
        assert_eq!(state.finalize().unwrap(), expected);
    }

    #[test]
    fn avg_with_double_data() {
        let mut state = Avg::new(0, 0);
        for &(avg, count) in TEST_DATA {
            state.accumulate(avg.into(), count);
        }
        assert_eq!(
            state.finalize().unwrap(),
            Datum::from(36_758.559_474_168_589)
        );
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
    fn avg_of_integers_keeps_fractional_scale() {
        let mut state = Avg::new(0, 0);
        for average in [
            dec!(2.9700000000000000),
            dec!(3.0100000000000000),
            dec!(3.0500000000000000),
            dec!(2.9500000000000000),
        ] {
            state.accumulate(average.into(), 100);
        }

        let result = state.finalize().unwrap();
        let encoded = result.encode(crate::net::messages::Format::Text).unwrap();
        assert_eq!(&encoded[..], b"2.9950000000000000");
    }

    #[test]
    fn large_integer_avg_uses_zero_scale() {
        let mut state = Avg::new(0, 0);
        state.accumulate(dec!(85_0.000_000_000_000_000_0).into(), 6);
        state.accumulate(dec!(48_343_436_988_849_539).into(), 9);

        let result = state.finalize().unwrap();
        let encoded = result.encode(crate::net::messages::Format::Text).unwrap();
        assert_eq!(&encoded[..], b"29006062193310063");
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

    #[test]
    fn avg_with_only_nulls_returns_null() {
        let mut state = Avg::new(0, 0);
        state.accumulate(Datum::Null, 0);
        assert_eq!(state.finalize().unwrap(), Datum::Null);
    }
}
