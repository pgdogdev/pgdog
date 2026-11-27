use super::{Error, checked_add_assign};
use crate::net::messages::Datum;
use pgdog_postgres_types::{DataType, Error as TypeError, Numeric};
use rust_decimal::prelude::*;

/// Merges the shards' `avg(x)` the way Postgres combines partial `avg()`
/// states in a parallel query: it adds up the sums and counts and divides
/// once.
///
/// `avg(real)` and `avg(double precision)` are the exception. Postgres adds
/// those up in `double precision`, but a shard's `sum(real)` is `real`, so
/// the shards' averages are weighted by their counts instead.
#[derive(Debug)]
pub(super) struct Avg {
    pub(super) column: usize,
    pub(super) count_helper: usize,
    pub(super) sum_helper: usize,
    count: i64,
    sum: Datum,
    /// The shards' `double precision` averages and their counts.
    float_averages: Vec<(f64, i64)>,
}

impl Avg {
    pub(super) fn new(column: usize, count_helper: usize, sum_helper: usize) -> Self {
        Self {
            column,
            count_helper,
            sum_helper,
            count: 0,
            sum: Datum::Null,
            float_averages: Vec::new(),
        }
    }

    /// Add one shard's `avg(x)`, `count(x)` and `sum(x)`.
    pub(super) fn accumulate(&mut self, avg: Datum, count: i64, sum: Datum) -> Result<(), Error> {
        self.count += count;
        match avg {
            Datum::Double(avg) => self.float_averages.push((avg.0, count)),
            // `sum(smallint)` and `sum(integer)` are `bigint`, but their
            // average is `numeric`.
            _ => {
                let sum = match sum {
                    Datum::SmallInt(_) | Datum::Integer(_) | Datum::Bigint(_) => {
                        Datum::Numeric(Numeric::from(sum.as_i64()?))
                    }
                    sum => sum,
                };
                checked_add_assign(&mut self.sum, sum)?;
            }
        }
        Ok(())
    }

    pub(super) fn finalize(self) -> Result<Datum, Error> {
        if self.count == 0 {
            return Ok(Datum::Null);
        }
        if self.float_averages.is_empty() {
            return divide(self.sum, self.count);
        }

        let total = Decimal::from(self.count);
        let avg = self
            .float_averages
            .into_iter()
            .fold(0.0, |acc, (avg, count)| {
                avg.mul_add((Decimal::from(count) / total).as_f64(), acc)
            });
        Ok(Datum::from(avg))
    }
}

/// Average of `count` values from their sum, like Postgres's `avg()` final
/// functions ([`int8_avg()`], [`numeric_avg()`]).
///
/// [`int8_avg()`]: https://github.com/postgres/postgres/blob/REL_18_3/src/backend/utils/adt/numeric.c#L6918-L6939
/// [`numeric_avg()`]: https://github.com/postgres/postgres/blob/REL_18_3/src/backend/utils/adt/numeric.c#L6246-L6279
#[allow(clippy::unnecessary_lazy_evaluations)]
pub(super) fn divide(mut sum: Datum, count: i64) -> Result<Datum, Error> {
    match &mut sum {
        Datum::Null => {}
        Datum::Numeric(n) => {
            // NaN and infinity stay as they are.
            if let Some(d) = n.as_decimal_mut() {
                let count = Decimal::from(count);
                let scale = div_scale(d, &count);
                *d = d
                    .checked_div(count)
                    .ok_or_else(|| TypeError::NumericOutOfRange(DataType::Numeric))?;
                d.rescale(scale);
            }
        }
        Datum::Double(f) => f.0 /= count as f64,
        other => {
            return Err(Error::UnsupportedAggregation {
                function: String::from("avg"),
                reason: format!("avg({}) is not yet supported", other.data_type()),
            });
        }
    }
    Ok(sum)
}

/// Scale Postgres gives `dividend / divisor`: enough for at least 16
/// significant digits, and no less than either input's scale
/// ([`select_div_scale()`]).
///
/// [`select_div_scale()`]: https://github.com/postgres/postgres/blob/REL_18_3/src/backend/utils/adt/numeric.c#L10134-L10195
fn div_scale(dividend: &Decimal, divisor: &Decimal) -> u32 {
    /// [`NUMERIC_MIN_SIG_DIGITS`](https://github.com/postgres/postgres/blob/REL_18_3/src/include/utils/numeric.h#L50)
    const NUMERIC_MIN_SIG_DIGITS: i32 = 16;
    /// [`DEC_DIGITS`](https://github.com/postgres/postgres/blob/REL_18_3/src/backend/utils/adt/numeric.c#L96-L98)
    const DEC_DIGITS: i32 = 4;
    /// `Decimal` holds up to 28 decimal places; Postgres up to 1,000.
    const MAX_SCALE: i32 = 28;

    let (weight1, first_digit1) = base_10000_lead(dividend);
    let (weight2, first_digit2) = base_10000_lead(divisor);

    // When the first digits are equal, Postgres assumes the dividend is the
    // smaller number.
    let mut quotient_weight = weight1 - weight2;
    if first_digit1 <= first_digit2 {
        quotient_weight -= 1;
    }

    (NUMERIC_MIN_SIG_DIGITS - quotient_weight * DEC_DIGITS)
        .max(dividend.scale() as i32)
        .max(divisor.scale() as i32)
        .clamp(0, MAX_SCALE) as u32
}

/// Weight and first digit of `value` in base 10,000, the base Postgres
/// stores `numeric` digits in. Zero is `(0, 0)`, as in `select_div_scale()`.
fn base_10000_lead(value: &Decimal) -> (i32, u128) {
    if value.is_zero() {
        return (0, 0);
    }

    let mantissa = value.mantissa().unsigned_abs();
    let digits = mantissa.ilog10() as i32 + 1;
    // Power of ten of the leading decimal digit.
    let exponent = digits - value.scale() as i32 - 1;
    let weight = exponent.div_euclid(4);
    // Decimal digits in the leading base-10,000 digit, from 1 to 4.
    let lead_digits = exponent - weight * 4 + 1;
    let shift = digits - lead_digits;
    let first_digit = if shift >= 0 {
        mantissa / 10u128.pow(shift as u32)
    } else {
        mantissa * 10u128.pow(shift.unsigned_abs())
    };

    (weight, first_digit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::messages::Format;
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

    /// `avg(x)` over `(avg, count, sum)` from each shard, as Postgres prints it.
    /// A `numeric` average is computed from the sums, so the shard averages
    /// only matter for floats.
    fn merged(shards: &[(Datum, i64, Datum)]) -> String {
        let mut state = Avg::new(0, 1, 2);
        for (avg, count, sum) in shards {
            state.accumulate(avg.clone(), *count, sum.clone()).unwrap();
        }
        let result = state.finalize().unwrap();
        String::from_utf8(result.encode(Format::Text).unwrap().to_vec()).unwrap()
    }

    #[test]
    fn merges_shard_sums_not_rounded_averages() {
        // Shard 0 has 0, 0, 1 and shard 1 has 1, 1, 1, 1, 1, 1 and 2 or 3.
        // Weighting the rounded shard averages gave 0.90000000000000003000
        // and 0.99999999999999999000.
        let shard0 = (dec!(0.33333333333333333333).into(), 3, Datum::Bigint(1));
        for (avg, sum, expected) in [
            (dec!(1.1428571428571429), 8, "0.90000000000000000000"),
            (dec!(1.2857142857142857), 9, "1.00000000000000000000"),
        ] {
            let shard1 = (avg.into(), 7, Datum::Bigint(sum));
            assert_eq!(merged(&[shard0.clone(), shard1]), expected);
        }
    }

    #[test]
    fn avg_with_double_data() {
        let mut state = Avg::new(0, 1, 2);
        for &(avg, count) in TEST_DATA {
            state
                .accumulate(avg.into(), count, (avg * count as f64).into())
                .unwrap();
        }
        assert_eq!(
            state.finalize().unwrap(),
            Datum::from(36_758.559_474_168_589)
        );
    }

    #[test]
    fn numeric_sum_out_of_range_errors() {
        let mut state = Avg::new(0, 1, 2);
        state
            .accumulate(Decimal::MAX.into(), 2, Decimal::MAX.into())
            .unwrap();
        assert!(
            state
                .accumulate(Decimal::MAX.into(), 3, Decimal::MAX.into())
                .is_err()
        );
    }

    #[test]
    fn avg_with_very_large_double() {
        let mut state = Avg::new(0, 1, 2);
        state
            .accumulate(f64::MAX.into(), 2, f64::INFINITY.into())
            .unwrap();
        state
            .accumulate(f64::MAX.into(), 3, f64::INFINITY.into())
            .unwrap();
        assert_eq!(state.finalize().unwrap(), Datum::from(f64::MAX));
    }

    #[test]
    fn avg_with_nulls() {
        let mut state = Avg::new(0, 1, 2);
        state.accumulate(1.0.into(), 1, 1.0.into()).unwrap();
        state.accumulate(Datum::Null, 0, Datum::Null).unwrap();
        assert_eq!(state.finalize().unwrap(), Datum::from(1.0));
    }

    #[test]
    fn avg_of_integers_keeps_fractional_scale() {
        let shards = [297, 301, 305, 295].map(|sum| (Datum::Null, 100, Datum::Bigint(sum)));
        assert_eq!(merged(&shards), "2.9950000000000000");
    }

    #[test]
    fn large_integer_avg_uses_zero_scale() {
        let shards = [
            (Datum::Null, 6, dec!(5100).into()),
            (Datum::Null, 9, dec!(435_090_932_899_645_851).into()),
        ];
        assert_eq!(merged(&shards), "29006062193310063");
    }

    #[test]
    fn scale_follows_postgres_division() {
        // SELECT avg(0) FROM generate_series(1, 10000)
        let zeros = [
            (Datum::Null, 4000, Datum::Bigint(0)),
            (Datum::Null, 6000, Datum::Bigint(0)),
        ];
        assert_eq!(merged(&zeros), "0.000000000000000000000000");

        // The input's scale wins when it's larger.
        let value = dec!(1.123456789012345678901);
        let shards = [
            (Datum::Null, 1, value.into()),
            (Datum::Null, 1, value.into()),
        ];
        assert_eq!(merged(&shards), "1.123456789012345678901");
    }

    #[test]
    fn avg_numeric_nan_returns_nan() {
        let shards = [
            (Datum::Null, 1, Datum::Numeric(Numeric::nan())),
            (Datum::Null, 1_000_000, dec!(1_000_000).into()),
        ];
        assert_eq!(merged(&shards), "NaN");
    }

    #[test]
    fn opposite_infinities_average_to_nan() {
        let shards = [
            (Datum::Null, 1, Datum::Numeric(Numeric::infinity())),
            (Datum::Null, 1, Datum::Numeric(Numeric::negative_infinity())),
        ];
        assert_eq!(merged(&shards), "NaN");
    }

    #[test]
    fn avg_double_nan_returns_nan() {
        let mut state = Avg::new(0, 1, 2);
        state
            .accumulate(f64::NAN.into(), 1, f64::NAN.into())
            .unwrap();
        state
            .accumulate(1.0.into(), 1_000_000, 1_000_000.0.into())
            .unwrap();
        assert_eq!(state.finalize().unwrap(), Datum::from(f64::NAN));
    }

    #[test]
    fn avg_with_only_nulls_returns_null() {
        let mut state = Avg::new(0, 1, 2);
        state.accumulate(Datum::Null, 0, Datum::Null).unwrap();
        assert_eq!(state.finalize().unwrap(), Datum::Null);
    }
}
