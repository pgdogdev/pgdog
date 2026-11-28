use super::{Error, checked_add_assign};
use crate::net::{Decoder, messages::Datum};
use pgdog_postgres_types::{DataType, Error as TypeError, Numeric};
use rust_decimal::prelude::*;

/// Merges the shards' `avg(x)` the way Postgres combines partial `avg()`
/// states in a parallel query: it adds up the sums and counts and divides
/// once.
///
/// Postgres adds up `avg(real)` and `avg(double precision)` in `double
/// precision` ([`float8_combine()`], [`float8_avg()`]), but a shard's
/// `sum(real)` is `real`. For those, a shard's sum is its average times its
/// count.
#[derive(Debug)]
pub(super) struct Avg {
    pub(super) column: usize,
    pub(super) count_helper: usize,
    pub(super) sum_helper: usize,
    count: i64,
    sum: Datum,
}

impl Avg {
    pub(super) fn new(column: usize, count_helper: usize, sum_helper: usize) -> Self {
        Self {
            column,
            count_helper,
            sum_helper,
            count: 0,
            sum: Datum::Null,
        }
    }

    /// Column to read from each shard: the average if it's `double
    /// precision`, the sum otherwise.
    pub(super) fn value_column(&self, decoder: &Decoder) -> usize {
        let field = decoder.row_description().field(self.column);
        match field.map(|field| field.data_type()) {
            Some(DataType::DoublePrecision) => self.column,
            _ => self.sum_helper,
        }
    }

    /// Add one shard's count and the value from [`Self::value_column`].
    pub(super) fn accumulate(&mut self, value: Datum, count: i64) -> Result<(), Error> {
        self.count += count;
        match value {
            Datum::Double(avg) => {
                let total = match self.sum {
                    Datum::Double(total) => total.0,
                    _ => 0.0,
                };
                self.sum = float8_pl(total, avg.0 * count as f64)?.into();
            }
            // `sum(smallint)` and `sum(integer)` are `bigint`, but their
            // average is `numeric`.
            Datum::SmallInt(_) | Datum::Integer(_) | Datum::Bigint(_) => {
                let sum = Numeric::from(value.as_i64()?);
                checked_add_assign(&mut self.sum, Datum::Numeric(sum))?;
            }
            sum => checked_add_assign(&mut self.sum, sum)?,
        }
        Ok(())
    }

    pub(super) fn finalize(self) -> Result<Datum, Error> {
        if self.count == 0 {
            return Ok(Datum::Null);
        }
        divide(self.sum, self.count)
    }
}

fn float8_pl(a: f64, b: f64) -> Result<f64, TypeError> {
    let sum = a + b;
    if sum.is_infinite() && a.is_finite() && b.is_finite() {
        return Err(TypeError::NumericOutOfRange(DataType::DoublePrecision));
    }
    Ok(sum)
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

    fn merged(shards: &[(Datum, i64)]) -> String {
        let mut state = Avg::new(0, 1, 2);
        for (sum, count) in shards {
            state.accumulate(sum.clone(), *count).unwrap();
        }
        let result = state.finalize().unwrap();
        String::from_utf8(result.encode(Format::Text).unwrap().to_vec()).unwrap()
    }

    #[test]
    fn merges_shard_sums_not_rounded_averages() {
        // Shard 0 has 0, 0, 1 and shard 1 has 1, 1, 1, 1, 1, 1 and 2 or 3.
        // Weighting the rounded shard averages gave 0.90000000000000003000
        // and 0.99999999999999999000.
        for (sum, expected) in [(8, "0.90000000000000000000"), (9, "1.00000000000000000000")] {
            let shards = [(Datum::Bigint(1), 3), (Datum::Bigint(sum), 7)];
            assert_eq!(merged(&shards), expected);
        }
    }

    #[test]
    fn avg_with_double_data() {
        let mut state = Avg::new(0, 1, 2);
        for &(avg, count) in TEST_DATA {
            state.accumulate(avg.into(), count).unwrap();
        }
        assert_eq!(
            state.finalize().unwrap(),
            Datum::from(36_758.559_474_168_589)
        );
    }

    #[test]
    fn numeric_sum_out_of_range_errors() {
        let mut state = Avg::new(0, 1, 2);
        state.accumulate(Decimal::MAX.into(), 2).unwrap();
        assert!(state.accumulate(Decimal::MAX.into(), 3).is_err());
    }

    #[test]
    fn double_sum_overflow_errors() {
        let mut state = Avg::new(0, 1, 2);
        state.accumulate(f64::MAX.into(), 1).unwrap();
        assert!(state.accumulate(f64::MAX.into(), 1).is_err());

        // Infinity isn't an overflow.
        let mut state = Avg::new(0, 1, 2);
        state.accumulate(f64::INFINITY.into(), 1).unwrap();
        state.accumulate(1.0.into(), 1).unwrap();
        assert_eq!(state.finalize().unwrap(), Datum::from(f64::INFINITY));
    }

    #[test]
    fn avg_with_nulls() {
        let mut state = Avg::new(0, 1, 2);
        state.accumulate(1.0.into(), 1).unwrap();
        state.accumulate(Datum::Null, 0).unwrap();
        assert_eq!(state.finalize().unwrap(), Datum::from(1.0));
    }

    #[test]
    fn avg_of_integers_keeps_fractional_scale() {
        let shards = [297, 301, 305, 295].map(|sum| (Datum::Bigint(sum), 100));
        assert_eq!(merged(&shards), "2.9950000000000000");
    }

    #[test]
    fn large_integer_avg_uses_zero_scale() {
        let shards = [
            (dec!(5100).into(), 6),
            (dec!(435_090_932_899_645_851).into(), 9),
        ];
        assert_eq!(merged(&shards), "29006062193310063");
    }

    #[test]
    fn scale_follows_postgres_division() {
        // SELECT avg(0) FROM generate_series(1, 10000)
        let zeros = [(Datum::Bigint(0), 4000), (Datum::Bigint(0), 6000)];
        assert_eq!(merged(&zeros), "0.000000000000000000000000");

        // The input's scale wins when it's larger.
        let value = dec!(1.123456789012345678901);
        let shards = [(value.into(), 1), (value.into(), 1)];
        assert_eq!(merged(&shards), "1.123456789012345678901");
    }

    #[test]
    fn avg_numeric_nan_returns_nan() {
        let shards = [
            (Datum::Numeric(Numeric::nan()), 1),
            (dec!(1_000_000).into(), 1_000_000),
        ];
        assert_eq!(merged(&shards), "NaN");
    }

    #[test]
    fn opposite_infinities_average_to_nan() {
        let shards = [
            (Datum::Numeric(Numeric::infinity()), 1),
            (Datum::Numeric(Numeric::negative_infinity()), 1),
        ];
        assert_eq!(merged(&shards), "NaN");
    }

    #[test]
    fn avg_double_nan_returns_nan() {
        let mut state = Avg::new(0, 1, 2);
        state.accumulate(f64::NAN.into(), 1).unwrap();
        state.accumulate(1.0.into(), 1_000_000).unwrap();
        assert_eq!(state.finalize().unwrap(), Datum::from(f64::NAN));
    }

    #[test]
    fn avg_with_only_nulls_returns_null() {
        let mut state = Avg::new(0, 1, 2);
        state.accumulate(Datum::Null, 0).unwrap();
        assert_eq!(state.finalize().unwrap(), Datum::Null);
    }
}
