use super::{sum::Sum, von};
use pgdog_postgres_types::{DataType, Datum, Error, Numeric};
use rust_decimal::prelude::*;

#[derive(Debug)]
pub(super) struct Variance {
    sumsq: Sum,
    sum: Sum,
    count: von::Count,
    sample: bool,
    sqrt: bool,
}

// FIXME(sage): It should be harder to pass the helpers in the wrong order
impl Variance {
    pub(super) fn new(
        sumsq_col: usize,
        sum_col: usize,
        count_col: usize,
        sample: bool,
        sqrt: bool,
    ) -> Self {
        Self {
            sumsq: Sum::new(sumsq_col),
            sum: Sum::new(sum_col),
            count: von::Count::new(count_col),
            sample,
            sqrt,
        }
    }

    pub(super) fn sumsq_col(&self) -> usize {
        self.sumsq.column
    }

    pub(super) fn sum_col(&self) -> usize {
        self.sum.column
    }

    pub(super) fn count_col(&self) -> usize {
        self.count.column
    }

    pub(super) fn accumulate(
        &mut self,
        sumsq_value: Datum,
        sum_value: Datum,
        count_value: Datum,
    ) -> Result<(), Error> {
        self.sumsq.accumulate(sumsq_value)?;
        self.sum.accumulate(sum_value)?;
        self.count.accumulate(count_value)?;
        Ok(())
    }

    pub(super) fn finalize(self) -> Result<Datum, Error> {
        // Naive algorithm for computing variance without mean
        // σ² = (∑xᵢ² − (∑xᵢ)²/N) / N
        // ref https://open.maricopa.edu/haasstatistics/chapter/4-4-calculating-variance/
        let sumsq = self.sumsq.finalize();
        let sum = self.sum.finalize();
        let Some(count) = self.count.finalize_i64() else {
            return Ok(Datum::Null);
        };

        match (sumsq, sum) {
            (Datum::Numeric(sumsq), Datum::Numeric(sum)) => {
                let (Some(sumsq), Some(sum)) = (sumsq.as_decimal(), sum.as_decimal()) else {
                    return Ok(Datum::Numeric(Numeric::nan()));
                };
                Ok(compute_variance(*sumsq, *sum, count, self.sample, self.sqrt).into())
            }
            // FIXME(sage): We lose precision by using Double for sum that
            // single shard wouldn't.  If we store the expected data type from
            // the schema we can have the helper use numeric
            (Datum::Numeric(sumsq), Datum::Double(sum)) => {
                let (Some(sumsq), Some(sum)) = (sumsq.as_decimal(), Decimal::from_f64(sum.0))
                else {
                    return Ok(f64::NAN.into());
                };
                Ok(compute_variance(*sumsq, sum, count, self.sample, self.sqrt)
                    .as_f64()
                    .into())
            }
            // Variance functions only return double or numeric, but sum
            // can return float and int8
            (Datum::Numeric(sumsq), Datum::Bigint(sum)) => {
                let Some(sumsq) = sumsq.as_decimal() else {
                    return Ok(Datum::Numeric(Numeric::nan()));
                };
                let sum = Decimal::from(sum);
                Ok(compute_variance(*sumsq, sum, count, self.sample, self.sqrt).into())
            }
            (Datum::Numeric(sumsq), Datum::Float(sum)) => {
                let (Some(sumsq), Some(sum)) = (sumsq.as_decimal(), Decimal::from_f32(sum.0))
                else {
                    return Ok(f64::NAN.into());
                };
                Ok(compute_variance(*sumsq, sum, count, self.sample, self.sqrt)
                    .as_f64()
                    .into())
            }
            // We're manually casting sumsq to be numeric,
            // And the only types PG returns for sum on types this function
            // supports are int8, numeric, real, and double precision
            (_, rhs) => Err(Error::IncompatibleTypes(DataType::Numeric, rhs.data_type())),
        }
    }
}

fn compute_variance(sumsq: Decimal, sum: Decimal, count: i64, sample: bool, sqrt: bool) -> Decimal {
    let sumsq = sumsq.normalize();
    let sum = sum.normalize();
    let count = Decimal::from(count);
    let mut result = (sumsq - sum * sum / count) / count;
    if sample {
        result *= (count / (count - dec!(1)));
    }
    if sqrt {
        result = result.sqrt().unwrap_or(Decimal::ZERO);
    }
    result
}

#[test]
fn test_stddev_samp() {
    let mut state = Variance::new(0, 0, 0, true, true);
    state
        .accumulate(dec!(296).into(), 24f64.into(), 2i64.into())
        .unwrap();
    state
        .accumulate(dec!(808).into(), 40f64.into(), 2i64.into())
        .unwrap();
    assert_eq!(Datum::from(5.163977794943222f64), state.finalize().unwrap());
}

#[test]
fn test_var_pop() {
    let mut state = Variance::new(0, 0, 0, false, false);
    state
        .accumulate(dec!(296).into(), 24f64.into(), 2i64.into())
        .unwrap();
    state
        .accumulate(dec!(808).into(), 40f64.into(), 2i64.into())
        .unwrap();
    assert_eq!(Datum::from(20.0f64), state.finalize().unwrap());
}
