//! Aggregate buffer.

use std::collections::{HashMap, VecDeque};

use crate::{
    frontend::router::parser::{
        Aggregate, AggregateFunction, AggregateTarget, HelperKind, RewritePlan,
    },
    net::{
        messages::{
            data_types::{Double, Float, Numeric},
            DataRow, Datum,
        },
        Decoder,
    },
};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;

use super::Error;

/// GROUP BY <columns>
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Grouping {
    columns: Vec<(usize, Datum)>,
}

impl Grouping {
    fn new(row: &DataRow, group_by: &[usize], decoder: &Decoder) -> Result<Self, Error> {
        let mut columns = vec![];
        for idx in group_by {
            let column = row.get_column(*idx, decoder)?;
            if let Some(column) = column {
                columns.push((*idx, column.value));
            }
        }

        Ok(Self { columns })
    }
}

/// The aggregate accumulator.
///
/// This transforms distributed aggregate functions
/// into a single value.
#[derive(Debug)]
struct Accumulator<'a> {
    target: &'a AggregateTarget,
    datum: Datum,
    avg: Option<AvgState>,
    variance: Option<VarianceState>,
}

impl<'a> Accumulator<'a> {
    pub fn from_aggregate(
        aggregate: &'a Aggregate,
        helpers: &HashMap<(usize, bool), HelperColumns>,
    ) -> Vec<Self> {
        aggregate
            .targets()
            .iter()
            .map(|target| {
                let helper = helpers
                    .get(&(target.expr_id(), target.is_distinct()))
                    .copied()
                    .unwrap_or_default();

                let mut accumulator = match target.function() {
                    AggregateFunction::Count => Accumulator {
                        target,
                        datum: Datum::Bigint(0),
                        avg: None,
                        variance: None,
                    },
                    _ => Accumulator {
                        target,
                        datum: Datum::Null,
                        avg: None,
                        variance: None,
                    },
                };

                if matches!(target.function(), AggregateFunction::Avg) {
                    accumulator.avg = Some(AvgState::new(helper.count));
                }

                if matches!(
                    target.function(),
                    AggregateFunction::StddevPop
                        | AggregateFunction::StddevSamp
                        | AggregateFunction::VarPop
                        | AggregateFunction::VarSamp
                ) {
                    accumulator.variance = Some(VarianceState::new(
                        target.function().clone(),
                        helper,
                        target.is_distinct(),
                    ));
                }

                accumulator
            })
            .collect()
    }

    /// Transform COUNT(*), MIN, MAX, etc., from multiple shards into a single value.
    fn accumulate(&mut self, row: &DataRow, decoder: &Decoder) -> Result<bool, Error> {
        let column = row
            .get_column(self.target.column(), decoder)?
            .ok_or(Error::DecoderRowError)?;
        match self.target.function() {
            AggregateFunction::Count => {
                self.datum = self.datum.clone() + column.value;
            }
            AggregateFunction::Max => {
                if !self.datum.is_null() {
                    if self.datum < column.value {
                        self.datum = column.value;
                    }
                } else {
                    self.datum = column.value;
                }
            }
            AggregateFunction::Min => {
                if !self.datum.is_null() {
                    if self.datum > column.value {
                        self.datum = column.value;
                    }
                } else {
                    self.datum = column.value;
                }
            }
            AggregateFunction::Sum => {
                if !self.datum.is_null() {
                    self.datum = self.datum.clone() + column.value;
                } else {
                    self.datum = column.value;
                }
            }
            AggregateFunction::Avg => {
                if let Some(state) = self.avg.as_mut() {
                    if !state.supported {
                        return Ok(false);
                    }

                    let Some(count_column) = state.count_column else {
                        state.supported = false;
                        return Ok(false);
                    };

                    let count = row
                        .get_column(count_column, decoder)?
                        .ok_or(Error::DecoderRowError)?;

                    if column.value.is_null() || count.value.is_null() {
                        return Ok(true);
                    }

                    if let Some(weighted) = multiply_for_average(&column.value, &count.value) {
                        state.weighted_sum = state.weighted_sum.clone() + weighted;
                        state.total_count = state.total_count.clone() + count.value.clone();
                    } else {
                        state.supported = false;
                        return Ok(false);
                    }
                }
            }
            AggregateFunction::StddevPop
            | AggregateFunction::StddevSamp
            | AggregateFunction::VarPop
            | AggregateFunction::VarSamp => {
                if let Some(state) = self.variance.as_mut() {
                    if !state.supported {
                        return Ok(false);
                    }

                    if !state.accumulate(row, decoder)? {
                        return Ok(false);
                    }
                }
            }
        }

        Ok(true)
    }

    fn finalize(&mut self) -> Result<bool, Error> {
        if let Some(state) = self.avg.as_mut() {
            if !state.supported {
                return Ok(false);
            }

            if state.count_column.is_none() {
                return Ok(false);
            }

            if state.total_count.is_null() {
                self.datum = Datum::Null;
                return Ok(true);
            }

            if let Some(result) = divide_for_average(&state.weighted_sum, &state.total_count) {
                self.datum = result;
                return Ok(true);
            }

            return Ok(false);
        }

        if let Some(state) = self.variance.as_mut() {
            match state.finalize()? {
                Some(result) => {
                    self.datum = result;
                    return Ok(true);
                }
                None => return Ok(false),
            }
        }

        Ok(true)
    }
}

#[derive(Debug)]
struct AvgState {
    count_column: Option<usize>,
    weighted_sum: Datum,
    total_count: Datum,
    supported: bool,
}

impl AvgState {
    fn new(count_column: Option<usize>) -> Self {
        Self {
            count_column,
            weighted_sum: Datum::Null,
            total_count: Datum::Null,
            supported: count_column.is_some(),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct HelperColumns {
    count: Option<usize>,
    sum: Option<usize>,
    sumsq: Option<usize>,
}

#[derive(Debug, Clone)]
struct UnsupportedAggregate {
    function: String,
    reason: String,
}

#[derive(Debug)]
struct VarianceState {
    function: AggregateFunction,
    count_column: Option<usize>,
    sum_column: Option<usize>,
    sumsq_column: Option<usize>,
    total_count: Datum,
    total_sum: Datum,
    total_sumsq: Datum,
    supported: bool,
}

impl VarianceState {
    fn new(function: AggregateFunction, helper: HelperColumns, distinct: bool) -> Self {
        let supported =
            !distinct && helper.count.is_some() && helper.sum.is_some() && helper.sumsq.is_some();
        Self {
            function,
            count_column: helper.count,
            sum_column: helper.sum,
            sumsq_column: helper.sumsq,
            total_count: Datum::Null,
            total_sum: Datum::Null,
            total_sumsq: Datum::Null,
            supported,
        }
    }

    fn accumulate(&mut self, row: &DataRow, decoder: &Decoder) -> Result<bool, Error> {
        if !self.supported {
            return Ok(false);
        }

        let Some(count_column) = self.count_column else {
            self.supported = false;
            return Ok(false);
        };

        let count = row
            .get_column(count_column, decoder)?
            .ok_or(Error::DecoderRowError)?;

        if count.value.is_null() {
            return Ok(true);
        }

        let Some(count_i128) = datum_as_i128(&count.value) else {
            self.supported = false;
            return Ok(false);
        };

        if count_i128 == 0 {
            return Ok(true);
        }

        self.total_count = self.total_count.clone() + count.value.clone();

        let Some(sum_column) = self.sum_column else {
            self.supported = false;
            return Ok(false);
        };
        let sum = row
            .get_column(sum_column, decoder)?
            .ok_or(Error::DecoderRowError)?;
        if sum.value.is_null() {
            self.supported = false;
            return Ok(false);
        }
        self.total_sum = self.total_sum.clone() + sum.value.clone();

        let Some(sumsq_column) = self.sumsq_column else {
            self.supported = false;
            return Ok(false);
        };
        let sumsq = row
            .get_column(sumsq_column, decoder)?
            .ok_or(Error::DecoderRowError)?;
        if sumsq.value.is_null() {
            self.supported = false;
            return Ok(false);
        }
        self.total_sumsq = self.total_sumsq.clone() + sumsq.value.clone();

        Ok(true)
    }

    fn finalize(&mut self) -> Result<Option<Datum>, Error> {
        if !self.supported {
            return Ok(None);
        }

        if self.total_sum.is_null() || self.total_sumsq.is_null() {
            return Ok(Some(Datum::Null));
        }

        let Some(count) = datum_as_i128(&self.total_count) else {
            return Ok(None);
        };

        let sample = matches!(
            self.function,
            AggregateFunction::StddevSamp | AggregateFunction::VarSamp
        );

        if count == 0 {
            return Ok(Some(Datum::Null));
        }

        if sample && count <= 1 {
            return Ok(Some(Datum::Null));
        }

        let result = match (&self.total_sum, &self.total_sumsq) {
            (Datum::Double(sum), Datum::Double(sumsq)) => {
                let Some(variance) = compute_variance_double(sum.0, sumsq.0, count, sample) else {
                    return Ok(None);
                };
                match self.function {
                    AggregateFunction::StddevPop | AggregateFunction::StddevSamp => {
                        Some(Datum::Double(Double(variance.max(0.0).sqrt())))
                    }
                    AggregateFunction::VarPop | AggregateFunction::VarSamp => {
                        Some(Datum::Double(Double(variance)))
                    }
                    _ => None,
                }
            }
            (Datum::Float(sum), Datum::Float(sumsq)) => {
                let Some(variance) =
                    compute_variance_double(sum.0 as f64, sumsq.0 as f64, count, sample)
                else {
                    return Ok(None);
                };
                match self.function {
                    AggregateFunction::StddevPop | AggregateFunction::StddevSamp => {
                        Some(Datum::Float(Float(variance.max(0.0).sqrt() as f32)))
                    }
                    AggregateFunction::VarPop | AggregateFunction::VarSamp => {
                        Some(Datum::Float(Float(variance as f32)))
                    }
                    _ => None,
                }
            }
            (Datum::Numeric(sum), Datum::Numeric(sumsq)) => {
                let Some(sum_dec) = sum.as_decimal() else {
                    return Ok(None);
                };
                let Some(sumsq_dec) = sumsq.as_decimal() else {
                    return Ok(None);
                };
                let sum_dec = sum_dec.to_owned();
                let sumsq_dec = sumsq_dec.to_owned();
                let Some(variance) = compute_variance_decimal(sum_dec, sumsq_dec, count, sample)
                else {
                    return Ok(None);
                };
                match self.function {
                    AggregateFunction::StddevPop | AggregateFunction::StddevSamp => {
                        let Some(stddev) = sqrt_decimal(variance) else {
                            return Ok(None);
                        };
                        Some(Datum::Numeric(Numeric::from(stddev)))
                    }
                    AggregateFunction::VarPop | AggregateFunction::VarSamp => {
                        Some(Datum::Numeric(Numeric::from(variance)))
                    }
                    _ => None,
                }
            }
            _ => None,
        };

        Ok(result)
    }
}

#[derive(Debug)]
pub(super) struct Aggregates<'a> {
    rows: &'a VecDeque<DataRow>,
    mappings: HashMap<Grouping, Vec<Accumulator<'a>>>,
    decoder: &'a Decoder,
    aggregate: &'a Aggregate,
    helper_columns: HashMap<(usize, bool), HelperColumns>,
    merge_supported: bool,
    unsupported: Option<UnsupportedAggregate>,
}

impl<'a> Aggregates<'a> {
    pub(super) fn new(
        rows: &'a VecDeque<DataRow>,
        decoder: &'a Decoder,
        aggregate: &'a Aggregate,
        plan: &RewritePlan,
    ) -> Self {
        let mut helper_columns: HashMap<(usize, bool), HelperColumns> = HashMap::new();
        let mut unsupported: Option<UnsupportedAggregate> = None;

        for target in aggregate.targets() {
            let key = (target.expr_id(), target.is_distinct());
            match target.function() {
                AggregateFunction::Count => {
                    helper_columns.entry(key).or_default().count = Some(target.column());
                }
                AggregateFunction::Sum => {
                    helper_columns.entry(key).or_default().sum = Some(target.column());
                }
                _ => {}
            }
        }

        for helper in plan.helpers() {
            let Some(index) = decoder.rd().field_index(&helper.alias) else {
                unsupported.get_or_insert(UnsupportedAggregate {
                    function: "aggregate".to_string(),
                    reason: format!("missing helper column alias '{}'", helper.alias),
                });
                continue;
            };

            let entry = helper_columns
                .entry((helper.expr_id, helper.distinct))
                .or_default();
            match helper.kind {
                HelperKind::Count => entry.count = Some(index),
                HelperKind::Sum => entry.sum = Some(index),
                HelperKind::SumSquares => entry.sumsq = Some(index),
            }
        }

        let merge_supported = aggregate.targets().iter().all(|target| {
            let key = (target.expr_id(), target.is_distinct());
            match target.function() {
                AggregateFunction::Avg => helper_columns
                    .get(&key)
                    .and_then(|columns| columns.count)
                    .is_some(),
                AggregateFunction::StddevPop
                | AggregateFunction::StddevSamp
                | AggregateFunction::VarPop
                | AggregateFunction::VarSamp => {
                    if target.is_distinct() {
                        unsupported.get_or_insert(UnsupportedAggregate {
                            function: target.function().as_str().to_string(),
                            reason: "DISTINCT is not supported".to_string(),
                        });
                        return false;
                    }
                    helper_columns
                        .get(&key)
                        .map(|columns| {
                            columns.count.is_some()
                                && columns.sum.is_some()
                                && columns.sumsq.is_some()
                        })
                        .unwrap_or_else(|| {
                            unsupported.get_or_insert(UnsupportedAggregate {
                                function: target.function().as_str().to_string(),
                                reason: "missing helper columns".to_string(),
                            });
                            false
                        })
                }
                _ => true,
            }
        });

        Self {
            rows,
            decoder,
            mappings: HashMap::new(),
            aggregate,
            helper_columns,
            merge_supported,
            unsupported,
        }
    }

    pub(super) fn aggregate(mut self) -> Result<VecDeque<DataRow>, Error> {
        if !self.merge_supported {
            if let Some(info) = self.unsupported {
                return Err(Error::UnsupportedAggregation {
                    function: info.function,
                    reason: info.reason,
                });
            }
            return Ok(self.rows.clone());
        }

        for row in self.rows {
            let grouping = Grouping::new(row, self.aggregate.group_by(), self.decoder)?;
            let entry = self.mappings.entry(grouping).or_insert_with(|| {
                Accumulator::from_aggregate(self.aggregate, &self.helper_columns)
            });

            for aggregate in entry {
                if !aggregate.accumulate(row, self.decoder)? {
                    if let Some(info) = self.unsupported.clone() {
                        return Err(Error::UnsupportedAggregation {
                            function: info.function,
                            reason: info.reason,
                        });
                    }
                    return Ok(self.rows.clone());
                }
            }
        }

        let mut rows = VecDeque::new();
        for (grouping, accumulator) in self.mappings {
            //
            // Aggregate rules in Postgres dictate that the only
            // columns present in the row are either:
            //
            // 1. part of the GROUP BY, which means they are
            //    stored in the grouping
            // 2. are aggregate functions, which means they
            //    are stored in the accumulator
            //
            let mut row = DataRow::new();
            for (idx, datum) in grouping.columns {
                row.insert(
                    idx,
                    datum.encode(self.decoder.format(idx))?,
                    datum.is_null(),
                );
            }
            for mut acc in accumulator {
                if !acc.finalize()? {
                    if let Some(info) = self.unsupported.clone() {
                        return Err(Error::UnsupportedAggregation {
                            function: info.function,
                            reason: info.reason,
                        });
                    }
                    return Ok(self.rows.clone());
                }
                row.insert(
                    acc.target.column(),
                    acc.datum.encode(self.decoder.format(acc.target.column()))?,
                    acc.datum.is_null(),
                );
            }
            rows.push_back(row);
        }

        Ok(rows)
    }
}

fn multiply_for_average(value: &Datum, count: &Datum) -> Option<Datum> {
    let multiplier_i128 = datum_as_i128(count)?;

    match value {
        Datum::Double(double) => {
            let multiplier = multiplier_i128 as f64;
            Some(Datum::Double(Double(double.0 * multiplier)))
        }
        Datum::Float(float) => {
            let multiplier = multiplier_i128 as f64;
            Some(Datum::Float(Float((float.0 as f64 * multiplier) as f32)))
        }
        Datum::Numeric(numeric) => {
            let decimal = numeric.as_decimal()?.to_owned();
            let product = decimal * Decimal::from_i128_with_scale(multiplier_i128, 0);
            Some(Datum::Numeric(Numeric::from(product)))
        }
        _ => None,
    }
}

fn divide_for_average(sum: &Datum, count: &Datum) -> Option<Datum> {
    if sum.is_null() || count.is_null() {
        return Some(Datum::Null);
    }

    let divisor_i128 = datum_as_i128(count)?;
    if divisor_i128 == 0 {
        return Some(Datum::Null);
    }

    match sum {
        Datum::Double(double) => Some(Datum::Double(Double(double.0 / divisor_i128 as f64))),
        Datum::Float(float) => Some(Datum::Float(Float(
            (float.0 as f64 / divisor_i128 as f64) as f32,
        ))),
        Datum::Numeric(numeric) => {
            let decimal = numeric.as_decimal()?.to_owned();
            let divisor = Decimal::from_i128_with_scale(divisor_i128, 0);
            if divisor == Decimal::ZERO {
                Some(Datum::Null)
            } else {
                Some(Datum::Numeric(Numeric::from(decimal / divisor)))
            }
        }
        _ => None,
    }
}

fn datum_as_i128(datum: &Datum) -> Option<i128> {
    match datum {
        Datum::Bigint(value) => Some(*value as i128),
        Datum::Integer(value) => Some(*value as i128),
        Datum::SmallInt(value) => Some(*value as i128),
        _ => None,
    }
}

fn compute_variance_double(sum: f64, sumsq: f64, count: i128, sample: bool) -> Option<f64> {
    let n = count as f64;
    let numerator = sumsq - (sum * sum) / n;
    let denominator = if sample { (count - 1) as f64 } else { n };
    if denominator == 0.0 {
        return None;
    }
    let variance = numerator / denominator;
    Some(if variance < 0.0 { 0.0 } else { variance })
}

fn compute_variance_decimal(
    sum: Decimal,
    sumsq: Decimal,
    count: i128,
    sample: bool,
) -> Option<Decimal> {
    let n = Decimal::from_i128_with_scale(count, 0);
    let denominator = if sample {
        Decimal::from_i128_with_scale(count - 1, 0)
    } else {
        n
    };

    if denominator == Decimal::ZERO {
        return None;
    }

    let numerator = sumsq - (sum * sum) / n;

    let variance = numerator / denominator;
    Some(if variance < Decimal::ZERO {
        Decimal::ZERO
    } else {
        variance
    })
}

fn sqrt_decimal(value: Decimal) -> Option<Decimal> {
    if value < Decimal::ZERO {
        return None;
    }
    let value_f64 = value.to_f64()?;
    Decimal::from_f64(value_f64.sqrt())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::frontend::router::parser::{HelperKind, HelperMapping};
    use crate::net::{
        messages::{Field, Format, RowDescription},
        Decoder,
    };
    use std::collections::VecDeque;

    #[test]
    fn multiply_for_average_double() {
        let value = Datum::Double(Double(10.0));
        let count = Datum::Bigint(3);
        let result = multiply_for_average(&value, &count).unwrap();
        match result {
            Datum::Double(Double(v)) => assert!((v - 30.0).abs() < f64::EPSILON),
            _ => panic!("unexpected datum variant"),
        }
    }

    #[test]
    fn divide_for_average_double() {
        let sum = Datum::Double(Double(30.0));
        let count = Datum::Bigint(3);
        let result = divide_for_average(&sum, &count).unwrap();
        match result {
            Datum::Double(Double(v)) => assert!((v - 10.0).abs() < f64::EPSILON),
            _ => panic!("unexpected datum variant"),
        }
    }

    #[test]
    fn divide_for_average_zero_count() {
        let sum = Datum::Double(Double(30.0));
        let count = Datum::Bigint(0);
        let result = divide_for_average(&sum, &count).unwrap();
        assert!(matches!(result, Datum::Null));
    }

    #[test]
    fn aggregate_merges_avg_with_count() {
        let stmt = pg_query::parse("SELECT COUNT(price), AVG(price) FROM menu")
            .unwrap()
            .protobuf
            .stmts
            .first()
            .cloned()
            .unwrap();
        let aggregate = match stmt.stmt.unwrap().node.unwrap() {
            pg_query::NodeEnum::SelectStmt(stmt) => Aggregate::parse(&stmt).unwrap(),
            _ => panic!("expected select stmt"),
        };

        let rd = RowDescription::new(&[Field::bigint("count"), Field::double("avg")]);
        let decoder = Decoder::from(&rd);

        let mut rows = VecDeque::new();
        let mut shard0 = DataRow::new();
        shard0.add(2_i64).add(12.0_f64);
        rows.push_back(shard0);

        let mut shard1 = DataRow::new();
        shard1.add(3_i64).add(18.0_f64);
        rows.push_back(shard1);

        let plan = RewritePlan::new();
        let aggregates = Aggregates::new(&rows, &decoder, &aggregate, &plan);
        assert!(aggregates.merge_supported);
        let mut result = aggregates.aggregate().unwrap();

        assert_eq!(result.len(), 1);
        let row = result.pop_front().unwrap();
        let total_count = row.get::<i64>(0, Format::Text).unwrap();
        assert_eq!(total_count, 5);

        let avg = row.get::<Double>(1, Format::Text).unwrap().0;
        assert!((avg - 15.6).abs() < f64::EPSILON);
    }

    #[test]
    fn aggregate_avg_without_count_passthrough() {
        let stmt = pg_query::parse("SELECT AVG(price) FROM menu")
            .unwrap()
            .protobuf
            .stmts
            .first()
            .cloned()
            .unwrap();
        let aggregate = match stmt.stmt.unwrap().node.unwrap() {
            pg_query::NodeEnum::SelectStmt(stmt) => Aggregate::parse(&stmt).unwrap(),
            _ => panic!("expected select stmt"),
        };

        let rd = RowDescription::new(&[Field::double("avg")]);
        let decoder = Decoder::from(&rd);

        let mut rows = VecDeque::new();
        let mut shard0 = DataRow::new();
        shard0.add(12.0_f64);
        rows.push_back(shard0);
        let mut shard1 = DataRow::new();
        shard1.add(18.0_f64);
        rows.push_back(shard1);

        let plan = RewritePlan::new();
        let aggregates = Aggregates::new(&rows, &decoder, &aggregate, &plan);
        assert!(!aggregates.merge_supported);
        let result = aggregates.aggregate().unwrap();

        assert_eq!(result.len(), 2);
        let avg0 = result[0].get::<Double>(0, Format::Text).unwrap().0;
        let avg1 = result[1].get::<Double>(0, Format::Text).unwrap().0;
        assert_eq!(avg0, 12.0);
        assert_eq!(avg1, 18.0);
    }

    #[test]
    fn aggregate_avg_with_rewrite_helper() {
        let stmt = pg_query::parse("SELECT AVG(price) FROM menu")
            .unwrap()
            .protobuf
            .stmts
            .first()
            .cloned()
            .unwrap();
        let aggregate = match stmt.stmt.unwrap().node.unwrap() {
            pg_query::NodeEnum::SelectStmt(stmt) => Aggregate::parse(&stmt).unwrap(),
            _ => panic!("expected select stmt"),
        };

        let rd = RowDescription::new(&[
            Field::double("avg"),
            Field::bigint("__pgdog_count_expr0_col0"),
        ]);
        let decoder = Decoder::from(&rd);

        let mut rows = VecDeque::new();
        let mut shard0 = DataRow::new();
        shard0.add(12.0_f64).add(2_i64);
        rows.push_back(shard0);
        let mut shard1 = DataRow::new();
        shard1.add(20.0_f64).add(2_i64);
        rows.push_back(shard1);

        let mut plan = RewritePlan::new();
        plan.add_drop_column(1);
        plan.add_helper(HelperMapping {
            target_column: 0,
            helper_column: 1,
            expr_id: 0,
            distinct: false,
            kind: HelperKind::Count,
            alias: "__pgdog_count_expr0_col0".into(),
        });

        let mut result = Aggregates::new(&rows, &decoder, &aggregate, &plan)
            .aggregate()
            .unwrap();

        assert_eq!(result.len(), 1);
        let row = result.pop_front().unwrap();
        let avg = row.get::<Double>(0, Format::Text).unwrap().0;
        assert!((avg - 16.0).abs() < f64::EPSILON);
    }

    #[test]
    fn aggregate_multiple_avg_with_helpers() {
        let stmt = pg_query::parse("SELECT AVG(price), AVG(discount) FROM menu")
            .unwrap()
            .protobuf
            .stmts
            .first()
            .cloned()
            .unwrap();
        let aggregate = match stmt.stmt.unwrap().node.unwrap() {
            pg_query::NodeEnum::SelectStmt(stmt) => Aggregate::parse(&stmt).unwrap(),
            _ => panic!("expected select stmt"),
        };

        let rd = RowDescription::new(&[
            Field::double("avg_price"),
            Field::double("avg_discount"),
            Field::bigint("__pgdog_count_expr0_col0"),
            Field::bigint("__pgdog_count_expr1_col1"),
        ]);
        let decoder = Decoder::from(&rd);

        let mut rows = VecDeque::new();
        let mut shard0 = DataRow::new();
        shard0.add(12.0_f64).add(2.0_f64).add(2_i64).add(2_i64);
        rows.push_back(shard0);
        let mut shard1 = DataRow::new();
        shard1.add(20.0_f64).add(4.0_f64).add(2_i64).add(2_i64);
        rows.push_back(shard1);

        let mut plan = RewritePlan::new();
        plan.add_drop_column(2);
        plan.add_drop_column(3);
        plan.add_helper(HelperMapping {
            target_column: 0,
            helper_column: 2,
            expr_id: 0,
            distinct: false,
            kind: HelperKind::Count,
            alias: "__pgdog_count_expr0_col0".into(),
        });
        plan.add_helper(HelperMapping {
            target_column: 1,
            helper_column: 3,
            expr_id: 1,
            distinct: false,
            kind: HelperKind::Count,
            alias: "__pgdog_count_expr1_col1".into(),
        });

        let mut result = Aggregates::new(&rows, &decoder, &aggregate, &plan)
            .aggregate()
            .unwrap();

        assert_eq!(result.len(), 1);
        let row = result.pop_front().unwrap();
        let avg_price = row.get::<Double>(0, Format::Text).unwrap().0;
        let avg_discount = row.get::<Double>(1, Format::Text).unwrap().0;

        assert!((avg_price - 16.0).abs() < f64::EPSILON);
        assert!((avg_discount - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn aggregate_stddev_samp_with_helpers() {
        let stmt = pg_query::parse("SELECT STDDEV(price) FROM menu")
            .unwrap()
            .protobuf
            .stmts
            .first()
            .cloned()
            .unwrap();
        let aggregate = match stmt.stmt.unwrap().node.unwrap() {
            pg_query::NodeEnum::SelectStmt(stmt) => Aggregate::parse(&stmt).unwrap(),
            _ => panic!("expected select stmt"),
        };

        let rd = RowDescription::new(&[
            Field::double("stddev_price"),
            Field::bigint("__pgdog_count_expr0_col0"),
            Field::double("__pgdog_sum_expr0_col0"),
            Field::double("__pgdog_sumsq_expr0_col0"),
        ]);
        let decoder = Decoder::from(&rd);

        let mut rows = VecDeque::new();
        let mut shard0 = DataRow::new();
        shard0
            .add(2.8284271247461903_f64)
            .add(2_i64)
            .add(24.0_f64)
            .add(296.0_f64);
        rows.push_back(shard0);
        let mut shard1 = DataRow::new();
        shard1
            .add(2.8284271247461903_f64)
            .add(2_i64)
            .add(40.0_f64)
            .add(808.0_f64);
        rows.push_back(shard1);

        let mut plan = RewritePlan::new();
        plan.add_drop_column(1);
        plan.add_drop_column(2);
        plan.add_drop_column(3);
        plan.add_helper(HelperMapping {
            target_column: 0,
            helper_column: 1,
            expr_id: 0,
            distinct: false,
            kind: HelperKind::Count,
            alias: "__pgdog_count_expr0_col0".into(),
        });
        plan.add_helper(HelperMapping {
            target_column: 0,
            helper_column: 2,
            expr_id: 0,
            distinct: false,
            kind: HelperKind::Sum,
            alias: "__pgdog_sum_expr0_col0".into(),
        });
        plan.add_helper(HelperMapping {
            target_column: 0,
            helper_column: 3,
            expr_id: 0,
            distinct: false,
            kind: HelperKind::SumSquares,
            alias: "__pgdog_sumsq_expr0_col0".into(),
        });

        let mut result = Aggregates::new(&rows, &decoder, &aggregate, &plan)
            .aggregate()
            .unwrap();

        assert_eq!(result.len(), 1);
        let row = result.pop_front().unwrap();
        let stddev = row.get::<Double>(0, Format::Text).unwrap().0;
        assert!((stddev - 5.163977794943222).abs() < 1e-9);
    }

    #[test]
    fn aggregate_var_pop_with_helpers() {
        let stmt = pg_query::parse("SELECT VAR_POP(price) FROM menu")
            .unwrap()
            .protobuf
            .stmts
            .first()
            .cloned()
            .unwrap();
        let aggregate = match stmt.stmt.unwrap().node.unwrap() {
            pg_query::NodeEnum::SelectStmt(stmt) => Aggregate::parse(&stmt).unwrap(),
            _ => panic!("expected select stmt"),
        };

        let rd = RowDescription::new(&[
            Field::double("var_price"),
            Field::bigint("__pgdog_count_expr0_col0"),
            Field::double("__pgdog_sum_expr0_col0"),
            Field::double("__pgdog_sumsq_expr0_col0"),
        ]);
        let decoder = Decoder::from(&rd);

        let mut rows = VecDeque::new();
        let mut shard0 = DataRow::new();
        shard0.add(4.0_f64).add(2_i64).add(24.0_f64).add(296.0_f64);
        rows.push_back(shard0);
        let mut shard1 = DataRow::new();
        shard1.add(4.0_f64).add(2_i64).add(40.0_f64).add(808.0_f64);
        rows.push_back(shard1);

        let mut plan = RewritePlan::new();
        plan.add_drop_column(1);
        plan.add_drop_column(2);
        plan.add_drop_column(3);
        plan.add_helper(HelperMapping {
            target_column: 0,
            helper_column: 1,
            expr_id: 0,
            distinct: false,
            kind: HelperKind::Count,
            alias: "__pgdog_count_expr0_col0".into(),
        });
        plan.add_helper(HelperMapping {
            target_column: 0,
            helper_column: 2,
            expr_id: 0,
            distinct: false,
            kind: HelperKind::Sum,
            alias: "__pgdog_sum_expr0_col0".into(),
        });
        plan.add_helper(HelperMapping {
            target_column: 0,
            helper_column: 3,
            expr_id: 0,
            distinct: false,
            kind: HelperKind::SumSquares,
            alias: "__pgdog_sumsq_expr0_col0".into(),
        });

        let mut result = Aggregates::new(&rows, &decoder, &aggregate, &plan)
            .aggregate()
            .unwrap();

        assert_eq!(result.len(), 1);
        let row = result.pop_front().unwrap();
        let variance = row.get::<Double>(0, Format::Text).unwrap().0;
        assert!((variance - 20.0).abs() < 1e-9);
    }

    #[test]
    fn aggregate_distinct_count_not_paired() {
        let stmt = pg_query::parse("SELECT COUNT(DISTINCT price), AVG(price) FROM menu")
            .unwrap()
            .protobuf
            .stmts
            .first()
            .cloned()
            .unwrap();
        let aggregate = match stmt.stmt.unwrap().node.unwrap() {
            pg_query::NodeEnum::SelectStmt(stmt) => Aggregate::parse(&stmt).unwrap(),
            _ => panic!("expected select stmt"),
        };

        let rd = RowDescription::new(&[Field::bigint("count"), Field::double("avg")]);
        let decoder = Decoder::from(&rd);

        let mut rows = VecDeque::new();
        let mut shard0 = DataRow::new();
        shard0.add(2_i64).add(12.0_f64);
        rows.push_back(shard0);
        let mut shard1 = DataRow::new();
        shard1.add(3_i64).add(18.0_f64);
        rows.push_back(shard1);

        let plan = RewritePlan::new();
        let aggregates = Aggregates::new(&rows, &decoder, &aggregate, &plan);
        assert!(!aggregates.merge_supported); // no matching COUNT without DISTINCT
        let result = aggregates.aggregate().unwrap();

        // No merge should happen; rows should remain per shard
        assert_eq!(result.len(), 2);
        let avg0 = result[0].get::<Double>(1, Format::Text).unwrap().0;
        let avg1 = result[1].get::<Double>(1, Format::Text).unwrap().0;
        assert_eq!(avg0, 12.0);
        assert_eq!(avg1, 18.0);
    }
}
