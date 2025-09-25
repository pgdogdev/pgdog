//! Aggregate buffer.

use std::collections::{HashMap, VecDeque};

use crate::{
    frontend::router::parser::{Aggregate, AggregateFunction, AggregateTarget, RewritePlan},
    net::{
        messages::{
            data_types::{Double, Float, Numeric},
            DataRow, Datum,
        },
        Decoder,
    },
};
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
}

impl<'a> Accumulator<'a> {
    pub fn from_aggregate(
        aggregate: &'a Aggregate,
        counts: &HashMap<(usize, bool), usize>,
    ) -> Vec<Self> {
        aggregate
            .targets()
            .iter()
            .map(|target| {
                let mut accumulator = match target.function() {
                    AggregateFunction::Count => Accumulator {
                        target,
                        datum: Datum::Bigint(0),
                        avg: None,
                    },
                    _ => Accumulator {
                        target,
                        datum: Datum::Null,
                        avg: None,
                    },
                };

                if matches!(target.function(), AggregateFunction::Avg) {
                    let count_column = counts
                        .get(&(target.expr_id(), target.is_distinct()))
                        .copied();
                    accumulator.avg = Some(AvgState::new(count_column));
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

#[derive(Debug)]
pub(super) struct Aggregates<'a> {
    rows: &'a VecDeque<DataRow>,
    mappings: HashMap<Grouping, Vec<Accumulator<'a>>>,
    decoder: &'a Decoder,
    aggregate: &'a Aggregate,
    count_columns: HashMap<(usize, bool), usize>,
    avg_supported: bool,
}

impl<'a> Aggregates<'a> {
    pub(super) fn new(
        rows: &'a VecDeque<DataRow>,
        decoder: &'a Decoder,
        aggregate: &'a Aggregate,
        plan: &RewritePlan,
    ) -> Self {
        let mut count_columns = aggregate
            .targets()
            .iter()
            .filter_map(|target| {
                if matches!(target.function(), AggregateFunction::Count) {
                    Some(((target.expr_id(), target.is_distinct()), target.column()))
                } else {
                    None
                }
            })
            .collect::<HashMap<_, _>>();

        for helper in plan.helpers() {
            count_columns
                .entry((helper.expr_id, helper.distinct))
                .or_insert(helper.helper_column);
        }

        let avg_supported = aggregate
            .targets()
            .iter()
            .filter(|target| matches!(target.function(), AggregateFunction::Avg))
            .all(|target| count_columns.contains_key(&(target.expr_id(), target.is_distinct())));

        Self {
            rows,
            decoder,
            mappings: HashMap::new(),
            aggregate,
            count_columns,
            avg_supported,
        }
    }

    pub(super) fn aggregate(mut self) -> Result<VecDeque<DataRow>, Error> {
        if !self.avg_supported {
            return Ok(self.rows.clone());
        }

        for row in self.rows {
            let grouping = Grouping::new(row, self.aggregate.group_by(), self.decoder)?;
            let entry = self.mappings.entry(grouping).or_insert_with(|| {
                Accumulator::from_aggregate(self.aggregate, &self.count_columns)
            });

            for aggregate in entry {
                if !aggregate.accumulate(row, self.decoder)? {
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::frontend::router::parser::HelperMapping;
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
        assert!(aggregates.avg_supported);
        assert_eq!(aggregates.count_columns.len(), 1);
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
        assert!(!aggregates.avg_supported);
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

        let rd = RowDescription::new(&[Field::double("avg"), Field::bigint("__pgdog_count")]);
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
            avg_column: 0,
            helper_column: 1,
            expr_id: 0,
            distinct: false,
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
            Field::bigint("__pgdog_count_2"),
            Field::bigint("__pgdog_count_3"),
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
            avg_column: 0,
            helper_column: 2,
            expr_id: 0,
            distinct: false,
        });
        plan.add_helper(HelperMapping {
            avg_column: 1,
            helper_column: 3,
            expr_id: 1,
            distinct: false,
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
        assert!(!aggregates.avg_supported); // no matching COUNT without DISTINCT
        let result = aggregates.aggregate().unwrap();

        // No merge should happen; rows should remain per shard
        assert_eq!(result.len(), 2);
        let avg0 = result[0].get::<Double>(1, Format::Text).unwrap().0;
        let avg1 = result[1].get::<Double>(1, Format::Text).unwrap().0;
        assert_eq!(avg0, 12.0);
        assert_eq!(avg1, 18.0);
    }
}
