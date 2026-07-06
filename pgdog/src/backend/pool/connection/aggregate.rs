//! Aggregate buffer.

use std::collections::{HashMap, VecDeque, hash_map::Entry};
use std::mem;

use crate::{
    frontend::router::parser::{
        Aggregate, AggregateFunction, AggregateTarget,
        rewrite::statement::aggregate::{AggregateRewritePlan, HelperKind},
    },
    net::{
        Decoder,
        messages::{DataRow, Datum},
    },
};
use pgdog_postgres_types::Error as TypeError;

use super::Error;

mod avg;
mod cmp;
mod sum;
mod variance;
#[path = "aggregate/count.rs"]
mod von;

/// GROUP BY <columns>
#[derive(Hash, PartialEq, Eq, Debug)]
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
    state: State,
}

impl<'a> Accumulator<'a> {
    pub fn from_aggregate(
        aggregate: &'a Aggregate,
        helpers: &HashMap<usize, HelperColumns>,
    ) -> Result<Vec<Self>, Error> {
        aggregate
            .targets()
            .iter()
            .map(|target| {
                let helper = helpers.get(&target.column()).copied().unwrap_or_default();

                let accumulator = Accumulator {
                    target,
                    state: State::new(target, helper)?,
                };

                Ok(accumulator)
            })
            .collect()
    }

    /// Transform COUNT(*), MIN, MAX, etc., from multiple shards into a single value.
    fn accumulate(&mut self, row: &DataRow, decoder: &Decoder) -> Result<(), Error> {
        self.state.accumulate(row, decoder)
    }

    fn finalize(self) -> Result<Datum, Error> {
        self.state.finalize()
    }
}

#[derive(Debug)]
enum State {
    Avg(avg::Avg),
    Cmp(cmp::Cmp),
    /// Ah ah aaaaaaah
    Count(von::Count),
    Sum(sum::Sum),
    Variance(variance::Variance),
}

impl State {
    /// Construct a new aggregate state based on the function provided
    /// Errors if the function is not one we support.
    fn new(target: &AggregateTarget, helper: HelperColumns) -> Result<Self, Error> {
        match (target.function(), target.is_distinct()) {
            (AggregateFunction::Avg, false) => Ok(Self::Avg(avg::Avg::new(
                target.column(),
                helper.count.ok_or_else(|| Error::UnsupportedAggregation {
                    function: String::from("avg"),
                    reason: String::from(
                        "internal count helper was missing (this is a bug in pgdog)",
                    ),
                })?,
            ))),
            (AggregateFunction::Count, false) => Ok(Self::Count(von::Count::new(target.column()))),
            (AggregateFunction::Max, _) => Ok(Self::Cmp(cmp::Cmp::max(target.column()))),
            (AggregateFunction::Min, _) => Ok(Self::Cmp(cmp::Cmp::min(target.column()))),
            (AggregateFunction::Sum, false) => Ok(Self::Sum(sum::Sum::new(target.column()))),
            (AggregateFunction::Sum, true) => Err(Error::UnsupportedAggregation {
                function: String::from("sum"),
                reason: String::from("sum(DISTINCT ...) is not yet supported"),
            }),
            (
                f @ (AggregateFunction::VarPop
                | AggregateFunction::VarSamp
                | AggregateFunction::StddevPop
                | AggregateFunction::StddevSamp),
                false,
            ) => {
                let sumsq_col = helper.sumsq.ok_or_else(|| Error::UnsupportedAggregation {
                    function: f.to_string(),
                    reason: String::from(
                        "internal count helper was missing (this is a bug in pgdog)",
                    ),
                })?;
                let sum_col = helper.sum.ok_or_else(|| Error::UnsupportedAggregation {
                    function: f.to_string(),
                    reason: String::from(
                        "internal count helper was missing (this is a bug in pgdog)",
                    ),
                })?;
                let count_col = helper.count.ok_or_else(|| Error::UnsupportedAggregation {
                    function: f.to_string(),
                    reason: String::from(
                        "internal count helper was missing (this is a bug in pgdog)",
                    ),
                })?;
                let sample = matches!(
                    f,
                    AggregateFunction::VarSamp | AggregateFunction::StddevSamp
                );
                let sqrt = matches!(
                    f,
                    AggregateFunction::StddevPop | AggregateFunction::StddevSamp
                );
                Ok(Self::Variance(variance::Variance::new(
                    sumsq_col, sum_col, count_col, sample, sqrt,
                )))
            }
            (
                f @ (AggregateFunction::Avg
                | AggregateFunction::Count
                | AggregateFunction::StddevPop
                | AggregateFunction::StddevSamp
                | AggregateFunction::VarPop
                | AggregateFunction::VarSamp),
                true,
            ) => Err(Error::UnsupportedAggregation {
                function: f.to_string(),
                reason: format!("{f}(DISTINCT ...) is not yet supported"),
            }),
            (AggregateFunction::Unrecognized(f), _) => Err(Error::UnsupportedAggregation {
                function: f.clone(),
                reason: format!("{f}() is not yet supported"),
            }),
        }
    }

    /// Merge the result from a single shard into the accumulated state.
    fn accumulate(&mut self, row: &DataRow, decoder: &Decoder) -> Result<(), Error> {
        match self {
            State::Avg(state) => {
                let value = row.get_column_checked(state.column, decoder)?.value;
                let weight = row
                    .get_column_checked(state.count_helper, decoder)?
                    .value
                    .as_i64()?;
                state.accumulate(value, weight);
                Ok(())
            }
            State::Cmp(state) => state
                .accumulate(row.get_column_checked(state.column, decoder)?.value)
                .map_err(Into::into),
            State::Count(state) => state
                .accumulate(row.get_column_checked(state.column, decoder)?.value)
                .map_err(Into::into),
            State::Sum(state) => state
                .accumulate(row.get_column_checked(state.column, decoder)?.value)
                .map_err(Into::into),
            State::Variance(state) => {
                let sumsq = row.get_column_checked(state.sumsq_col(), decoder)?.value;
                let sum = row.get_column_checked(state.sum_col(), decoder)?.value;
                let count = row.get_column_checked(state.count_col(), decoder)?.value;
                state.accumulate(sumsq, sum, count).map_err(Into::into)
            }
        }
    }

    /// Perform any final work needed and compute the result of the function
    fn finalize(self) -> Result<Datum, Error> {
        match self {
            State::Avg(state) => Ok(state.finalize()?),
            State::Cmp(state) => Ok(state.finalize()),
            State::Count(state) => Ok(state.finalize()),
            State::Sum(state) => Ok(state.finalize()),
            State::Variance(state) => Ok(state.finalize()?),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct HelperColumns {
    count: Option<usize>,
    sum: Option<usize>,
    sumsq: Option<usize>,
}

#[derive(Debug)]
pub(super) struct Aggregates<'a> {
    rows: &'a VecDeque<DataRow>,
    mappings: HashMap<Grouping, Vec<Accumulator<'a>>>,
    decoder: &'a Decoder,
    aggregate: &'a Aggregate,
    helper_columns: HashMap<usize, HelperColumns>,
}

impl<'a> Aggregates<'a> {
    pub(super) fn new(
        rows: &'a VecDeque<DataRow>,
        decoder: &'a Decoder,
        aggregate: &'a Aggregate,
        plan: &AggregateRewritePlan,
    ) -> Option<Self> {
        let mut helper_columns: HashMap<usize, HelperColumns> = HashMap::new();

        for target in aggregate.targets() {
            let key = target.column();
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
                continue;
            };

            let entry = helper_columns.entry(helper.target_column).or_default();
            match helper.kind {
                HelperKind::Count => entry.count = Some(index),
                HelperKind::Sum => entry.sum = Some(index),
                HelperKind::SumSquares => entry.sumsq = Some(index),
            }
        }

        let helpers_present = aggregate.targets().iter().all(|target| {
            let key = target.column();
            match target.function() {
                AggregateFunction::Avg => helper_columns
                    .get(&key)
                    .and_then(|columns| columns.count)
                    .is_some(),
                AggregateFunction::StddevPop
                | AggregateFunction::StddevSamp
                | AggregateFunction::VarPop
                | AggregateFunction::VarSamp => helper_columns
                    .get(&key)
                    .map(|columns| {
                        columns.count.is_some() && columns.sum.is_some() && columns.sumsq.is_some()
                    })
                    .unwrap_or(false),
                _ => true,
            }
        });

        // FIXME(sage): Indicates the rewriter didn't run. This is expected for
        // direct-to-shard and explain, but we should avoid calling this
        // function at all for those queries and treat missing helpers as an
        // error in order to catch bugs
        if helpers_present {
            Some(Self {
                rows,
                decoder,
                mappings: HashMap::new(),
                aggregate,
                helper_columns,
            })
        } else {
            None
        }
    }

    pub(super) fn aggregate(mut self) -> Result<VecDeque<DataRow>, Error> {
        for row in self.rows {
            let grouping = Grouping::new(row, self.aggregate.group_by(), self.decoder)?;
            let entry = match self.mappings.entry(grouping) {
                Entry::Occupied(o) => o.into_mut(),
                Entry::Vacant(v) => v.insert(Accumulator::from_aggregate(
                    self.aggregate,
                    &self.helper_columns,
                )?),
            };

            for aggregate in entry {
                aggregate.accumulate(row, self.decoder)?;
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
            for acc in accumulator {
                let target_column = acc.target.column();
                let datum = acc.finalize()?;
                row.insert(
                    target_column,
                    datum.encode(self.decoder.format(target_column))?,
                    datum.is_null(),
                );
            }
            rows.push_back(row);
        }

        Ok(rows)
    }
}

/// Adds rhs to self. Returns an error if self + rhs are not the same type, or
/// if self is a type that cannot be added.
///
/// The behavior of this function diverges from postgres when handling NULL.
/// When calculating x + NULL, we will return x, while postgres will return NULL
fn checked_add_assign(lhs: &mut Datum, rhs: Datum) -> Result<(), TypeError> {
    use Datum::*;
    match (lhs, rhs) {
        (Bigint(a), Bigint(b)) => *a += b,
        (Integer(a), Integer(b)) => *a += b,
        (SmallInt(a), SmallInt(b)) => *a += b,
        (Interval(a), Interval(b)) => *a += b,
        (Numeric(a), Numeric(b)) => *a += b,
        (Float(a), Float(b)) => a.0 += b.0,
        (Double(a), Double(b)) => a.0 += b.0,
        (a @ Datum::Null, b) => *a = b,
        (_, Datum::Null) => {}
        (a, b) if mem::discriminant(a) != mem::discriminant(&b) => {
            return Err(TypeError::IncompatibleTypes(a.data_type(), b.data_type()));
        }
        (a, _) => {
            return Err(TypeError::InvalidOperation {
                op: "add",
                ty: a.data_type(),
            });
        }
    }

    Ok(())
}

fn checked_add(mut lhs: Datum, rhs: Datum) -> Result<Datum, TypeError> {
    checked_add_assign(&mut lhs, rhs)?;
    Ok(lhs)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::frontend::router::parser::rewrite::statement::aggregate::{
        HelperKind, HelperMapping,
    };
    use crate::net::{
        Decoder,
        messages::{Field, Format, RowDescription},
    };
    use bytes::Bytes;
    use pg_query::{NodeEnum, protobuf::SelectStmt};
    use pgdog_postgres_types::Double;
    use std::assert_matches;
    use std::collections::VecDeque;

    fn integer_field(name: &str) -> Field {
        Field {
            name: name.into(),
            table_oid: 0,
            column: 0,
            type_oid: 23, // PostgreSQL OID for int4/integer
            type_size: 4,
            type_modifier: -1,
            format: 0,
        }
    }

    fn integer_array_field(name: &str) -> Field {
        Field {
            name: name.into(),
            table_oid: 0,
            column: 0,
            type_oid: 1007,
            type_size: -1,
            type_modifier: -1,
            format: 0,
        }
    }

    fn interval_array_field(name: &str) -> Field {
        Field {
            name: name.into(),
            table_oid: 0,
            column: 0,
            type_oid: 1187,
            type_size: -1,
            type_modifier: -1,
            format: 0,
        }
    }

    fn select(stmt: &str) -> SelectStmt {
        let stmt = pg_query::parse(stmt)
            .unwrap()
            .protobuf
            .stmts
            .remove(0)
            .stmt
            .unwrap();
        match stmt.node.unwrap() {
            NodeEnum::SelectStmt(stmt) => *stmt,
            _ => panic!("not a select"),
        }
    }

    fn parse(stmt: &str) -> Aggregate {
        Aggregate::parse(&select(stmt), &Default::default())
    }

    #[test]
    fn aggregate_count_with_int_typecast() {
        // Regression test for https://github.com/pgdogdev/pgdog/issues/861
        // SELECT COUNT(*)::int returns int4 from each shard; the accumulator
        // must merge the per-shard values and preserve the requested type.
        let aggregate = parse("SELECT COUNT(*)::int FROM users");

        let rd = RowDescription::new(&[integer_field("count")]);
        let decoder = Decoder::from(&rd);

        let mut rows = VecDeque::new();
        let mut shard0 = DataRow::new();
        shard0.add("2");
        rows.push_back(shard0);

        let mut shard1 = DataRow::new();
        shard1.add("3");
        rows.push_back(shard1);

        let plan = AggregateRewritePlan::default();
        let mut result = Aggregates::new(&rows, &decoder, &aggregate, &plan)
            .unwrap()
            .aggregate()
            .unwrap();

        assert_eq!(result.len(), 1);
        let row = result.pop_front().unwrap();
        let total_count = row.get::<i32>(0, Format::Text).unwrap();
        assert_eq!(total_count, 5);
    }

    #[test]
    #[ignore = "this unit test constructs a synthetic case that isn't realistic, and we currently rely on this behavior for control flow on direct-to-shard and explain"]
    fn aggregate_errors_when_helper_alias_missing() {
        let aggregate = parse("SELECT AVG(price) FROM menu");

        let rd = RowDescription::new(&[Field::double("avg")]);
        let decoder = Decoder::from(&rd);

        let mut rows = VecDeque::new();
        let mut shard0 = DataRow::new();
        shard0.add(12.0_f64);
        rows.push_back(shard0);

        let mut plan = AggregateRewritePlan::default();
        plan.add_helper(HelperMapping {
            target_column: 0,
            helper_column: 1,
            distinct: false,
            kind: HelperKind::Count,
            alias: "__pgdog_count_expr0_col0".into(),
        });

        let result = Aggregates::new(&rows, &decoder, &aggregate, &plan)
            .unwrap()
            .aggregate();

        assert_matches!(
            result,
            Err(Error::UnsupportedAggregation {
                function,
                ..
            }) if function == "avg"
        );
    }

    #[test]
    fn aggregate_group_by_merges_rows() {
        let aggregate = parse("SELECT price, SUM(quantity) FROM menu GROUP BY 1");

        let rd = RowDescription::new(&[Field::double("price"), Field::bigint("sum")]);
        let decoder = Decoder::from(&rd);

        let mut rows = VecDeque::new();
        let mut shard0 = DataRow::new();
        shard0.add(10.0_f64).add(5_i64);
        rows.push_back(shard0);
        let mut shard1 = DataRow::new();
        shard1.add(10.0_f64).add(7_i64);
        rows.push_back(shard1);
        let mut shard2 = DataRow::new();
        shard2.add(20.0_f64).add(4_i64);
        rows.push_back(shard2);

        let mut result = Aggregates::new(
            &rows,
            &decoder,
            &aggregate,
            &AggregateRewritePlan::default(),
        )
        .unwrap()
        .aggregate()
        .unwrap();

        assert_eq!(result.len(), 2);
        let mut groups: Vec<(f64, i64)> = result
            .drain(..)
            .map(|row| {
                let price = row.get::<Double>(0, Format::Text).unwrap().0;
                let sum = row.get::<i64>(1, Format::Text).unwrap();
                (price, sum)
            })
            .collect();
        groups.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        assert_eq!(groups[0], (10.0, 12));
        assert_eq!(groups[1], (20.0, 4));
    }

    #[test]
    fn aggregate_group_by_multidimensional_arrays_uses_raw_bytes() {
        let aggregate = parse("SELECT matrix, COUNT(*) FROM samples GROUP BY 1");

        let rd = RowDescription::new(&[integer_array_field("matrix"), Field::bigint("count")]);
        let decoder = Decoder::from(&rd);

        let mut rows = VecDeque::new();

        let mut shard0 = DataRow::new();
        shard0.add(Bytes::from_static(b"{{1,2},{3,4}}")).add(1_i64);
        rows.push_back(shard0);

        let mut shard1 = DataRow::new();
        shard1.add(Bytes::from_static(b"{{1,2},{3,4}}")).add(1_i64);
        rows.push_back(shard1);

        let mut shard2 = DataRow::new();
        shard2.add(Bytes::from_static(b"{{5,6},{7,8}}")).add(1_i64);
        rows.push_back(shard2);

        let mut result = Aggregates::new(
            &rows,
            &decoder,
            &aggregate,
            &AggregateRewritePlan::default(),
        )
        .unwrap()
        .aggregate()
        .unwrap();

        let mut groups: Vec<(String, i64)> = result
            .drain(..)
            .map(|row| {
                let matrix = row.get::<String>(0, Format::Text).unwrap();
                let count = row.get::<i64>(1, Format::Text).unwrap();
                (matrix, count)
            })
            .collect();
        groups.sort();

        assert_eq!(
            groups,
            vec![("{{1,2},{3,4}}".into(), 2), ("{{5,6},{7,8}}".into(), 1),]
        );
    }

    #[test]
    fn aggregate_group_by_interval_arrays_preserves_postgres_text_output() {
        let aggregate = parse("SELECT sample_interval_array, COUNT(*) FROM samples GROUP BY 1");

        let rd = RowDescription::new(&[
            interval_array_field("sample_interval_array"),
            Field::bigint("count"),
        ]);
        let decoder = Decoder::from(&rd);

        let input = Bytes::from_static(br#"{"1 year 2 mons 1 day 04:05:06.7"}"#);

        let mut rows = VecDeque::new();
        let mut shard0 = DataRow::new();
        shard0.add(input.clone()).add(1_i64);
        rows.push_back(shard0);

        let mut shard1 = DataRow::new();
        shard1.add(input.clone()).add(1_i64);
        rows.push_back(shard1);

        let mut result = Aggregates::new(
            &rows,
            &decoder,
            &aggregate,
            &AggregateRewritePlan::default(),
        )
        .unwrap()
        .aggregate()
        .unwrap();

        assert_eq!(result.len(), 1);
        let row = result.pop_front().unwrap();
        let intervals = row.get::<String>(0, Format::Text).unwrap();
        let count = row.get::<i64>(1, Format::Text).unwrap();

        assert_eq!(intervals, r#"{"1 year 2 mons 1 day 04:05:06.7"}"#);
        assert_eq!(count, 2);
    }

    #[test]
    fn test_adding_types_which_cannot_be_added() {
        let mut datum = Datum::Text("hello".to_owned());
        // operator does not exist: text + text
        let result = checked_add_assign(&mut datum, Datum::Text("goodbye".to_owned()));
        assert_matches!(result, Err(TypeError::InvalidOperation { .. }));
    }

    #[test]
    fn test_adding_incompatible_types() {
        let mut datum = Datum::Integer(1);
        let result = checked_add_assign(&mut datum, Datum::Text("1".to_owned()));
        assert_matches!(result, Err(TypeError::IncompatibleTypes(..)));
    }
}
