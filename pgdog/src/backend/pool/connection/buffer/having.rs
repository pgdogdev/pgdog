use std::{cmp::Ordering, collections::HashMap};

use pgdog_postgres_types::Datum;

use crate::{
    frontend::router::parser::{CompareOperator, HavingExpr, HavingValue},
    net::{Decoder, messages::DataRow},
};

use super::super::aggregate::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TruthValue {
    True,
    False,
    Unknown,
}

pub(super) struct Evaluator<'a> {
    expression: &'a HavingExpr,
    columns: HashMap<&'a str, usize>,
}

impl<'a> Evaluator<'a> {
    pub(super) fn new(expression: &'a HavingExpr, decoder: &'a Decoder) -> Self {
        let columns = decoder
            .row_description()
            .fields
            .iter()
            .enumerate()
            .map(|(index, field)| (field.name.as_str(), index))
            .collect();
        Self {
            expression,
            columns,
        }
    }

    pub(super) fn evaluate(&self, row: &DataRow, decoder: &Decoder) -> Result<bool, Error> {
        Ok(matches!(
            self.evaluate_expression(self.expression, row, decoder)?,
            TruthValue::True
        ))
    }

    fn evaluate_expression(
        &self,
        expression: &HavingExpr,
        row: &DataRow,
        decoder: &Decoder,
    ) -> Result<TruthValue, Error> {
        match expression {
            HavingExpr::Compare {
                left,
                operator,
                right,
            } => {
                let left = self.value(left, row, decoder)?;
                let right = self.value(right, row, decoder)?;
                compare(&left, *operator, &right)
            }
        }
    }

    fn value(&self, value: &HavingValue, row: &DataRow, decoder: &Decoder) -> Result<Datum, Error> {
        let index =
            self.columns.get(value.alias()).copied().ok_or_else(|| {
                Error::InvalidHaving(format!("missing operand {}", value.alias()))
            })?;
        Ok(row.get_column_checked(index, decoder)?.value)
    }
}

#[cfg(test)]
fn evaluate(expression: &HavingExpr, row: &DataRow, decoder: &Decoder) -> Result<bool, Error> {
    Evaluator::new(expression, decoder).evaluate(row, decoder)
}

#[cfg(test)]
fn evaluate_expression(
    expression: &HavingExpr,
    row: &DataRow,
    decoder: &Decoder,
) -> Result<TruthValue, Error> {
    Evaluator::new(expression, decoder).evaluate_expression(expression, row, decoder)
}

fn compare(left: &Datum, operator: CompareOperator, right: &Datum) -> Result<TruthValue, Error> {
    if left.is_null() || right.is_null() {
        return Ok(TruthValue::Unknown);
    }

    let ordering = numeric_ordering(left, right)
        .or_else(|| left.partial_cmp(right))
        .ok_or_else(|| {
            Error::UnsupportedHaving(format!(
                "cannot compare {} with {}",
                left.data_type(),
                right.data_type()
            ))
        })?;
    let result = match operator {
        CompareOperator::Equal => ordering == Ordering::Equal,
        CompareOperator::NotEqual => ordering != Ordering::Equal,
        CompareOperator::Less => ordering == Ordering::Less,
        CompareOperator::LessOrEqual => ordering != Ordering::Greater,
        CompareOperator::Greater => ordering == Ordering::Greater,
        CompareOperator::GreaterOrEqual => ordering != Ordering::Less,
    };
    Ok(if result {
        TruthValue::True
    } else {
        TruthValue::False
    })
}

fn numeric_ordering(left: &Datum, right: &Datum) -> Option<Ordering> {
    let (left, right) = (integer(left)?, integer(right)?);
    Some(left.cmp(&right))
}

fn integer(value: &Datum) -> Option<i128> {
    match value {
        Datum::Bigint(value) => Some((*value).into()),
        Datum::Integer(value) => Some((*value).into()),
        Datum::SmallInt(value) => Some((*value).into()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::{Field, RowDescription};

    fn compare_expression(operator: CompareOperator) -> HavingExpr {
        HavingExpr::Compare {
            left: HavingValue::new("__pgdog_having_0".into()),
            operator,
            right: HavingValue::new("__pgdog_having_1".into()),
        }
    }

    #[test]
    fn null_comparison_is_unknown_and_filters_the_group() {
        let decoder = Decoder::from(RowDescription::new(&[
            Field::bigint("__pgdog_having_0"),
            Field::bigint("__pgdog_having_1"),
        ]));
        let mut row = DataRow::new();
        row.add(Datum::Null).add(2_i64);

        let unknown = compare_expression(CompareOperator::Greater);
        assert_eq!(
            evaluate_expression(&unknown, &row, &decoder).unwrap(),
            TruthValue::Unknown
        );
        assert!(!evaluate(&unknown, &row, &decoder).unwrap());

        let missing = HavingExpr::Compare {
            left: HavingValue::new("__pgdog_missing".into()),
            operator: CompareOperator::Equal,
            right: HavingValue::new("__pgdog_having_1".into()),
        };
        assert!(matches!(
            evaluate(&missing, &row, &decoder),
            Err(Error::InvalidHaving(_))
        ));
    }

    #[test]
    fn compares_integers_across_widths_with_all_operators() {
        for (operator, left, right, expected) in [
            (
                CompareOperator::Equal,
                Datum::Bigint(2),
                Datum::Integer(2),
                true,
            ),
            (
                CompareOperator::NotEqual,
                Datum::Integer(2),
                Datum::SmallInt(3),
                true,
            ),
            (
                CompareOperator::Less,
                Datum::SmallInt(2),
                Datum::Bigint(3),
                true,
            ),
            (
                CompareOperator::LessOrEqual,
                Datum::Bigint(2),
                Datum::Integer(2),
                true,
            ),
            (
                CompareOperator::Greater,
                Datum::Bigint(3),
                Datum::SmallInt(2),
                true,
            ),
            (
                CompareOperator::GreaterOrEqual,
                Datum::Integer(2),
                Datum::Bigint(2),
                true,
            ),
            (
                CompareOperator::Greater,
                Datum::Text("a".into()),
                Datum::Text("z".into()),
                false,
            ),
        ] {
            assert_eq!(
                compare(&left, operator, &right).unwrap(),
                if expected {
                    TruthValue::True
                } else {
                    TruthValue::False
                }
            );
        }

        assert!(matches!(
            compare(
                &Datum::Text("2".into()),
                CompareOperator::Equal,
                &Datum::Integer(2)
            ),
            Err(Error::UnsupportedHaving(_))
        ));
    }
}
