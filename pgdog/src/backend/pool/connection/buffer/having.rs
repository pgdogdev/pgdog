use std::cmp::Ordering;

use pgdog_postgres_types::{Datum, Numeric};

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

impl TruthValue {
    fn not(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Unknown => Self::Unknown,
        }
    }
}

pub(super) fn evaluate(
    expression: &HavingExpr,
    row: &DataRow,
    decoder: &Decoder,
) -> Result<bool, Error> {
    Ok(matches!(
        evaluate_expression(expression, row, decoder)?,
        TruthValue::True
    ))
}

fn evaluate_expression(
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
            let left = value(left, row, decoder)?;
            let right = value(right, row, decoder)?;
            compare(&left, *operator, &right)
        }
        HavingExpr::And(expressions) => {
            let mut result = TruthValue::True;
            for expression in expressions {
                match evaluate_expression(expression, row, decoder)? {
                    TruthValue::False => return Ok(TruthValue::False),
                    TruthValue::Unknown => result = TruthValue::Unknown,
                    TruthValue::True => {}
                }
            }
            Ok(result)
        }
        HavingExpr::Or(expressions) => {
            let mut result = TruthValue::False;
            for expression in expressions {
                match evaluate_expression(expression, row, decoder)? {
                    TruthValue::True => return Ok(TruthValue::True),
                    TruthValue::Unknown => result = TruthValue::Unknown,
                    TruthValue::False => {}
                }
            }
            Ok(result)
        }
        HavingExpr::Not(expression) => Ok(evaluate_expression(expression, row, decoder)?.not()),
        HavingExpr::IsNull {
            value: operand,
            negated,
        } => {
            let is_null = value(operand, row, decoder)?.is_null();
            Ok(if is_null != *negated {
                TruthValue::True
            } else {
                TruthValue::False
            })
        }
    }
}

fn value(operand: &HavingValue, row: &DataRow, decoder: &Decoder) -> Result<Datum, Error> {
    let index = decoder
        .row_description()
        .field_index(operand.alias())
        .ok_or_else(|| Error::InvalidHaving(format!("missing operand {}", operand.alias())))?;
    Ok(row.get_column_checked(index, decoder)?.value)
}

fn compare(left: &Datum, operator: CompareOperator, right: &Datum) -> Result<TruthValue, Error> {
    if left.is_null() || right.is_null() {
        return Ok(TruthValue::Unknown);
    }

    let ordering = numeric_ordering(left, right)
        .or_else(|| comparable_ordering(left, right))
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

fn comparable_ordering(left: &Datum, right: &Datum) -> Option<Ordering> {
    match (left, right) {
        (Datum::Text(_), Datum::Text(_))
        | (Datum::Interval(_), Datum::Interval(_))
        | (Datum::Timestamp(_), Datum::Timestamp(_))
        | (Datum::TimestampTz(_), Datum::TimestampTz(_))
        | (Datum::Uuid(_), Datum::Uuid(_))
        | (Datum::Boolean(_), Datum::Boolean(_))
        | (Datum::Oid(_), Datum::Oid(_)) => left.partial_cmp(right),
        _ => None,
    }
}

fn numeric_ordering(left: &Datum, right: &Datum) -> Option<Ordering> {
    if let (Some(left), Some(right)) = (integer(left), integer(right)) {
        return Some(left.cmp(&right));
    }

    if let (Some(left), Some(right)) = (exact_numeric(left), exact_numeric(right)) {
        return left.partial_cmp(&right);
    }

    if let (Some(left), Some(right)) = (float(left), float(right)) {
        return Some(postgres_float_ordering(left, right));
    }

    None
}

fn postgres_float_ordering(left: f64, right: f64) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => left
            .partial_cmp(&right)
            .expect("non-NaN floats always have an ordering"),
    }
}

fn integer(value: &Datum) -> Option<i128> {
    match value {
        Datum::Bigint(value) => Some((*value).into()),
        Datum::Integer(value) => Some((*value).into()),
        Datum::SmallInt(value) => Some((*value).into()),
        _ => None,
    }
}

fn exact_numeric(value: &Datum) -> Option<Numeric> {
    match value {
        Datum::Numeric(value) => Some(*value),
        Datum::Bigint(value) => Some(Numeric::from(*value)),
        Datum::Integer(value) => Some(Numeric::from(*value)),
        Datum::SmallInt(value) => Some(Numeric::from(i32::from(*value))),
        _ => None,
    }
}

fn float(value: &Datum) -> Option<f64> {
    match value {
        Datum::Double(value) => Some(value.0),
        Datum::Float(value) => Some(value.0.into()),
        Datum::Bigint(value) => Some(*value as f64),
        Datum::Integer(value) => Some((*value).into()),
        Datum::SmallInt(value) => Some((*value).into()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        frontend::router::parser::HavingValue,
        net::{
            Field, RowDescription,
            messages::{DataRow, Datum},
        },
    };
    use pgdog_postgres_types::{Double, Float};

    fn compare_expression(operator: CompareOperator) -> HavingExpr {
        HavingExpr::Compare {
            left: HavingValue::new("__pgdog_having_0".into()),
            operator,
            right: HavingValue::new("__pgdog_having_1".into()),
        }
    }

    fn integer_field(name: &str) -> Field {
        Field {
            name: name.into(),
            table_oid: 0,
            column: 0,
            type_oid: 23,
            type_size: 4,
            type_modifier: -1,
            format: 0,
        }
    }

    #[test]
    fn compares_merged_bigint_with_integer_without_losing_precision() {
        let decoder = Decoder::from(RowDescription::new(&[
            Field::bigint("__pgdog_having_0"),
            integer_field("__pgdog_having_1"),
        ]));
        let mut row = DataRow::new();
        row.add(9_007_199_254_740_993_i64).add(2_i32);

        assert!(
            evaluate(
                &compare_expression(CompareOperator::Greater),
                &row,
                &decoder
            )
            .unwrap()
        );
    }

    #[test]
    fn implements_sql_three_valued_boolean_logic() {
        let decoder = Decoder::from(RowDescription::new(&[
            Field::bigint("__pgdog_having_0"),
            integer_field("__pgdog_having_1"),
        ]));
        let mut row = DataRow::new();
        row.add(Datum::Null).add(2_i32);

        let unknown = compare_expression(CompareOperator::Greater);
        let is_null = HavingExpr::IsNull {
            value: HavingValue::new("__pgdog_having_0".into()),
            negated: false,
        };
        let is_not_null = HavingExpr::IsNull {
            value: HavingValue::new("__pgdog_having_1".into()),
            negated: true,
        };
        let known_false = HavingExpr::Compare {
            left: HavingValue::new("__pgdog_having_1".into()),
            operator: CompareOperator::Greater,
            right: HavingValue::new("__pgdog_having_1".into()),
        };

        assert_eq!(
            evaluate_expression(&unknown, &row, &decoder).unwrap(),
            TruthValue::Unknown
        );
        assert_eq!(
            evaluate_expression(
                &HavingExpr::And(vec![is_null.clone(), unknown.clone()]),
                &row,
                &decoder
            )
            .unwrap(),
            TruthValue::Unknown
        );
        assert_eq!(
            evaluate_expression(
                &HavingExpr::And(vec![unknown.clone(), known_false.clone()]),
                &row,
                &decoder
            )
            .unwrap(),
            TruthValue::False
        );
        assert_eq!(
            evaluate_expression(
                &HavingExpr::Or(vec![known_false.clone(), unknown.clone()]),
                &row,
                &decoder
            )
            .unwrap(),
            TruthValue::Unknown
        );
        assert!(
            evaluate(
                &HavingExpr::Or(vec![unknown.clone(), is_null]),
                &row,
                &decoder
            )
            .unwrap()
        );
        assert!(evaluate(&is_not_null, &row, &decoder).unwrap());
        assert!(!evaluate(&HavingExpr::Not(Box::new(unknown)), &row, &decoder).unwrap());

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
    fn compares_supported_scalar_types_and_all_operators() {
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
                Datum::Numeric(Numeric::from(3_i32)),
                Datum::SmallInt(2),
                true,
            ),
            (
                CompareOperator::GreaterOrEqual,
                Datum::Float(Float(2.0)),
                Datum::Double(Double(2.0)),
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

        assert_eq!(
            numeric_ordering(&Datum::Double(Double(f64::NAN)), &Datum::Float(Float(1.0))),
            Some(Ordering::Greater)
        );
        assert_eq!(postgres_float_ordering(f64::NAN, f64::NAN), Ordering::Equal);
        assert_eq!(postgres_float_ordering(1.0, f64::NAN), Ordering::Less);
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
