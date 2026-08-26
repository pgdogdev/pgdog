//! Get parameter value from [`Bind`] or [`ExecuteStmt`].

use bytes::Bytes;
use pg_raw_parse::{ConstValue, Node, nodes::ExecuteStmt};

use crate::net::{
    Error,
    bind::{Bind, Format, Parameter as BindParameter, ParameterWithFormat},
};

#[derive(Debug)]
pub(crate) struct ExecuteParams {
    params: Vec<BindParameter>,
    format: Vec<Format>,
}

impl ExecuteParams {
    pub(crate) fn new(stmt: &ExecuteStmt) -> Self {
        let params = stmt.params().iter().map(Self::from_node).collect();

        Self {
            params,
            format: vec![Format::Text],
        }
    }

    /// Extract a bind parameter from an EXECUTE argument node.
    ///
    /// Postgres wraps cast expressions as [`Node::TypeCast`], e.g.
    /// `EXECUTE stmt(1::bigint)`. Peel those wrappers so the underlying
    /// constant is used for sharding key extraction.
    fn from_node(param: Node<'_>) -> BindParameter {
        match param {
            Node::TypeCast(cast) => Self::from_node(cast.arg()),
            Node::A_Const(a_const) => match a_const.val() {
                None => BindParameter::new_null(),
                Some(ConstValue::String(text)) => {
                    let data = Bytes::from(text.to_string());
                    BindParameter {
                        len: data.len() as i32,
                        data,
                    }
                }

                Some(ConstValue::Integer(int)) => {
                    let data = Bytes::from(int.to_string());
                    BindParameter {
                        len: data.len() as i32,
                        data,
                    }
                }

                Some(ConstValue::Float(float)) => {
                    let data = Bytes::from(float.to_string());
                    BindParameter {
                        len: data.len() as i32,
                        data,
                    }
                }

                Some(ConstValue::Boolean(bool)) => {
                    let data = Bytes::from((if bool { "t" } else { "f" }).to_string());
                    BindParameter {
                        len: data.len() as i32,
                        data,
                    }
                }

                _ => BindParameter::new_null(),
            },

            _ => BindParameter::new_null(),
        }
    }

    fn parameter(&self, index: usize) -> Option<ParameterWithFormat<'_>> {
        self.params
            .get(index)
            .map(|parameter| ParameterWithFormat::new(parameter, Format::Text))
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum StatementParameters<'a> {
    Bind(&'a Bind),
    Execute(&'a ExecuteParams),
}

impl<'a> From<&'a Bind> for StatementParameters<'a> {
    fn from(value: &'a Bind) -> Self {
        Self::Bind(value)
    }
}

impl<'a> StatementParameters<'a> {
    pub(super) fn parameter(self, index: usize) -> Result<Option<ParameterWithFormat<'a>>, Error> {
        match self {
            Self::Bind(bind) => bind.parameter(index),
            Self::Execute(params) => Ok(params.parameter(index)),
        }
    }

    pub(super) fn params_raw(self) -> &'a [BindParameter] {
        match self {
            Self::Bind(bind) => bind.params_raw(),
            Self::Execute(params) => &params.params,
        }
    }

    pub(super) fn format_codes_raw(self) -> &'a [Format] {
        match self {
            Self::Bind(bind) => bind.format_codes_raw(),
            Self::Execute(params) => &params.format,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_exec_parameter() {
        let stmt = pg_raw_parse::parse("EXECUTE __pgdog_1 (1, 'test', now())").unwrap();
        let stmt_1 = stmt.stmts().next().unwrap();
        match stmt_1 {
            Node::ExecuteStmt(execute) => {
                let exec_params = ExecuteParams::new(execute);
                let params = StatementParameters::Execute(&exec_params);
                let first = params.parameter(0).unwrap().unwrap().bigint().unwrap();
                assert_eq!(first, 1);
            }

            _ => panic!("not an execute stmt"),
        }
    }

    #[test]
    fn test_exec_parameter_with_type_cast() {
        let stmt = pg_raw_parse::parse("EXECUTE __pgdog_1 (1::bigint, 'test'::text)").unwrap();
        let stmt_1 = stmt.stmts().next().unwrap();
        match stmt_1 {
            Node::ExecuteStmt(execute) => {
                let exec_params = ExecuteParams::new(execute);
                let params = StatementParameters::Execute(&exec_params);
                let first = params.parameter(0).unwrap().unwrap().bigint().unwrap();
                assert_eq!(first, 1);

                let second_param = params.parameter(1).unwrap().unwrap();
                assert_eq!(second_param.text().unwrap(), "test");
            }

            _ => panic!("not an execute stmt"),
        }
    }

    #[test]
    fn test_exec_parameter_with_nested_type_cast() {
        // Double cast still peels to the underlying constant.
        let stmt = pg_raw_parse::parse("EXECUTE __pgdog_1 ((1::int)::bigint)").unwrap();
        let stmt_1 = stmt.stmts().next().unwrap();
        match stmt_1 {
            Node::ExecuteStmt(execute) => {
                let exec_params = ExecuteParams::new(execute);
                let params = StatementParameters::Execute(&exec_params);
                let first = params.parameter(0).unwrap().unwrap().bigint().unwrap();
                assert_eq!(first, 1);
            }

            _ => panic!("not an execute stmt"),
        }
    }
}
