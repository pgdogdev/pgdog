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
        let mut params = vec![];

        for param in stmt.params() {
            let param = match param {
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
            };

            params.push(param);
        }

        Self {
            params,
            format: vec![Format::Text],
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
                let params = StatementParameters::Execute(&ExecuteParams::new(execute));
                let first = params.parameter(0).unwrap().unwrap().bigint().unwrap();
                assert_eq!(first, 1);
            }

            _ => panic!("not an execute stmt"),
        }
    }
}
