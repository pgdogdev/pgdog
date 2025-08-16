use std::{ffi::c_void, ops::Deref};

use pg_query::protobuf::{ParseResult, RawStmt};

use crate::bindings::PdQuery;

impl From<&ParseResult> for PdQuery {
    fn from(value: &ParseResult) -> Self {
        Self {
            data: value.stmts.as_ptr() as *mut c_void,
            version: value.version,
            len: value.stmts.len() as u64,
        }
    }
}

pub struct PdParseResult {
    parse_result: Option<ParseResult>,
    borrowed: bool,
}

impl Clone for PdParseResult {
    fn clone(&self) -> Self {
        Self {
            parse_result: self.parse_result.clone(),
            borrowed: false,
        }
    }
}

impl From<PdQuery> for PdParseResult {
    fn from(value: PdQuery) -> Self {
        Self {
            parse_result: Some(ParseResult {
                version: value.version,
                stmts: unsafe {
                    Vec::from_raw_parts(
                        value.data as *mut RawStmt,
                        value.len as usize,
                        value.len as usize,
                    )
                },
            }),
            borrowed: true,
        }
    }
}

impl Drop for PdParseResult {
    fn drop(&mut self) {
        if self.borrowed {
            let parse_result = self.parse_result.take();
            std::mem::forget(parse_result.unwrap().stmts);
        }
    }
}

impl Deref for PdParseResult {
    type Target = ParseResult;

    fn deref(&self) -> &Self::Target {
        self.parse_result.as_ref().unwrap()
    }
}

impl PdQuery {
    pub fn protobuf(&self) -> PdParseResult {
        PdParseResult::from(*self)
    }
}

#[cfg(test)]
mod test {
    use pg_query::NodeEnum;

    use super::*;

    #[test]
    fn test_ast() {
        let ast = pg_query::parse("SELECT * FROM users WHERE id = $1").unwrap();
        let ffi = PdQuery::from(&ast.protobuf);
        match ffi
            .protobuf()
            .stmts
            .first()
            .unwrap()
            .stmt
            .as_ref()
            .unwrap()
            .node
            .as_ref()
            .unwrap()
        {
            NodeEnum::SelectStmt(_) => (),
            _ => {
                panic!("not a select")
            }
        };

        let _ = ffi.protobuf().clone();
    }
}
