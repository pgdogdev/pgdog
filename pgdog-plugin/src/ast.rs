//! Wrapper around `pg_query` protobuf-generated statement.
//!
//! This is passed through FFI safely by ensuring two conditions:
//!
//! 1. The version of the **Rust compiler** used to build the plugin is the same used to build PgDog
//! 2. The version of the **`pg_query` library** used by the plugin is the same used by PgDog
//!
use std::{ffi::c_void, ops::Deref};

use pg_query::protobuf::{ParseResult, RawStmt};

use crate::bindings::PdStatement;

impl PdStatement {
    /// Create FFI binding from `pg_query` output.
    ///
    /// SAFETY: The reference must live for the entire time
    /// this struct is used. This is _not_ checked by the compiler,
    /// and is the responsibility of the caller.
    ///
    pub unsafe fn from_proto(value: &ParseResult) -> Self {
        Self {
            data: value.stmts.as_ptr() as *mut c_void,
            version: value.version,
            len: value.stmts.len() as u64,
        }
    }
}

/// Wrapper around [`pg_query::protobuf::ParseResult`], which allows
/// the caller to use `pg_query` types and methods to inspect the statement.
#[derive(Debug)]
pub struct PdParseResult {
    parse_result: Option<ParseResult>,
    borrowed: bool,
}

impl Clone for PdParseResult {
    /// Cloning the binding is safe. A new structure
    /// will be created without any references to the original
    /// reference.
    fn clone(&self) -> Self {
        Self {
            parse_result: self.parse_result.clone(),
            borrowed: false,
        }
    }
}

impl From<PdStatement> for PdParseResult {
    /// Create the binding from a FFI-passed reference.
    ///
    /// SAFETY: Memory is owned by the caller.
    ///
    fn from(value: PdStatement) -> Self {
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
    /// Drop the binding and forget the memory if the binding
    /// is using the referenced struct. Otherwise, deallocate as normal.
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

impl PdStatement {
    /// Get the protobuf-wrapped PostgreSQL statement. The returned structure, [`PdParseResult`],
    /// implements [`Deref`] to [`pg_query::protobuf::ParseResult`] and can be used to parse the
    /// statement.
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
        let ffi = unsafe { PdStatement::from_proto(&ast.protobuf) };
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
