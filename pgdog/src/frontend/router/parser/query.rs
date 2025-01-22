use crate::frontend::{router::Route, Buffer};

use super::{copy::CopyParser, Error};

/// Command determined by the query parser.
#[derive(Debug)]
pub enum Command {
    Query(Route),
    Copy(CopyParser),
}

pub fn parse(buffer: &Buffer) -> Result<Command, Error> {
    if let Some(query) = buffer.query()? {
        todo!()
    }
    todo!()
}
