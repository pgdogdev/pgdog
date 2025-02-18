use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Parse for statement is missing")]
    StatementParseMissing,
}
