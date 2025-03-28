use crate::net::{Bind, RowDescription};

#[derive(Debug, Clone)]
pub enum Context<'a> {
    Bind(&'a Bind),
    RowDescription(&'a RowDescription),
}

impl<'a> From<&'a RowDescription> for Context<'a> {
    fn from(value: &'a RowDescription) -> Self {
        Context::RowDescription(value)
    }
}

impl<'a> From<&'a Bind> for Context<'a> {
    fn from(value: &'a Bind) -> Self {
        Context::Bind(value)
    }
}
