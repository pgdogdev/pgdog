use crate::net::Parameter;

#[derive(Debug, Clone, Default)]
pub struct ServerOptions {
    pub params: Vec<Parameter>,
}
