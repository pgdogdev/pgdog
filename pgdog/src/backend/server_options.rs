use crate::net::Parameter;

#[derive(Debug, Clone)]
pub struct ServerOptions {
    pub params: Vec<Parameter>,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self { params: vec![] }
    }
}
