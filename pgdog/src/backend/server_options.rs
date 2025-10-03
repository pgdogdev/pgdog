use crate::net::Parameter;

#[derive(Debug, Clone, Default)]
pub struct ServerOptions {
    pub params: Vec<Parameter>,
    pub pool_id: u64,
}

impl ServerOptions {
    pub fn replication_mode(&self) -> bool {
        self.params
            .iter()
            .any(|p| p.name == "replication" && p.value == "database")
    }

    pub fn new_replication() -> Self {
        Self {
            params: vec![Parameter {
                name: "replication".into(),
                value: "database".into(),
            }],
            pool_id: 0,
        }
    }
}
