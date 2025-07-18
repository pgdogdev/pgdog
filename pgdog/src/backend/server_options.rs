use crate::net::Parameter;

#[derive(Debug, Clone, Default)]
pub struct ServerOptions {
    pub params: Vec<Parameter>,
}

impl ServerOptions {
    pub fn replication_mode(&self) -> bool {
        self.params
            .iter()
            .any(|p| p.name == "replication" && p.value == "database")
    }

    #[cfg(test)]
    pub fn new_replication() -> Self {
        Self {
            params: vec![Parameter {
                name: "replication".into(),
                value: "database".into(),
            }],
        }
    }
}
