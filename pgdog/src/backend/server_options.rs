use crate::net::Parameter;

#[derive(Debug, Clone, Default)]
pub(crate) struct ServerOptions {
    pub(crate) params: Vec<Parameter>,
}

impl ServerOptions {
    pub(crate) fn replication_mode(&self) -> bool {
        self.params
            .iter()
            .any(|p| p.name == "replication" && p.value == "database")
    }

    pub(crate) fn new_replication() -> Self {
        Self {
            params: vec![Parameter {
                name: "replication".into(),
                value: "database".into(),
            }],
        }
    }
}
