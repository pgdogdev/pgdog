use crate::net::{Parameter, parameter::ParameterValue};

#[derive(Debug, Clone, Default)]
pub(crate) struct ServerOptions {
    pub(crate) params: Vec<Parameter>,
    pub(crate) pool_id: u64,
}

impl ServerOptions {
    pub(crate) fn replication_mode(&self) -> bool {
        self.params.iter().any(|p| {
            p.name == "replication"
                && match p.value {
                    ParameterValue::String(ref value) => value == "database",
                    _ => false,
                }
        })
    }

    pub(crate) fn new_replication() -> Self {
        Self {
            params: vec![Parameter {
                name: "replication".into(),
                value: "database".into(),
            }],
            pool_id: 0,
        }
    }
}
