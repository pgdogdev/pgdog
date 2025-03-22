//! Difference between parameter sets.
use crate::backend::{Error, Server};

use super::{parameter::Parameters, Parameter};

/// Compute a parameter diff between client and server.
pub struct ParameterDiff<'a> {
    diff: Vec<&'a Parameter>,
}

impl<'a> ParameterDiff<'a> {
    /// Compute diff between client and server params.
    pub fn new(one: &'a Parameters, two: &'a Parameters) -> Self {
        let mut diff = vec![];

        for param in one.iter() {
            let other = two.get(&param.name);
            if let Some(other) = other {
                if other != param.value {
                    diff.push(param);
                }
            } else {
                diff.push(param);
            }
        }

        Self { diff }
    }

    /// Synchronize parameters on the server.
    pub async fn sync(&self, server: &mut Server) -> Result<(), Error> {
        let queries = self.queries();
        if !queries.is_empty() {
            server
                .execute_batch(&queries.iter().map(|s| s.as_str()).collect::<Vec<&str>>())
                .await?;
        }
        Ok(())
    }

    /// Create queries to update parameters to desired values.
    fn queries(&self) -> Vec<String> {
        self.diff
            .iter()
            .map(|p| format!("SET \"{}\" TO '{}';", p.name, p.value))
            .collect()
    }
}
