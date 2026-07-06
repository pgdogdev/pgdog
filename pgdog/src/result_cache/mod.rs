pub mod redis;

use std::time::Duration;

use crate::frontend::ClientRequest;
use crate::frontend::router::parser::OwnedTable;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResultCacheKey {
    pub redis_key: String,
    pub ttl: Option<Duration>,
    pub max_entry_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct CacheableRequest {
    pub query: String,
    pub route_sig: &'static str,
    pub tables: Vec<OwnedTable>,
    pub parameters: Vec<Vec<u8>>,
}

impl CacheableRequest {
    pub fn from_client_request(client_request: &ClientRequest) -> Option<Self> {
        // Must be an executable unit (Query or Execute).
        if !client_request.is_executable() {
            return None;
        }

        let buffered = client_request.query().ok()??;
        let query = buffered.query().to_string();

        let route = client_request.route();
        if !route.is_read() {
            return None;
        }

        let route_sig = if route.is_cross_shard() { "cross" } else { "direct" };
        let tables = client_request
            .ast
            .as_ref()
            .map(|ast| ast.tables().into_iter().map(|t| t.to_owned()).collect())
            .unwrap_or_default();

        let parameters = client_request
            .parameters()
            .ok()?
            .map(|bind| {
                bind.params_raw()
                    .iter()
                    .map(|p| p.data.to_vec())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        Some(Self {
            query,
            route_sig,
            tables,
            parameters,
        })
    }
}

#[cfg(test)]
mod test;

