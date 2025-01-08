//! Query router.

use std::ffi::CString;

use crate::plugin::plugins;

use pgdog_plugin::{Query, Route};
use tokio::time::Instant;
use tracing::debug;

pub mod error;
pub mod parser;

pub use error::Error;

use super::Buffer;

/// Query router.
pub struct Router {
    route: Route,
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

impl Router {
    /// Create new router.
    pub fn new() -> Router {
        Self {
            route: Route::unknown(),
        }
    }

    /// Route a query to a shard.
    ///
    /// If the router can't determine the route for the query to take,
    /// previous route is preserved. This is useful in case the client
    /// doesn't supply enough information in the buffer, e.g. just issued
    /// a Describe request to a previously submitted Parse.
    pub fn query(&mut self, buffer: &Buffer) -> Result<Route, Error> {
        let query = buffer
            .query()
            .map_err(|_| Error::NoQueryInBuffer)?
            .ok_or(Error::NoQueryInBuffer)?;
        let c_query = CString::new(query.as_str())?;
        let mut query = Query::new(&c_query);

        // SAFETY: query has not allocated memory for parameters yet.
        if let Ok(Some(bind)) = buffer.parameters() {
            let params = bind.plugin_parameters()?;

            // SAFETY: memory for parameters is owned by Query.
            query.parameters(&params);
        }

        let now = Instant::now();

        for plugin in plugins() {
            match plugin.route(query) {
                None => continue,
                Some(output) => {
                    if let Some(route) = output.route() {
                        if route.is_unknown() {
                            continue;
                        }
                        self.route = route;

                        debug!(
                            "routing {} to shard {} [{}, {:.3}ms]",
                            if route.read() { "read" } else { "write" },
                            route.shard().unwrap_or(0),
                            plugin.name(),
                            now.elapsed().as_secs_f64() * 1000.0,
                        );
                        query.drop();
                        return Ok(route);
                    }
                }
            }
        }

        query.drop();
        Ok(self.route)
    }

    /// Get current route.
    pub fn route(&self) -> &Route {
        &self.route
    }
}
