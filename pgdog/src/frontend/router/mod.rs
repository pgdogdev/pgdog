//! Query router.

use crate::{backend::Cluster, plugin::plugins};

use pgdog_plugin::{Input, RoutingInput};
use tokio::time::Instant;
use tracing::debug;

pub mod error;
pub mod request;
pub mod route;

use request::Request;

pub use error::Error;
pub use route::Route;

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
    pub fn query(&mut self, buffer: &Buffer, cluster: &Cluster) -> Result<(), Error> {
        // TODO: avoid allocating a String
        // and pass a raw pointer from Bytes.
        let query = if let Ok(Some(query)) = buffer.query() {
            query
        } else {
            return Ok(());
        };

        let mut request = Request::new(query.as_str())?;

        if let Ok(Some(bind)) = buffer.parameters() {
            // SAFETY: memory for parameters is owned by Request.
            // If this errors out, Request will drop and deallocate all
            // previously set parameters.
            let params = unsafe { bind.plugin_parameters()? };

            request.set_parameters(&params);
        }

        // SAFETY: deallocated by Input below.
        let config = unsafe { cluster.plugin_config()? };
        let input = Input::new(config, RoutingInput::query(request.query()));

        let now = Instant::now();

        for plugin in plugins() {
            match plugin.route(input) {
                None => continue,
                Some(output) => {
                    if let Some(route) = output.route() {
                        if route.is_unknown() {
                            continue;
                        }

                        self.route = route.into();
                        unsafe { output.drop() }

                        debug!(
                            "routing {} to shard {} [{}, {:.3}ms]",
                            if route.is_read() { "read" } else { "write" },
                            route.shard().unwrap_or(0),
                            plugin.name(),
                            now.elapsed().as_secs_f64() * 1000.0,
                        );

                        break;
                    }
                }
            }
        }

        unsafe { input.drop() }

        Ok(())
    }

    /// Get current route.
    pub fn route(&self) -> &Route {
        &self.route
    }
}
