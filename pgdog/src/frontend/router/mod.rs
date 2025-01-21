//! Query router.

use crate::{backend::Cluster, plugin::plugins};

use pgdog_plugin::{CopyInput, Input, RoutingInput};
use tokio::time::Instant;
use tracing::debug;

pub mod copy;
pub mod error;
pub mod request;
pub mod route;

use request::Request;

pub use copy::{Copy, CopyRow};
pub use error::Error;
pub use route::Route;

use super::Buffer;

/// Query router.
pub struct Router {
    route: Route,
    copy: Option<Copy>,
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
            copy: None,
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
        let input = Input::new_query(config, RoutingInput::query(request.query()));

        let now = Instant::now();

        for plugin in plugins() {
            match plugin.route(input) {
                None => continue,
                Some(output) => {
                    // COPY subprotocol support.
                    if let Some(copy) = output.copy() {
                        self.copy = Some(copy.into());
                        self.route = pgdog_plugin::Route::write_all().into();
                        break;
                    } else if let Some(route) = output.route() {
                        // Don't override route unless we have one.
                        if route.is_unknown() {
                            continue;
                        }

                        self.route = route.into();
                        unsafe { output.deallocate() }

                        debug!(
                            "routing {} to {} [{}, {:.3}ms]",
                            if route.is_read() { "read" } else { "write" },
                            if let Some(shard) = self.route.shard() {
                                format!("shard {}", shard)
                            } else {
                                "all shards".to_string()
                            },
                            plugin.name(),
                            now.elapsed().as_secs_f64() * 1000.0,
                        );

                        break;
                    }
                }
            }
        }

        unsafe { input.deallocate() }

        Ok(())
    }

    /// Parse CopyData messages and shard them.
    pub fn copy_data(&mut self, buffer: &Buffer, cluster: &Cluster) -> Result<Vec<CopyRow>, Error> {
        let mut rows = vec![];
        if let Some(ref mut copy) = self.copy {
            let messages = buffer.copy_data()?;

            for copy_data in messages {
                let copy_input = CopyInput::new(copy_data.data(), 0, copy.headers, copy.delimiter);

                // SAFETY: deallocated by Input below.
                let config = unsafe { cluster.plugin_config()? };
                let input = Input::new_copy(config, RoutingInput::copy(copy_input));

                for plugin in plugins() {
                    match plugin.route(input) {
                        None => continue,
                        Some(output) => {
                            if let Some(copy_rows) = output.copy_rows() {
                                if let Some(headers) = copy_rows.header() {
                                    copy.headers(headers);
                                    rows.push(CopyRow::headers(headers));
                                }
                                for row in copy_rows.rows() {
                                    rows.push((*row).into());
                                }
                            }

                            unsafe {
                                output.deallocate();
                            }

                            // Allow only one plugin to remap copy data rows.
                            if !rows.is_empty() {
                                break;
                            }
                        }
                    }
                }

                unsafe {
                    input.deallocate();
                }
            }

            // Make sure we tell the plugin no more headers are expected.
            copy.headers = false;
        }

        Ok(rows)
    }

    /// Get current route.
    pub fn route(&self) -> &Route {
        &self.route
    }
}
