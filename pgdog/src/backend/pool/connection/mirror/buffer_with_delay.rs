//! Client request with duration from previous request,
//! to simulate similar query timings on the mirror.

use std::time::Duration;

use crate::frontend::ClientRequest;

/// Simulate original delay between requests.
#[derive(Clone, Debug)]
pub(crate) struct BufferWithDelay {
    pub(super) delay: Duration,
    pub(super) buffer: ClientRequest,
}
