use super::*;
use crate::frontend::ClientRequest;

impl QueryEngine {
    /// Check if the request needs splitting and perform the split if necessary.
    ///
    /// Caller is expected to abort the request and return the result back to the caller
    /// for resubmission.
    pub(super) fn check_extended_request_split(
        request: &ClientRequest,
    ) -> Result<Option<QueryEngineResult>, Error> {
        if request.is_multi_exec() {
            Ok(Some(QueryEngineResult::Split(request.split_extended()?)))
        } else {
            Ok(None)
        }
    }

    /// Return true if we should ignore this request because
    /// the extended protocol state is out of sync, e.g., in error state.
    ///
    /// This is identical behavior to Postgres.
    ///
    /// If we see a [`crate::net::Sync`]-only request, we execute it to restore servers
    /// back to normal state.
    ///
    pub(super) fn extended_in_sync_check(&self, context: &QueryEngineContext<'_>) -> bool {
        self.backend.out_of_sync()
            && !context.client_request.is_sync_only()
            && context.requests_left > 0
    }
}
