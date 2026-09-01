use crate::frontend::ClientRequest;
use crate::frontend::client::query_engine::multi_step::types::{QueryPlanner, Step, StepRequest};

impl QueryPlanner {
    /// Fallback when we don't match to another `QueryPlannerType`;
    /// Runs the `ClientRequest` as normal without any special handling.
    /// Allows us to push all execution flow through the `QueryPlanner` instead of special cases.
    pub(crate) fn plan_normal(client_request: &ClientRequest) -> QueryPlanner {
        let solo_step = Self::construct_solo_step(client_request);
        QueryPlanner {
            steps: vec![solo_step],
            // Everything is forwarded by-request; don't need anything in aggregate
            forward_to_client: None,
        }
    }

    fn construct_solo_step(client_request: &ClientRequest) -> Step {
        Step {
            save_key: None,
            request: StepRequest::from(client_request.clone()),
        }
    }
}
