use crate::frontend::client::query_engine::QueryEngineContext;
use crate::frontend::client::query_engine::multi_step::error::Error;
use crate::frontend::router::{Ast, Route};
use crate::net::{Bind, CommandComplete, DataRow, Message, Parse, RowDescription};
use dyn_clone::DynClone;
use std::fmt::Debug;

/// Responses saved from a completed `Step`
#[derive(Debug, Clone, Default)]
pub(crate) struct StepResponses {
    pub(crate) key: Option<&'static str>,
    /// Need this for getting a fresh look at the table. Avoids cache problems.
    pub(crate) row_description: Option<RowDescription>,
    pub(crate) parameter_description: Option<Message>,
    pub(crate) rows: Vec<DataRow>,
    pub(crate) command_complete: Option<CommandComplete>,
}

/// Previously completed `Step` responses
#[derive(Debug, Clone, Default)]
pub(crate) struct ResponseHistory {
    steps: Vec<StepResponses>,
}

/// We need to preserve all responses instead of just the ones we plan to look up,
/// for example, for purposes of constructing a Response based on `CommandComplete`s,
/// which is why I opted for this structure over a `HashMap`
impl ResponseHistory {
    pub(crate) fn push(&mut self, responses: StepResponses) {
        self.steps.push(responses);
    }

    pub(crate) fn get(&self, key: &str) -> Option<&StepResponses> {
        self.steps.iter().find(|step| step.key == Some(key))
    }

    pub(crate) fn steps(&self) -> &[StepResponses] {
        &self.steps
    }
}

/// The caller determines themselves what planning approach we take (based on parser checks)
#[derive(Debug, Clone)]
pub(crate) enum QueryPlannerType {
    InsertSplit,
    ShardingKeyUpdate,
    /// Runs the `ClientRequest` as one step (as normal); forwards all Responses.
    Normal,
}

/// Return this to the caller after [`QueryPlanner::plan_query`] is called, for execution later on.
/// It represents everything that should be needed to fully execute the flow of a normal request
#[derive(Debug, Clone)]
pub(crate) struct QueryPlanner {
    pub(crate) steps: Vec<Step>,
    /// This runs at the conclusion of `steps` assuming no errors or skips
    /// for how we should aggregate a Response to the Client.
    pub(crate) forward_to_client: Option<Box<dyn ForwardToClient>>,
}
#[derive(Debug, Clone)]
pub(crate) struct Step {
    /// The key that the `Step` responses are saved under in `ResponseHistory`
    pub(crate) save_key: Option<&'static str>,
    /// Statically contains or dynamically constructs the `ClientRequest`
    pub(crate) request: StepRequest,
}

/// `ClientRequest` a `Step` resolves to.
/// Assembled at execution time so we can (if we'd like) dynamically resolve
/// from prior `Step` responses.
#[derive(Debug, Clone)]
pub(crate) enum StepRequest {
    /// The client's own `ClientRequest` as-is.
    Raw,

    // We're dynamically putting something together.
    Statement(Box<StatementRequest>),
}

/// A single statement pgdog constructed for a `Step`
#[derive(Debug, Clone)]
pub(crate) struct StatementRequest {
    pub(crate) source: Box<dyn StatementSource>,
    pub(crate) protocol: StepProtocol,
    pub(crate) route: Route,
    pub(crate) ast: Option<Ast>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum StepProtocol {
    Simple,
    Extended,
}

/// Produces the statement a `Step` executes; resolved against prior `Step` responses.
/// We use this in instances where we don't know the Parse/Bind upfront (due to dependency issues)
pub(crate) trait StatementSource: Debug + DynClone + Send + Sync {
    fn resolve(&self, map: &ResponseHistory) -> Result<Option<(Parse, Bind)>, Error>;
}

/// After the conclusion of all `steps`, look at the responses (through `map`),
/// and determine what we should pretend the Server sent back (for the Client)
/// based on looking at all of them in aggregate
// TODO: I think we discussed renaming this; forgot what was suggested.
pub(crate) trait ForwardToClient: Debug + DynClone + Send + Sync {
    fn forward_to_client(&self, context: &QueryEngineContext, map: ResponseHistory)
    -> Vec<Message>;
}

// Derive Clone on all traits
dyn_clone::clone_trait_object!(ForwardToClient);
dyn_clone::clone_trait_object!(StatementSource);
