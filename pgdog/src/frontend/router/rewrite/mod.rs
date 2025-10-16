use crate::frontend::router::parser::{Command, Error as ParserError, QueryParserContext, Route};

/// Enum describing supported rewrite plans.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RewriteExecutionKind {
    /// Placeholder variant for future rewrites.
    Unspecified,
}

/// Planner output describing the rewrite to execute.
#[derive(Debug, Clone)]
pub struct RewriteExecutionPlan {
    kind: RewriteExecutionKind,
    route: Route,
}

impl RewriteExecutionPlan {
    pub fn new(kind: RewriteExecutionKind, route: Route) -> Self {
        Self { kind, route }
    }

    pub fn kind(&self) -> &RewriteExecutionKind {
        &self.kind
    }

    pub fn route(&self) -> &Route {
        &self.route
    }

    pub fn route_mut(&mut self) -> &mut Route {
        &mut self.route
    }
}

/// Input provided to rewrite planners.
#[derive(Debug)]
pub enum PlannerInput<'a> {
    /// Full router command before rewrite handling.
    Command(&'a Command),
}

/// Shared planner context exposing parser metadata.
pub struct PlannerContext<'a, 'b> {
    context: &'a QueryParserContext<'b>,
}

impl<'a, 'b> PlannerContext<'a, 'b> {
    pub fn new(context: &'a QueryParserContext<'b>) -> Self {
        Self { context }
    }

    pub fn parser_context(&self) -> &'a QueryParserContext<'b> {
        self.context
    }
}

pub trait RewritePlanner {
    fn plan(
        &self,
        _planner_context: &PlannerContext<'_, '_>,
        _input: &PlannerInput<'_>,
    ) -> Result<Option<RewriteExecutionPlan>, ParserError>;
}

pub struct RewriteRegistry {
    planners: Vec<Box<dyn RewritePlanner + Send + Sync>>,
}

impl RewriteRegistry {
    pub fn new() -> Self {
        Self {
            planners: Vec::new(),
        }
    }

    pub fn add_planner(&mut self, planner: Box<dyn RewritePlanner + Send + Sync>) {
        self.planners.push(planner);
    }

    pub fn plan(
        &self,
        context: &PlannerContext<'_, '_>,
        input: &PlannerInput<'_>,
    ) -> Result<Option<RewriteExecutionPlan>, ParserError> {
        for planner in &self.planners {
            if let Some(plan) = planner.plan(context, input)? {
                return Ok(Some(plan));
            }
        }

        Ok(None)
    }
}

impl Default for RewriteRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for RewriteRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RewriteRegistry").finish()
    }
}
