use lazy_static::lazy_static;

use crate::{
    backend::Cluster,
    config::ReadWriteStrategy,
    frontend::{
        buffer::BufferedQuery,
        router::{parser::Shard, Route},
    },
};

lazy_static! {
    static ref DEFAULT_ROUTE: Route = Route::default();
}

/// Keep transaction state.
#[derive(Default, Clone, Debug)]
pub struct Transaction {
    /// Statement that starts the transaction.
    begin: Option<BufferedQuery>,
    /// Last route
    last_route: Option<Route>,
    /// Read/write strategy
    rw_strategy: ReadWriteStrategy,
    /// Transaction started
    started: bool,
}

impl Transaction {
    /// Start new transaction.
    pub fn new(stmt: BufferedQuery, cluster: &Cluster) -> Self {
        Self {
            begin: Some(stmt),
            last_route: None,
            rw_strategy: *cluster.read_write_strategy(),
            started: false,
        }
    }

    /// Buffer transaction begin statement.
    pub fn buffer(&mut self, stmt: &BufferedQuery) {
        self.begin = Some(stmt.clone());
        self.started = true;
    }

    /// Start transaction.
    pub fn start(&mut self) {
        self.started = true;
    }

    pub fn finish(&mut self) {
        self.started = false;
        self.begin = None;
        self.last_route = None;
    }

    pub fn started(&self) -> bool {
        self.started
    }

    /// Execute buffered transaction.
    pub fn take_begin(&mut self) -> Option<BufferedQuery> {
        self.begin.take()
    }

    pub fn buffered(&self) -> bool {
        self.begin.is_some()
    }

    /// Set latest route information.
    pub fn set_route(&mut self, route: &Route) {
        if let Some(ref last_route) = self.last_route {
            // Make sure we don't flip from primary to replica and vice versa.
            self.last_route = Some(route.clone().set_read(last_route.is_read()));
        } else {
            self.last_route = Some(route.clone());
        }
    }

    /// Which route to use to send the begin statement.
    ///
    /// If the rw split strategy is aggressive, use route determined
    /// by last statement. Otherwise, go to the primary.
    pub fn transaction_route(&mut self) -> &Route {
        if self.buffered() {
            match self.rw_strategy {
                ReadWriteStrategy::Aggressive => self.route(),
                ReadWriteStrategy::Conservative => &DEFAULT_ROUTE,
            }
        } else {
            self.route()
        }
    }

    /// Get transaction route.
    fn route(&self) -> &Route {
        self.last_route.as_ref().unwrap_or(&DEFAULT_ROUTE)
    }
}
