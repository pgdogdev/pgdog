use crate::{
    backend::Cluster,
    config::ReadWriteStrategy,
    frontend::{
        buffer::BufferedQuery,
        router::{parser::Shard, Route},
    },
};

/// Keep transaction state.
#[derive(Default, Clone, Debug)]
pub struct Transaction {
    /// Statement that starts the transaction.
    begin: Option<BufferedQuery>,
    /// Last route
    last_route: Route,
    /// Route for the BEGIN statement.
    begin_route: Route,
    /// Read/write strategy
    rw_strategy: ReadWriteStrategy,
    /// How many shards in the cluster?
    shards: usize,
    /// Transaction started
    started: bool,
}

impl Transaction {
    /// Start new transaction.
    pub fn new(stmt: BufferedQuery, cluster: &Cluster) -> Self {
        Self {
            begin: Some(stmt),
            last_route: Route::default(),
            begin_route: Route::default(),
            rw_strategy: *cluster.read_write_strategy(),
            shards: cluster.shards().len(),
            started: false,
        }
    }

    pub fn buffer_start(&mut self, stmt: &BufferedQuery) {
        self.begin = Some(stmt.clone());
        self.started = true;
    }

    pub fn start(&mut self) {
        self.started = true;
    }

    pub fn done(&mut self) {
        self.started = false;
        self.begin = None;
    }

    pub fn started(&self) -> bool {
        self.started
    }

    pub fn execute_start(&mut self) -> Option<BufferedQuery> {
        self.begin.take()
    }

    pub fn buffered(&self) -> bool {
        self.begin.is_some()
    }

    /// Set latest route infomation.
    pub fn set_route(&mut self, route: &Route) {
        self.last_route = route.clone();
    }

    /// Which route to use to send the begin statement.
    ///
    /// If the rw split strategy is aggressive, use route determined
    /// by last statement. Otherwise, go to the primary.
    pub fn transaction_route(&mut self) -> &Route {
        if self.buffered() {
            self.begin_route = if self.shards > 1 {
                match self.rw_strategy {
                    ReadWriteStrategy::Aggressive => self.last_route.clone(),
                    ReadWriteStrategy::Conservative => Route::write(Shard::All),
                }
            } else {
                self.last_route.clone()
            };

            &self.begin_route
        } else {
            &self.begin_route
        }
    }

    pub fn route(&mut self) -> &Route {
        if self.buffered() {
            self.transaction_route()
        } else {
            &self.last_route
        }
    }
}
