//! Handle transaction cleanup.

use crate::{
    backend::pool::Connection,
    frontend::{Comms, Error, Stats},
    net::Parameters,
};
use tracing::debug;

pub struct Cleanup<'a> {
    backend: &'a mut Connection,
    stats: &'a mut Stats,
    params: &'a mut Parameters,
    comms: &'a Comms,
}

impl<'a> Cleanup<'a> {
    pub fn new(
        backend: &'a mut Connection,
        stats: &'a mut Stats,
        params: &'a mut Parameters,
        comms: &'a mut Comms,
    ) -> Self {
        Self {
            backend,
            stats,
            params,
            comms,
        }
    }

    pub fn handle(&mut self) -> Result<(), Error> {
        let changed_params = self.backend.changed_params();

        // Update client params with values
        // sent from the server using ParameterStatus(B) messages.
        if !changed_params.is_empty() {
            for (name, value) in changed_params.iter() {
                debug!("setting client's \"{}\" to {}", name, value);
                self.params.insert(name.clone(), value.clone());
            }
            self.comms.update_params(&self.params);
        }

        // Release servers.
        if self.backend.transaction_mode() {
            self.backend.disconnect();
        }

        debug!(
            "transaction finished [{:.3}ms]",
            self.stats.last_transaction_time.as_secs_f64() * 1000.0
        );

        Ok(())
    }
}
