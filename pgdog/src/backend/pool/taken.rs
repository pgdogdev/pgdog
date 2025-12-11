use fnv::FnvHashMap as HashMap;

use crate::net::BackendKeyData;

use super::{Error, Mapping};

#[derive(Default, Clone, Debug)]
pub(super) struct Taken {
    /// Guaranteed to be unique per client/server connection.
    taken: HashMap<usize, Mapping>,
    /// Guaranteed to be unique because servers can only be mapped
    /// to one client at a time.
    server_client: HashMap<BackendKeyData, usize>,
    /// Not unique, but will contain the server that's actively executing a query
    /// for that client.
    client_server: HashMap<BackendKeyData, BackendKeyData>,
    /// Counter that guarantees uniqueness. Wraparound happens after a gazillion billion transactions.
    counter: usize,
}

impl Taken {
    #[inline]
    pub(super) fn take(&mut self, mapping: &Mapping) -> Result<(), Error> {
        self.taken.insert(self.counter, *mapping);
        self.server_client.insert(mapping.server, self.counter);
        self.client_server.insert(mapping.client, mapping.server);
        self.counter = self.counter.wrapping_add(1);
        Ok(())
    }

    #[inline]
    pub(super) fn check_in(&mut self, server: &BackendKeyData) -> Result<(), Error> {
        let counter = self
            .server_client
            .remove(server)
            .ok_or(Error::UntrackedConnCheckin(*server))?;
        let mapping = self
            .taken
            .remove(&counter)
            .ok_or(Error::MappingMissing(counter))?;
        self.client_server.remove(&mapping.client);

        Ok(())
    }

    #[inline]
    pub(super) fn len(&self) -> usize {
        self.taken.len()
    }

    #[allow(dead_code)]
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub(super) fn server(&self, client: &BackendKeyData) -> Option<BackendKeyData> {
        self.client_server.get(client).copied()
    }

    #[cfg(test)]
    pub(super) fn clear(&mut self) {
        self.taken.clear();
    }
}
