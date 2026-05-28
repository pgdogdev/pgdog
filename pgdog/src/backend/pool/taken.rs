use fnv::FnvHashMap as HashMap;

use crate::net::{BackendKeyData, BackendPid};

use super::{Error, Mapping};

#[derive(Default, Clone, Debug)]
pub(super) struct Taken {
    /// Guaranteed to be unique per client/server connection.
    taken: HashMap<usize, Mapping>,
    /// Guaranteed to be unique because servers can only be mapped to one client at a time.
    server_client: HashMap<BackendPid, usize>,
    /// Not unique, but will contain the server cancel key for the server actively executing
    /// a query for that client. The server pid is recovered via `key.pid()`.
    client_server: HashMap<BackendPid, BackendKeyData>,
    /// Counter that guarantees uniqueness.
    counter: usize,
}

impl Taken {
    #[inline]
    pub(super) fn take(
        &mut self,
        mapping: &Mapping,
        cancel_key: BackendKeyData,
    ) -> Result<(), Error> {
        self.taken.insert(self.counter, *mapping);
        self.server_client.insert(mapping.server, self.counter);
        self.client_server.insert(mapping.client, cancel_key);
        self.counter = self.counter.wrapping_add(1);
        Ok(())
    }

    #[inline]
    pub(super) fn check_in(&mut self, server: BackendPid) -> Result<(), Error> {
        let counter = self
            .server_client
            .remove(&server)
            .ok_or(Error::UntrackedConnCheckin(server))?;
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

    /// Server cancel key for the server currently assigned to this client.
    #[inline]
    pub(super) fn cancel_key(&self, client: BackendPid) -> Option<&BackendKeyData> {
        self.client_server.get(&client)
    }

    /// All cancel keys for currently checked-out server connections.
    pub(super) fn cancel_keys(&self) -> impl Iterator<Item = &BackendKeyData> {
        self.client_server.values()
    }

    #[cfg(test)]
    pub(super) fn clear(&mut self) {
        self.taken.clear();
    }
}
