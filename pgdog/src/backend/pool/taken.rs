use fnv::FnvHashMap as HashMap;

use crate::net::BackendKeyData;

use super::{Error, Mapping};

#[derive(Default, Clone, Debug)]
pub(super) struct Taken {
    taken: HashMap<usize, Mapping>,
    server_client: HashMap<BackendKeyData, usize>,
    counter: usize,
}

impl Taken {
    #[inline]
    pub(super) fn take(&mut self, mapping: &Mapping) -> Result<(), Error> {
        self.taken.insert(self.counter, *mapping);
        self.server_client.insert(mapping.server, self.counter);
        self.counter = self.counter.wrapping_add(1);
        Ok(())
    }

    #[inline]
    pub(super) fn check_in(&mut self, server: &BackendKeyData) -> Result<(), Error> {
        let counter = self
            .server_client
            .remove(server)
            .ok_or(Error::UntrackedConnCheckin(*server))?;
        self.taken
            .remove(&counter)
            .ok_or(Error::MappingMissing(counter))?;

        Ok(())
    }

    #[inline]
    pub(super) fn len(&self) -> usize {
        self.taken.len() // Both should always be the same length.
    }

    #[allow(dead_code)]
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub(super) fn server(&self, client: &BackendKeyData) -> Option<BackendKeyData> {
        self.taken
            .values()
            .find(|mapping| &mapping.client == client)
            .map(|mapping| mapping.server)
    }

    #[cfg(test)]
    pub(super) fn clear(&mut self) {
        self.taken.clear();
    }
}
