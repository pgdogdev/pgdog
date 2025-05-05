use fnv::FnvHashMap as HashMap;

use crate::net::BackendKeyData;

use super::Mapping;

#[derive(Default, Clone, Debug)]
pub(super) struct Taken {
    client_server: HashMap<BackendKeyData, BackendKeyData>,
    server_client: HashMap<BackendKeyData, BackendKeyData>,
}

impl Taken {
    #[inline]
    pub(super) fn take(&mut self, mapping: &Mapping) {
        self.client_server.insert(mapping.client, mapping.server);
        self.server_client.insert(mapping.server, mapping.client);
    }

    #[inline]
    pub(super) fn check_in(&mut self, server: &BackendKeyData) {
        let client = self.server_client.remove(server);
        if let Some(client) = client {
            self.client_server.remove(&client);
        }
    }

    #[inline]
    pub(super) fn len(&self) -> usize {
        self.client_server.len() // Both should always be the same length.
    }

    #[allow(dead_code)]
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub(super) fn server(&self, client: &BackendKeyData) -> Option<BackendKeyData> {
        self.client_server.get(client).cloned()
    }

    #[allow(dead_code)]
    pub(super) fn client(&self, server: &BackendKeyData) -> Option<BackendKeyData> {
        self.server_client.get(server).cloned()
    }

    #[cfg(test)]
    pub(super) fn clear(&mut self) {
        self.client_server.clear();
        self.server_client.clear();
    }
}
