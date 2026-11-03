use crate::net::BackendKeyData;

#[derive(Debug)]
pub(super) struct CancelQueryGuard {
    pub(super) key: BackendKeyData,
}
