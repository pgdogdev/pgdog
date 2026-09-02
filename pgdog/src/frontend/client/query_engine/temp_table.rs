#[derive(Debug, Clone)]
pub(super) struct TempTableState {
    pub(super) committed: bool,
    pub(super) drop_on_commit: bool,
}

#[derive(Debug, Clone)]
pub(in crate::frontend) enum TempTableChange {
    Create { name: String, drop_on_commit: bool },
    Drop(String),
}
