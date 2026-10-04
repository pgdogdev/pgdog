#[derive(Debug, Clone, PartialEq, Default)]
pub(in crate::frontend) enum Statement {
    Commit,
    Rollback,
    Begin,
    Notify,
    Listen,
    Unlisten,
    Update,
    #[allow(unused)]
    Delete,
    #[allow(unused)]
    Select,
    #[allow(unused)]
    Insert,
    Set,
    Reset,
    #[default]
    Unknown,
}
