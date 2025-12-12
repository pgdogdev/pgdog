/// Statement rewrite plan.
///
/// Executed in order of fields in this struct.
///
#[derive(Default)]
pub struct RewritePlan {
    /// Number of parameters ($1, $2, etc.) in
    /// the original statement. This is calculated first,
    /// and $params+n parameters are added to the statement to
    /// substitute values we are rewriting.
    pub(super) params: u16,

    /// Number of unique IDs to append to the Bind message.
    pub(super) unique_ids: u16,
}
