#[derive(Debug, Clone, Default)]
pub struct Overrides {
    pub default_pool_size: Option<usize>,
    pub min_pool_size: Option<usize>,
    pub session_mode: Option<bool>,
}
