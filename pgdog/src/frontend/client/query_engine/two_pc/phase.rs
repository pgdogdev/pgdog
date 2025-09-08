#[derive(Debug, Clone, PartialEq, Copy, Default)]
pub enum TwoPcPhase {
    #[default]
    Phase1,
    Phase2,
    Rollback,
}
