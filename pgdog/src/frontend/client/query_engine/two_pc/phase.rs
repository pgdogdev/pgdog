use std::fmt::Display;

#[derive(Debug, Clone, PartialEq, Copy, Default)]
pub enum TwoPcPhase {
    #[default]
    Phase1,
    Phase2,
    Rollback,
}

impl Display for TwoPcPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Phase1 => write!(f, "phase 1"),
            Self::Phase2 => write!(f, "phase 2"),
            Self::Rollback => write!(f, "rollback"),
        }
    }
}
