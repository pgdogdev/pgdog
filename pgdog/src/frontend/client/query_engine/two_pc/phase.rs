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

impl From<TwoPcPhase> for char {
    fn from(value: TwoPcPhase) -> Self {
        match value {
            TwoPcPhase::Phase1 => '1',
            TwoPcPhase::Phase2 => '2',
            TwoPcPhase::Rollback => 'r',
        }
    }
}

impl TryFrom<char> for TwoPcPhase {
    type Error = ();

    fn try_from(value: char) -> Result<Self, Self::Error> {
        match value {
            '1' => Ok(Self::Phase1),
            '2' => Ok(Self::Phase2),
            'r' => Ok(Self::Rollback),
            _ => Err(()),
        }
    }
}
