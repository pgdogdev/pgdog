use std::fmt::Display;

#[derive(Default, PartialEq, Debug, Clone, Copy)]
pub enum AuthResult {
    /// No problems.
    #[default]
    Ok,
    /// Password provided by user doesn't match config.
    NoPasswordMatch,
    /// Passwords not configured.
    NoPasswordConfig,
    /// User identity (TLS cert) doesn't match configured identity.
    NoIdentity,
    /// Passthrough auth says user doesn't exist.
    NoPassthroughNoUser,
    /// Passthrough auth doesn't allow password changes.
    NoPassthroughPasswordChange,
    /// No user or database in config.
    NoUserOrDatabase,
    /// Client didn't provide password message.
    NoPasswordMessage,
}

impl AuthResult {
    pub fn is_ok(&self) -> bool {
        matches!(self, Self::Ok)
    }
}

impl PartialEq<bool> for AuthResult {
    fn eq(&self, other: &bool) -> bool {
        self.is_ok() == *other
    }
}

impl Display for AuthResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ok => write!(f, "auth ok"),
            Self::NoPasswordMatch => write!(f, "wrong password"),
            Self::NoPasswordConfig => write!(f, "user has no passwords in config"),
            Self::NoIdentity => write!(f, "user identity does not match certificate"),
            Self::NoPassthroughNoUser => write!(f, "no user in config (passthrough auth)"),
            Self::NoPassthroughPasswordChange => {
                write!(f, "passthrough auth does not allow password change")
            }
            Self::NoUserOrDatabase => write!(f, "no user or database in config"),
            Self::NoPasswordMessage => write!(f, "client did not send password message"),
        }
    }
}
