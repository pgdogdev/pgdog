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
    /// User requires a client TLS certificate but didn't provide one.
    NoClientCertificate,
    /// Passthrough auth says user doesn't exist.
    NoPassthroughNoUser,
    /// Passthrough auth doesn't allow password changes.
    NoPassthroughPasswordChange,
    /// Passthrough auth could not verify the credential against the server
    /// (the server was unreachable or returned a non-auth error).
    PassthroughVerificationFailed,
    /// No user or database in config.
    NoUserOrDatabase,
    /// Client didn't provide password message.
    NoPasswordMessage,
    /// An authentication plugin explicitly denied the client.
    PluginDenied,
    /// No authentication plugin made a decision (all skipped). The frontend
    /// uses this result to fall back to password or passthrough authentication.
    PluginNoDecision,
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
            Self::NoClientCertificate => {
                write!(
                    f,
                    "user requires a client certificate but none was provided"
                )
            }
            Self::NoPassthroughNoUser => write!(f, "no user in config (passthrough auth)"),
            Self::NoPassthroughPasswordChange => {
                write!(f, "passthrough auth does not allow password change")
            }
            Self::PassthroughVerificationFailed => {
                write!(
                    f,
                    "could not verify password against the server (passthrough auth)"
                )
            }
            Self::NoUserOrDatabase => write!(f, "no user or database in config"),
            Self::NoPasswordMessage => write!(f, "client did not send password message"),
            Self::PluginDenied => write!(f, "authentication plugin denied the client"),
            Self::PluginNoDecision => write!(f, "no authentication plugin accepted the client"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AuthResult;

    #[test]
    fn no_client_certificate_is_an_error_and_explains_itself() {
        let result = AuthResult::NoClientCertificate;

        assert!(!result.is_ok());
        assert_eq!(
            result.to_string(),
            "user requires a client certificate but none was provided"
        );
    }
}
