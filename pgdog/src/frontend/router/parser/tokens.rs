//! PostgreSQL protocol tokens.

/// Protocol token.
#[derive(Debug)]
pub enum Token {
    /// ' '
    Space,
    /// ,
    Comma,
    /// "users"
    Entity(String),
    /// 'users'
    String(String),
    /// 5
    Integer(i64),
    /// 5.5
    Real(f64),

    /// WITH
    With,
    /// RECURSIVE
    Recursive,
    Select,
    From,
    Order,
    Limit,
    Fetch,
    For,
    Update,
    Share,
    KeyShare,
    Lateral,
    Natural,
    Join,
    Outer,
    Left,
    Right,
    Star,
    End,
}
