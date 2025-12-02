//! Rewrite engine stats.

use std::ops::Add;

#[derive(Debug, Default, Clone, Copy)]
pub struct RewriteStats {
    pub parse: usize,
    pub bind: usize,
    pub simple: usize,
}

impl Add<Self> for RewriteStats {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            parse: self.parse + rhs.parse,
            bind: self.bind + rhs.bind,
            simple: self.simple + rhs.simple,
        }
    }
}
