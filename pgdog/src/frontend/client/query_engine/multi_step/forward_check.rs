use fnv::FnvHashSet as HashSet;

use crate::{frontend::ClientRequest, net::Protocol};

#[derive(Debug, Clone)]
pub(crate) struct ForwardCheck {
    codes: HashSet<char>,
    sent: HashSet<char>,
    describe: bool,
}

impl ForwardCheck {
    /// Create new forward checker from a client request.
    ///
    /// Will construct a mapping to allow only the messages the client expects through
    ///
    pub(crate) fn new(request: &ClientRequest) -> Self {
        Self {
            codes: request.iter().map(|m| m.code()).collect(),
            describe: request.iter().any(|m| m.code() == 'D'),
            sent: HashSet::default(),
        }
    }

    /// Check if we should forward a particular message to the client.
    pub(crate) fn forward(&mut self, code: char) -> bool {
        let forward = match code {
            '1' => self.codes.contains(&'P'), // ParseComplete
            '2' => self.codes.contains(&'B'), // BindComplete
            'D' | 'E' => true,                // DataRow
            'T' => self.describe && !self.sent.contains(&'T') || self.codes.contains(&'Q'),
            't' => self.describe && !self.sent.contains(&'t'),
            _ => false,
        };

        self.sent.insert(code);

        forward
    }
}
