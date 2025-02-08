use std::{ops::Range, str::from_utf8};

/// A complete CSV record.
#[derive(Debug)]
pub struct Record {
    /// Raw record data.
    pub data: Vec<u8>,
    ///
    pub fields: Vec<Range<usize>>,
}

impl Record {
    pub(super) fn new(data: &[u8], ends: &[usize]) -> Self {
        let mut last = 0;
        let mut fields = vec![];
        for e in ends {
            fields.push(last..*e);
            last = *e;
        }
        Self {
            data: data.to_vec(),
            fields,
        }
    }

    pub fn get(&self, index: usize) -> Option<&str> {
        self.fields
            .get(index)
            .cloned()
            .map(|range| from_utf8(&self.data[range]).ok())
            .flatten()
    }
}
