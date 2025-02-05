//! Handle partial CSV records.

use std::mem::take;

/// CSV buffer that supports partial records.
#[derive(Debug, Clone)]
pub struct CsvBuffer {
    buffer: Vec<u8>,
    remainder: Vec<u8>,
}

impl Default for CsvBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl CsvBuffer {
    /// New CSV buffer.
    pub fn new() -> Self {
        Self {
            buffer: vec![],
            remainder: vec![],
        }
    }

    /// Add data to buffer.
    ///
    /// TODO: Handle new lines escaped between double quotes.
    ///
    pub fn add(&mut self, data: &[u8]) {
        let nl = data.iter().rev().position(|p| *p as char == '\n');

        if let Some(nl) = nl {
            let actual = data.len() - (nl + 1);
            let remainder = take(&mut self.remainder);
            self.buffer.extend(remainder);
            self.buffer.extend(&data[..=actual]);
            if let Some(remainder) = data.get(actual + 1..) {
                self.remainder.extend(remainder);
            }
        } else {
            self.remainder.extend(data);
        }
    }

    /// Get data out of buffer.
    pub fn read(&self) -> &[u8] {
        &self.buffer
    }

    /// Clear the buffer, leaving only the remainder.
    pub fn clear(&mut self) {
        self.buffer.clear();
    }

    /// Get record as bytes.
    pub fn record(&self, start: usize) -> Option<&[u8]> {
        if let Some(slice) = self.buffer.get(start..) {
            if let Some(end) = slice.iter().position(|c| *c as char == '\n') {
                return Some(&slice[..=end]);
            }
        }
        None
    }

    /// No dangling records left.
    pub fn done(&self) -> bool {
        self.remainder.is_empty()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_csv_buffer() {
        let mut buffer = CsvBuffer::new();
        let full = "1234,test@test.com\n".as_bytes();
        buffer.add(full);
        assert_eq!(buffer.buffer, full);
        assert!(buffer.remainder.is_empty());
        assert_eq!(buffer.read(), full);
        assert_eq!(buffer.record(0), Some(full));
        buffer.clear();
        assert!(buffer.done());

        let partial = "1234,sdfsf\n4321,sddd\n11,df".as_bytes();
        buffer.add(partial);
        assert_eq!(buffer.remainder, "11,df".as_bytes());
        assert_eq!(buffer.read(), "1234,sdfsf\n4321,sddd\n".as_bytes());
        buffer.clear();
        buffer.add("\n44,test@test.com".as_bytes());
        assert_eq!(buffer.read(), "11,df\n".as_bytes());
        buffer.clear();
        assert_eq!(buffer.remainder, "44,test@test.com".as_bytes());

        let mut buffer = CsvBuffer::new();

        let in_quotes = "1234,\"hello\nworld\"\n".as_bytes();
        buffer.add(in_quotes);
        assert_eq!(buffer.read(), "1234,\"hello\nworld\"\n".as_bytes());
        assert!(buffer.remainder.is_empty());
    }
}
