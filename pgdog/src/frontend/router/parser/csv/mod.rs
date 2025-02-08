use csv_core::{ReadRecordResult, Reader, ReaderBuilder};

pub mod iterator;
pub mod record;

pub use iterator::Iter;
pub use record::Record;

/// CSV reader that can handle partial inputs.
pub struct CsvStream {
    buffer: Vec<u8>,
    record: Vec<u8>,
    ends: Vec<usize>,
    reader: Reader,
}

impl CsvStream {
    /// Create new CSV stream reader.
    pub fn new(delimiter: char) -> Self {
        Self {
            buffer: Vec::new(),
            record: vec![0u8; 4096],
            ends: vec![0usize; 4096],
            reader: ReaderBuilder::new()
                .delimiter(delimiter as u8)
                .double_quote(true)
                .build(),
        }
    }

    pub fn write(&mut self, data: &[u8]) {
        self.buffer.extend(data);
    }

    pub fn record(&mut self) -> Option<Record> {
        loop {
            let (result, read, written, ends) =
                self.reader
                    .read_record(&self.buffer, &mut self.record, &mut self.ends);
            match result {
                ReadRecordResult::OutputFull => {
                    self.record.resize(self.buffer.len() * 2, 0u8);
                }

                // Data incomplete.
                ReadRecordResult::InputEmpty | ReadRecordResult::End => {
                    return None;
                }

                ReadRecordResult::Record => {
                    let record = Record::new(&self.record[..written], &self.ends[..ends]);
                    self.record.clear();
                    self.buffer = Vec::from(&self.buffer[read..]);
                    return Some(record);
                }

                ReadRecordResult::OutputEndsFull => {
                    self.ends.resize(self.ends.len() * 2, 0usize);
                }
            }
        }
    }

    /// Get an iterator over the records.
    pub fn records(&mut self) -> Iter<'_> {
        Iter::new(self)
    }
}

#[cfg(test)]
mod test {
    use super::CsvStream;

    #[test]
    fn test_csv() {
        let csv = "one,two,three\none,two";
        let mut reader = CsvStream::new(',');
        reader.write(csv.as_bytes());
        let record = reader.record().unwrap();
        assert_eq!(record.get(0), Some("one"));
        assert_eq!(record.get(1), Some("two"));
        assert_eq!(record.get(2), Some("three"));

        reader.write(",four\n".as_bytes());
        let record = reader.record().unwrap();
        assert_eq!(record.get(2), Some("four"));

        assert!(reader.record().is_none());
    }
}
