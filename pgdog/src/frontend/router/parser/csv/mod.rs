use csv_core::{ReadRecordResult, Reader, ReaderBuilder};

pub mod iterator;
pub mod record;

pub use iterator::Iter;
pub use record::Record;

/// CSV reader that can handle partial inputs.
pub struct CsvStream {
    /// Input buffer.
    buffer: Vec<u8>,
    /// Temporary buffer for the record.
    record: Vec<u8>,
    /// Temporary buffer for indices for the fields in a record.
    ends: Vec<usize>,
    /// CSV reader.
    reader: Reader,
}

impl CsvStream {
    /// Create new CSV stream reader.
    pub fn new(delimiter: char) -> Self {
        Self {
            buffer: Vec::new(),
            record: Vec::new(),
            ends: vec![0usize; 2048],
            reader: ReaderBuilder::new()
                .delimiter(delimiter as u8)
                .double_quote(true)
                .build(),
        }
    }

    /// Write some data to the CSV stream.
    ///
    /// This data will be appended to the input buffer. To read records from
    /// that stream, call [`Self::record`].
    pub fn write(&mut self, data: &[u8]) {
        self.buffer.extend(data);
    }

    /// Fetch a record from the stream. This mutates the inner buffer,
    /// so you can only fetch the record once.
    pub fn record(&mut self) -> Option<Record> {
        loop {
            let (result, read, written, ends) =
                self.reader
                    .read_record(&self.buffer, &mut self.record, &mut self.ends);

            match result {
                ReadRecordResult::OutputFull => {
                    self.record.resize(self.buffer.len() * 2 + 1, 0u8);
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
                    self.ends.resize(self.ends.len() * 2 + 1, 0usize);
                }
            }
        }
    }

    /// Get an iterator over all records available in the buffer.
    pub fn records(&mut self) -> Iter<'_> {
        Iter::new(self)
    }
}

#[cfg(test)]
mod test {
    use super::CsvStream;

    #[test]
    fn test_csv() {
        let csv = "on,two,three\none,two";
        let mut reader = CsvStream::new(',');
        reader.write(csv.as_bytes());
        let record = reader.record().unwrap();
        assert_eq!(record.get(0), Some("on"));
        assert_eq!(record.get(1), Some("two"));
        assert_eq!(record.get(2), Some("three"));

        reader.write(",four\n".as_bytes());
        let record = reader.record().unwrap();
        assert_eq!(record.get(2), Some("four"));

        assert!(reader.record().is_none());
    }
}
