use super::{CsvStream, Record};

pub struct Iter<'a> {
    csv: &'a mut CsvStream,
}

impl<'a> Iter<'a> {
    pub(super) fn new(csv: &'a mut CsvStream) -> Self {
        Self { csv }
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        self.csv.record()
    }
}
