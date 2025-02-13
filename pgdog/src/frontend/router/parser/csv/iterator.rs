use super::{super::Error, CsvStream, Record};

pub struct Iter<'a> {
    csv: &'a mut CsvStream,
}

impl<'a> Iter<'a> {
    pub(super) fn new(csv: &'a mut CsvStream) -> Self {
        Self { csv }
    }
}

impl Iterator for Iter<'_> {
    type Item = Result<Record, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.csv.record().transpose()
    }
}
