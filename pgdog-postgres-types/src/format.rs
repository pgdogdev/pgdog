#[derive(PartialEq, Debug, Copy, Clone, PartialOrd, Ord, Eq)]
#[repr(C)]
pub enum Format {
    Text = 0,
    Binary = 1,
}

impl From<Format> for i16 {
    fn from(val: Format) -> Self {
        match val {
            Format::Text => 0,
            Format::Binary => 1,
        }
    }
}
