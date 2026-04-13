use uuid::Uuid;

pub(super) struct Segment {
    pub(super) prefix: Uuid,
    pub(super) counter: usize,
    pub(super) data: Vec<u8>,
}
