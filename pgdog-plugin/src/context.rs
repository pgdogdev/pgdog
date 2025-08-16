use crate::{bindings::PdRouterContext, PdQuery, PdRoute};

impl PdRouterContext {
    pub fn statement(&self) -> PdQuery {
        self.query
    }

    pub fn read_only(&self) -> bool {
        self.has_primary == 0
    }

    pub fn write_only(&self) -> bool {
        self.has_replicas == 0
    }

    pub fn has_replicas(&self) -> bool {
        !self.write_only()
    }

    pub fn has_primary(&self) -> bool {
        !self.read_only()
    }

    pub fn shards(&self) -> usize {
        self.shards as usize
    }

    pub fn sharded(&self) -> bool {
        self.shards() > 1
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Shard {
    Direct(usize),
    All,
    Unknown,
}

impl From<Shard> for i64 {
    fn from(value: Shard) -> Self {
        match value {
            Shard::Direct(value) => value as i64,
            Shard::All => -1,
            Shard::Unknown => -2,
        }
    }
}

impl TryFrom<i64> for Shard {
    type Error = ();
    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Ok(if value == -1 {
            Shard::All
        } else if value == -2 {
            Shard::Unknown
        } else if value >= 0 {
            Shard::Direct(value as usize)
        } else {
            return Err(());
        })
    }
}

impl TryFrom<u8> for ReadWrite {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(if value == 0 {
            ReadWrite::Write
        } else if value == 1 {
            ReadWrite::Read
        } else if value == 2 {
            ReadWrite::Unknown
        } else {
            return Err(());
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ReadWrite {
    Read,
    Write,
    Unknown,
}

impl From<ReadWrite> for u8 {
    fn from(value: ReadWrite) -> Self {
        match value {
            ReadWrite::Write => 0,
            ReadWrite::Read => 1,
            ReadWrite::Unknown => 2,
        }
    }
}

impl PdRoute {
    pub fn unknown() -> PdRoute {
        PdRoute {
            shard: Shard::Unknown.into(),
            read_write: ReadWrite::Unknown.into(),
        }
    }

    pub fn new(shard: Shard, read_write: ReadWrite) -> PdRoute {
        PdRoute {
            shard: shard.into(),
            read_write: read_write.into(),
        }
    }
}
