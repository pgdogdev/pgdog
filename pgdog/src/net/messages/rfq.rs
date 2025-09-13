//! ReadyForQuery (B) message.

use crate::net::messages::{code, prelude::*};

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum TransactionState {
    Idle,
    Error,
    InTrasaction,
}

// ReadyForQuery (F).
#[derive(Debug, Clone, Copy)]
pub struct ReadyForQuery {
    pub status: char,
}

impl ReadyForQuery {
    /// New idle message.
    pub fn idle() -> Self {
        ReadyForQuery { status: 'I' }
    }

    /// In transaction message.
    pub fn in_transaction(in_transaction: bool) -> Self {
        if in_transaction {
            ReadyForQuery { status: 'T' }
        } else {
            Self::idle()
        }
    }

    pub fn error() -> Self {
        ReadyForQuery { status: 'E' }
    }

    pub fn is_transaction_aborted(&self) -> bool {
        self.status == 'E'
    }

    /// Get transaction state.
    pub fn state(&self) -> Result<TransactionState, Error> {
        match self.status {
            'E' => Ok(TransactionState::Error),
            'T' => Ok(TransactionState::InTrasaction),
            'I' => Ok(TransactionState::Idle),
            c => Err(Error::UnknownTransactionStateIdentifier(c)),
        }
    }
}

impl ToBytes for ReadyForQuery {
    fn to_bytes(&self) -> Result<bytes::Bytes, Error> {
        let mut payload = Payload::named(self.code());
        payload.put_u8(self.status as u8);

        Ok(payload.freeze())
    }
}

impl FromBytes for ReadyForQuery {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'Z');

        let _len = bytes.get_i32();
        let status = bytes.get_u8() as char;

        Ok(Self { status })
    }
}

impl Protocol for ReadyForQuery {
    fn code(&self) -> char {
        'Z'
    }
}
