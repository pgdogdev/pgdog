use std::sync::Arc;

use bytes::{Buf, BufMut};
use pgdog_stats::User;

use super::super::super::TwoPcTransaction;
use super::Record;
use crate::net::{Payload, c_string_buf};

/// Identity of a new two-phase commit transaction.
///
/// This record also establishes Phase 1 during recovery, so the identity is
/// written only once per transaction.
///
/// # Format
///
/// | Column   | Data type | Length   |
/// |----------|-----------|----------|
/// | tid      | u64       | 8        |
/// | user     | string    | variable |
/// | database | string    | variable |
/// | gid      | string    | variable |
///
/// `gid` is the full coordinator gid the transaction was prepared with. It
/// embeds this process's `instance_id`, which a restarted PgDog does not
/// reproduce, so recovery must drive `COMMIT PREPARED` / `ROLLBACK PREPARED`
/// with this stored value rather than re-rendering it. Records written before
/// gid persistence have no trailing `gid`; the transaction then renders its
/// gid live, which is correct only in the process that created it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TwoPcRecordIdentity {
    pub(crate) transaction: TwoPcTransaction,
    pub(crate) identifier: Arc<User>,
}

impl From<TwoPcRecordIdentity> for Record {
    fn from(value: TwoPcRecordIdentity) -> Self {
        let mut payload = Payload::raw();
        payload.put_u64(value.transaction.id() as u64);
        payload.put_string(&value.identifier.user);
        payload.put_string(&value.identifier.database);
        payload.put_string(&value.transaction.to_string());

        Record {
            code: 'i',
            data: payload.freeze(),
        }
    }
}

impl TryFrom<Record> for TwoPcRecordIdentity {
    type Error = ();

    fn try_from(mut value: Record) -> Result<Self, Self::Error> {
        let tid = value.data.get_u64() as usize;
        let user = c_string_buf(&mut value.data);
        let database = c_string_buf(&mut value.data);
        // Records written before gid persistence stop here; c_string_buf
        // returns "" on the exhausted buffer and we leave the gid unset.
        let gid = c_string_buf(&mut value.data);
        let transaction = if gid.is_empty() {
            TwoPcTransaction::from_id(tid)
        } else {
            TwoPcTransaction::from_id(tid).with_gid(gid)
        };

        Ok(Self {
            transaction,
            identifier: Arc::new(User { user, database }),
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn identity_round_trips_gid() {
        // Encoding then decoding an identity record must preserve the exact
        // coordinator gid, so recovery drives COMMIT/ROLLBACK PREPARED with
        // the name Postgres holds even after a restart with a new instance_id.
        let transaction = TwoPcTransaction::new();
        let gid = transaction.to_string();
        let record = Record::from(TwoPcRecordIdentity {
            transaction,
            identifier: Arc::new(User {
                user: "alice".into(),
                database: "shop".into(),
            }),
        });

        let decoded = TwoPcRecordIdentity::try_from(record).unwrap();
        assert_eq!(decoded.transaction.to_string(), gid);
        assert_eq!(decoded.identifier.user, "alice");
        assert_eq!(decoded.identifier.database, "shop");
    }

    #[test]
    fn identity_without_gid_renders_live() {
        // A record written before gid persistence carries only tid/user/
        // database. Decoding must leave the gid unset so the transaction
        // renders from live process state (no worse than before the field
        // existed).
        let tid = 4242usize;
        let mut payload = Payload::raw();
        payload.put_u64(tid as u64);
        payload.put_string("u");
        payload.put_string("d");
        let record = Record {
            code: 'i',
            data: payload.freeze(),
        };

        let decoded = TwoPcRecordIdentity::try_from(record).unwrap();
        assert_eq!(
            decoded.transaction.to_string(),
            TwoPcTransaction::from_id(tid).to_string()
        );
    }
}
