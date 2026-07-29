# PgDog 2pc WAL

This module implements a durable write-ahead log for two-phase commit transactions. Its goal is to make sure
no transactions are left in an orphaned state if PgDog crashes unexpectedly.

It roughly follows the Postgres implementation, with some minor differences.

## Architecture

The WAL has 3 components:

1. The writer, which appends records to the log
2. The checkpointer, which removes unused log segments
3. Recovery, which reads the segments at PgDog startup and restores the 2pc manager state

Clients are responsible for writing to the log. The log is append only, so multiple clients can write at the same time. The writer takes care of synchronizing
writes (using a `Mutex<VecDeque>`) and can flush multiple records simultaneously.

## Recovery

Recovery runs at PgDog startup and replays the log, restoring the in-memory state of the 2pc manager before a crash. This is very quick: we just read the segments and insert keys into a `HashMap`. Recovery only reads, and doesn't write anything to disk.

It can naturally handle partially written segments and even corrupted ones.

## Checkpointer

It runs on a loop and removes segments that don't have any in-progress 2pc transactions. Pretty simple process, since 2pc transactions are short-lived and we are not expected to keep
any other state about them once they are done.
