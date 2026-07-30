# PgDog 2pc WAL

This module implements a durable write-ahead log for two-phase commit transactions. Its goal is to make sure
no transactions are left in an orphaned state if PgDog crashes unexpectedly.

It roughly follows the Postgres implementation, with some minor differences.

## Architecture

The WAL has 3 components:

1. The writer, which appends records to the log
2. The checkpointer, which removes unused log segments
3. Recovery, which reads the segments at PgDog startup and restores the 2pc manager state

Clients are requesting the WAL writer to write to the log and do not touch the file themselves. The log is append only, so multiple clients can request a write at the same time. The writer takes care of synchronizing
writes (using a `Mutex<VecDeque>`) and can flush multiple records simultaneously.

## Recovery

Recovery runs at PgDog startup and replays the log, restoring the in-memory state of the 2pc manager before a crash. This is very quick: we just read the segments and insert keys into a `HashMap`. Recovery only reads, and doesn't write anything to disk.

It can naturally handle partially written segments. We didn't add checksums. Corrupted segments are mostly skipped: we read as much as we can and stop at the first sign of trouble.

## Checkpointer

It runs on a loop and removes segments that don't have any in-progress 2pc transactions. Pretty simple process, since 2pc transactions are short-lived and we are not expected to keep
any other state about them once they are done.

## Notable differences

Our WAL segment size is a suggestion. We initiate the segment swap when a segment reaches it, but we let in-flight clients write to it until the swap is complete. This is by design to avoid a lock
on the swap operation, which could take a second (creating a new file is slower than writing to an open file). Also, it makes the architecture pretty simple.

Another thing is our WAL segments are guaranteed to always always contain complete records. No record will span multiple segments. This makes recovering them much easier. This of course also contributes to
a variable segment size.

Trade-offs I made gladly.
