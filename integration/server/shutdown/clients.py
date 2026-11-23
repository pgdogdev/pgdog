import sys

import psycopg

SCHEMA = "pgdog_shutdown"
# Sessions per database, so the drain must wait for several clients.
CLIENTS = 3
# Inserts per session during the drain, the same count as the old complex/shutdown.py.
DRAIN_QUERIES = 25
# Postgres databases that store the rows of each pgdog database.
BACKENDS = {"pgdog": ["pgdog"], "pgdog_sharded": ["shard_0", "shard_1"]}


def connect(database, port=6432):
    return psycopg.connect(
        host="127.0.0.1",
        port=port,
        dbname=database,
        user="pgdog",
        password="pgdog",
        autocommit=True,
    )


class Session:
    def __init__(self, number, database):
        self.number = number
        self.database = database
        self.ids = []
        self.conn = connect(database)
        self.conn.execute("BEGIN")
        self.insert()

    def insert(self):
        row = self.number * 1_000_000 + len(self.ids)
        returned = self.conn.execute(
            f"INSERT INTO {SCHEMA}.clients (id, client) VALUES (%s, %s) RETURNING id",
            (row, self.number),
        ).fetchone()
        if returned != (row,):
            sys.exit(f"insert into {self.database} returned {returned}, expected ({row},)")
        self.ids.append(row)

    def stored(self):
        ids = []
        for backend in BACKENDS[self.database]:
            with connect(backend, port=5432) as conn:
                ids += [
                    row[0]
                    for row in conn.execute(
                        f"SELECT id FROM {SCHEMA}.clients WHERE client = %s", (self.number,)
                    )
                ]
        return sorted(ids)


def check(sessions):
    for session in sessions:
        session.insert()
    print("ok", flush=True)


def release(sessions):
    for session in sessions:
        for _ in range(DRAIN_QUERIES):
            session.insert()
        session.conn.execute("COMMIT")
        session.conn.close()
        if session.stored() != session.ids:
            sys.exit(f"rows of session {session.number} on {session.database} were not committed in Postgres")


def terminated(sessions):
    for session in sessions:
        try:
            session.conn.execute("SELECT 1")
        except psycopg.OperationalError:
            pass
        else:
            sys.exit(f"connection to {session.database} is still open after shutdown")
        if session.stored():
            sys.exit(f"rows of session {session.number} on {session.database} were committed in Postgres")


sessions = []
for database in BACKENDS:
    for _ in range(CLIENTS):
        sessions.append(Session(len(sessions), database))

print("held", flush=True)

for line in sys.stdin:
    command = line.strip()
    if command == "check":
        check(sessions)
    elif command == "release":
        release(sessions)
        break
    elif command == "terminated":
        terminated(sessions)
        break
    else:
        sys.exit(f"unknown command: {command!r}")
