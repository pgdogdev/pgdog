"""Session parameters must not survive the client that set them.

Runs against a database with a single server connection, so the next client
always gets the connection the previous one used. current_setting() is used
instead of SHOW because SHOW can be answered by PgDog itself.

Every test runs against two copies of that database that differ only in parser
level: "on" always parses, "auto" leaves a single-primary cluster to the regex
gate. A statement that only the gate can let through -- set_config(), a function
call inside a SELECT rather than a statement-start keyword -- reaches the parser
on one and has to earn it on the other.
"""

import uuid

import psycopg
import pytest

DATABASES = ["pgdog_leak", "pgdog_leak_auto"]


def connect(dbname):
    conn = psycopg.connect(
        user="pgdog",
        password="pgdog",
        dbname=dbname,
        host="127.0.0.1",
        port=6432,
    )
    # Otherwise psycopg wraps each statement in a transaction and rolls it
    # back on close, undoing the state we're testing for.
    conn.autocommit = True
    return conn


def read(dbname, setting):
    conn = connect(dbname)
    value = conn.execute(f"SELECT current_setting('{setting}')").fetchone()[0]
    conn.close()

    return value


@pytest.fixture(params=DATABASES)
def dbname(request):
    return request.param


def test_reset_rolled_back(dbname):
    """A ROLLBACK brings back the value the RESET cleared.

    This is the sequence pg_dump -t <table> emits. SET search_path TO '' leaves
    an empty quoted identifier, hence the two spellings of "empty".
    """
    conn = connect(dbname)
    conn.execute("SET search_path TO ''")
    with conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        conn.execute("RESET search_path")
        conn.execute("SELECT 1")
        raise psycopg.Rollback()
    conn.close()

    assert read(dbname, "search_path") not in ("", '""')


def test_set_committed_after_connecting(dbname):
    """A SET that lands once the connection is already ours."""
    conn = connect(dbname)
    with conn.transaction():
        conn.execute("SELECT 1")
        conn.execute("SET statement_timeout TO '5s'")
    conn.close()

    assert read(dbname, "statement_timeout") == "0"


def test_set_config_bound_params(dbname):
    """set_config() arguments arrive in the Bind message, not as constants."""
    conn = connect(dbname)
    conn.execute("SELECT pg_catalog.set_config(%s, %s, false)", ("search_path", ""))
    conn.close()

    assert read(dbname, "search_path") not in ("", '""')


def test_reset_committed(dbname):
    """A committed RESET is permanent and needs no undoing."""
    conn = connect(dbname)
    conn.execute("SET search_path TO public")
    with conn.transaction():
        conn.execute("SELECT 1")
        conn.execute("RESET search_path")
    conn.close()

    assert read(dbname, "search_path") == '"$user", public'


TENANT_A = "11111111-1111-1111-1111-111111111111"
TENANT_B = "22222222-2222-2222-2222-222222222222"


@pytest.fixture
def tenants(dbname):
    """A table whose rows are visible only to the tenant named in a GUC.

    Reads go through a plain role: the pooler's own user is a superuser, and
    superusers ignore row-level security however the table is configured.

    NULLIF() is deliberate: a targeted RESET leaves the placeholder GUC as an
    empty string rather than unset, and '' would fail the ::uuid cast.
    """
    table = "rls_probe_" + uuid.uuid4().hex[:8]
    conn = connect(dbname)
    conn.execute(
        "DO $$ BEGIN CREATE ROLE rls_tenant NOLOGIN; "
        "EXCEPTION WHEN duplicate_object THEN NULL; END $$"
    )
    conn.execute(f"CREATE TABLE public.{table} (org_id uuid, note text)")
    conn.execute(
        f"INSERT INTO public.{table} VALUES ('{TENANT_A}', 'a'), ('{TENANT_B}', 'b')"
    )
    conn.execute(f"GRANT SELECT ON public.{table} TO rls_tenant")
    conn.execute(f"ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY")
    conn.execute(
        f"CREATE POLICY tenant_isolation ON public.{table} USING "
        "(org_id = NULLIF(current_setting('app.current_org_id', true), '')::uuid)"
    )
    conn.close()

    yield table

    conn = connect(dbname)
    conn.execute("RESET ROLE")
    conn.execute(f"DROP TABLE public.{table}")
    conn.close()


def test_tenant_guc_does_not_outlive_its_client(dbname, tenants):
    """The silent half of the leak: no error, just another tenant's rows.

    Row-level security keyed on a custom GUC is how multi-tenant applications
    isolate tenants, and set_config() with a bound parameter is how that GUC
    gets set. A value that survives checkin makes the next client read as the
    previous tenant.
    """
    first = connect(dbname)
    first.execute("SET ROLE rls_tenant")
    first.execute(
        "SELECT pg_catalog.set_config('app.current_org_id', %s, false)", (TENANT_A,)
    )
    mine = first.execute(f"SELECT note FROM public.{tenants}").fetchall()
    first.close()

    assert mine == [("a",)], "the tenant that set the GUC sees its own row"

    second = connect(dbname)
    second.execute("SET ROLE rls_tenant")
    theirs = second.execute(f"SELECT note FROM public.{tenants}").fetchall()
    leaked = second.execute(
        "SELECT current_setting('app.current_org_id', true)"
    ).fetchone()[0]
    second.close()

    assert theirs == [], f"next client read as tenant {leaked!r}"
