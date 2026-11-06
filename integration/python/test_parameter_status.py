import psycopg
import pytest

REPORTED = [
    "client_encoding",
    "DateStyle",
    "IntervalStyle",
    "TimeZone",
    "search_path",
    "standard_conforming_strings",
]

STARTUP = {
    "no_options": {},
    "lowercase_client_encoding": {"client_encoding": "utf8"},
    "options": {
        "options": "-c TimeZone=Europe/Paris -c IntervalStyle=sql_standard -c DateStyle=sql,dmy"
    },
    "partial_datestyle": {"options": "-c DateStyle=YMD"},
    "lowercase_datestyle": {"options": "-c datestyle=german"},
}

DATESTYLE_STARTUP = {
    "full": "-c DateStyle=sql,dmy",
    "partial_order": "-c DateStyle=YMD",
    "partial_style": "-c DateStyle=german",
    "lowercase_name": "-c datestyle=iso,ymd",
}


def connect(port, startup):
    conn = psycopg.connect(
        user="pgdog",
        password="pgdog",
        dbname="pgdog",
        host="127.0.0.1",
        port=port,
        **startup,
    )
    conn.autocommit = True
    return conn


def status(conn, name):
    value = conn.pgconn.parameter_status(name.encode())
    return None if value is None else value.decode()


def reports(port, startup):
    with connect(port, startup) as conn:
        result = {f"login {name}": status(conn, name) for name in REPORTED}
        for name, value in [
            ("TimeZone", "America/New_York"),
            ("DateStyle", "Postgres"),
            ("IntervalStyle", "iso_8601"),
        ]:
            conn.execute(f"SET {name} = '{value}'")
            result[f"SET {name}"] = status(conn, name)
            conn.execute(f"RESET {name}")
            result[f"RESET {name}"] = status(conn, name)
        return result


def datestyle_reports(port, options):
    with connect(port, {"options": options}) as conn:
        result = {"login": status(conn, "DateStyle")}
        result["SHOW at login"] = conn.execute("SHOW datestyle").fetchone()[0]
        for query in [
            "SET datestyle = 'SQL'",
            "SET datestyle = 'DMY'",
            "RESET datestyle",
            "SET datestyle = 'Postgres'; RESET datestyle; SET datestyle = 'German'",
            "RESET datestyle",
        ]:
            conn.execute(query)
            result[query] = status(conn, "DateStyle")
        result["SHOW at end"] = conn.execute("SHOW datestyle").fetchone()[0]
        result["date text"] = conn.execute("SELECT '2026-03-04'::date::text").fetchone()[0]
        return result


@pytest.mark.parametrize("startup", STARTUP.values(), ids=STARTUP.keys())
def test_parameter_status_matches_postgres(startup):
    assert reports(6432, startup) == reports(5432, startup)


@pytest.mark.parametrize(
    "options", DATESTYLE_STARTUP.values(), ids=DATESTYLE_STARTUP.keys()
)
def test_startup_datestyle_matches_postgres(options):
    assert datestyle_reports(6432, options) == datestyle_reports(5432, options)
