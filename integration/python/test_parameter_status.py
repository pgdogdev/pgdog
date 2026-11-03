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
        "options": "-c TimeZone=Europe/Paris -c IntervalStyle=sql_standard"
    },
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


@pytest.mark.parametrize("startup", STARTUP.values(), ids=STARTUP.keys())
def test_parameter_status_matches_postgres(startup):
    assert reports(6432, startup) == reports(5432, startup)
