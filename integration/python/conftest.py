import pytest

from globals import admin, direct_sync
from toxiproxy import toxi, reset_peer  # noqa: F401


@pytest.fixture
def idle_timeout():
    """Set idle_in_transaction_session_timeout to 500ms; reset after the test."""
    direct = direct_sync()
    direct.autocommit = True
    direct.execute(
        "ALTER USER pgdog SET idle_in_transaction_session_timeout = '50ms'"
    )

    adm = admin()
    adm.cursor().execute("RECONNECT")

    yield

    direct.execute("ALTER USER pgdog RESET idle_in_transaction_session_timeout")
    direct.close()

    adm.cursor().execute("RECONNECT")
    adm.close()

@pytest.fixture
def auth_md5():
    adm = admin()
    adm.cursor().execute("SET auth_type TO 'md5'")
    yield
    adm.cursor().execute("SET auth_type TO 'scram'")
