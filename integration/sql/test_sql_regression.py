import pytest

from .lib import load_suite, run_case

SUITE = load_suite()


@pytest.mark.parametrize("case", SUITE.cases, ids=lambda c: c.id)
def test_sql_regression(case):
    run_case(SUITE, case)
