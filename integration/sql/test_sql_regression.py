import pytest

from .lib import build_matrix, load_suite, run_case

SUITE = load_suite()
MATRIX = build_matrix(SUITE)


@pytest.mark.parametrize("entry", MATRIX, ids=lambda cp: f"{cp.case.id}::{cp.pair.name}")
def test_sql_regression(entry):
    case = entry.case
    pair = entry.pair
    baseline_cfg = SUITE.lookup_target(pair.baseline)
    candidate_cfg = SUITE.lookup_target(pair.candidate)
    run_case(SUITE, case, baseline_cfg, candidate_cfg)
