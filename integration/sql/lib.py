from __future__ import annotations

import re
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import psycopg
import psycopg.rows
import sqlparse


@dataclass(frozen=True)
class ConnectionConfig:
    name: str
    dsn: str
    format: str  # text | binary
    tags: frozenset[str]

    def cursor_kwargs(self) -> dict:
        if self.format not in {"text", "binary"}:
            raise ValueError(f"unsupported format {self.format} for {self.name}")
        return {"binary": self.format == "binary"}

    def connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self.dsn)
        conn.autocommit = False
        conn.row_factory = psycopg.rows.tuple_row
        return conn


@dataclass(frozen=True)
class CaseDefinition:
    id: str
    description: str
    path: Path
    setup: Path | None
    teardown: Path | None
    tags: frozenset[str]
    transactional: bool
    only_targets: Tuple[str, ...]
    skip_targets: frozenset[str]


@dataclass
class StatementResult:
    sql: str
    status: str
    rowcount: int
    columns: Sequence[str]
    type_names: Sequence[str]
    rows: Sequence[tuple]


@dataclass(frozen=True)
class CaseMetadata:
    description: str
    tags: frozenset[str]
    transactional: bool
    only_targets: Tuple[str, ...]
    skip_targets: frozenset[str]


class TypeRegistry:
    def __init__(self) -> None:
        self._cache: dict[int, str] = {}

    def resolve(self, conn: psycopg.Connection, oids: Iterable[int]) -> List[str]:
        resolved: List[str] = []
        missing: list[int] = []
        for oid in oids:
            if oid in self._cache:
                resolved.append(self._cache[oid])
            else:
                missing.append(oid)
                resolved.append("<pending>")
        if missing:
            query = "SELECT oid, format_type(oid, NULL) FROM pg_type WHERE oid = ANY(%s)"
            with conn.cursor() as cur:
                cur.execute(query, (missing,))
                for oid, name in cur.fetchall():
                    self._cache[int(oid)] = name
        final: List[str] = []
        for oid in oids:
            final.append(self._cache.get(oid, f"unknown({oid})"))
        return final


@dataclass(frozen=True)
class SuiteConfig:
    root: Path
    targets: Tuple[ConnectionConfig, ...]
    target_index: Dict[str, ConnectionConfig]
    cases: List[CaseDefinition]
    global_setup: Path | None
    global_teardown: Path | None

    def lookup_target(self, name: str) -> ConnectionConfig:
        try:
            return self.target_index[name]
        except KeyError as exc:
            raise KeyError(f"unknown connection target '{name}'") from exc

    def targets_for_case(self, case: CaseDefinition) -> List[ConnectionConfig]:
        if case.only_targets:
            missing = [name for name in case.only_targets if name not in self.target_index]
            if missing:
                raise KeyError(
                    f"case '{case.id}' references unknown targets: {', '.join(missing)}"
                )
            ordered = [self.target_index[name] for name in case.only_targets]
        else:
            ordered = list(self.targets)
        filtered = [cfg for cfg in ordered if cfg.name not in case.skip_targets]
        if not filtered:
            raise ValueError(f"case '{case.id}' has no remaining targets to execute")
        return filtered


STANDARD_DSN = "postgresql://pgdog:pgdog@127.0.0.1:5432/pgdog?gssencmode=disable"
PGDOG_DSN = "postgresql://pgdog:pgdog@127.0.0.1:6432/pgdog?gssencmode=disable"
SHARDED_DSN = "postgresql://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded?gssencmode=disable"


DEFAULT_TARGETS: Tuple[ConnectionConfig, ...] = (
    ConnectionConfig(
        name="postgres_standard_text",
        dsn=STANDARD_DSN,
        format="text",
        tags=frozenset({"standard"}),
    ),
    ConnectionConfig(
        name="postgres_standard_binary",
        dsn=STANDARD_DSN,
        format="binary",
        tags=frozenset({"standard"}),
    ),
    ConnectionConfig(
        name="pgdog_standard_text",
        dsn=PGDOG_DSN,
        format="text",
        tags=frozenset({"standard"}),
    ),
    ConnectionConfig(
        name="pgdog_standard_binary",
        dsn=PGDOG_DSN,
        format="binary",
        tags=frozenset({"standard"}),
    ),
    ConnectionConfig(
        name="pgdog_sharded_text",
        dsn=SHARDED_DSN,
        format="text",
        tags=frozenset({"sharded"}),
    ),
    ConnectionConfig(
        name="pgdog_sharded_binary",
        dsn=SHARDED_DSN,
        format="binary",
        tags=frozenset({"sharded"}),
    ),
)


def load_suite(
    root: Path | None = None,
    targets: Sequence[ConnectionConfig] | None = None,
) -> SuiteConfig:
    root = root or Path(__file__).resolve().parent
    target_list = tuple(targets or DEFAULT_TARGETS)
    target_index = {cfg.name: cfg for cfg in target_list}
    cases = discover_cases(root / "cases")
    global_setup = root / "global_setup.sql"
    if not global_setup.exists():
        global_setup = None
    global_teardown = root / "global_teardown.sql"
    if not global_teardown.exists():
        global_teardown = None
    return SuiteConfig(
        root=root,
        targets=target_list,
        target_index=target_index,
        cases=cases,
        global_setup=global_setup,
        global_teardown=global_teardown,
    )


def discover_cases(cases_dir: Path) -> List[CaseDefinition]:
    if not cases_dir.exists():
        return []
    pattern = re.compile(
        r"^(?P<index>\d+?)_(?P<slug>[a-z0-9_]+)_(?P<section>setup|case|teardown)\.sql$"
    )
    grouped: Dict[Tuple[str, str], Dict[str, Path]] = {}
    for path in sorted(cases_dir.glob("*.sql")):
        match = pattern.match(path.name)
        if not match:
            continue
        key = (match.group("index"), match.group("slug"))
        grouped.setdefault(key, {})[match.group("section")] = path
    cases: List[CaseDefinition] = []
    for (index, slug), sections in sorted(grouped.items(), key=lambda item: (int(item[0][0]), item[0][1])):
        case_path = sections.get("case")
        if not case_path:
            raise ValueError(f"missing case file for scenario {index}_{slug}")
        metadata = parse_case_metadata(case_path)
        cases.append(
            CaseDefinition(
                id=f"{index}_{slug}",
                description=metadata.description,
                path=case_path,
                setup=sections.get("setup"),
                teardown=sections.get("teardown"),
                tags=metadata.tags,
                transactional=metadata.transactional,
                only_targets=metadata.only_targets,
                skip_targets=metadata.skip_targets,
            )
        )
    return cases


def parse_case_metadata(path: Path) -> CaseMetadata:
    tags: List[str] = []
    description = ""
    transactional = True
    only_targets: List[str] = []
    skip_targets: List[str] = []
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if not stripped.startswith("--"):
            break
        body = stripped[2:].strip()
        if ":" not in body:
            continue
        key, value = (part.strip() for part in body.split(":", 1))
        key_lower = key.lower()
        if key_lower == "tags":
            tokens = [token.lower() for token in _split_metadata_tokens(value)]
            tags = tokens
        elif key_lower == "description":
            description = value
        elif key_lower == "transactional":
            transactional = value.lower() not in {"false", "0", "no"}
        elif key_lower == "only-targets":
            only_targets = _split_metadata_tokens(value)
        elif key_lower == "skip-targets":
            skip_targets = _split_metadata_tokens(value)
    if not tags:
        tags = ["standard"]
    return CaseMetadata(
        description=description,
        tags=frozenset(tags),
        transactional=transactional,
        only_targets=tuple(only_targets),
        skip_targets=frozenset(skip_targets),
    )


def strip_metadata_header(text: str) -> str:
    metadata_keys = {"tags", "description", "transactional", "only-targets", "skip-targets"}
    lines = text.splitlines()
    index = 0
    while index < len(lines):
        stripped = lines[index].strip()
        if not stripped:
            index += 1
            continue
        if stripped.startswith("--"):
            body = stripped[2:].strip()
            if ":" in body:
                key = body.split(":", 1)[0].strip().lower()
                if key in metadata_keys:
                    index += 1
                    continue
        break
    return "\n".join(lines[index:])


def parse_sql_file(path: Path) -> List[str]:
    text = strip_metadata_header(path.read_text())
    statements = [stmt.strip() for stmt in sqlparse.split(text) if stmt.strip()]
    return statements


def _split_metadata_tokens(value: str) -> List[str]:
    cleaned = value.replace(",", " ")
    return [token.strip() for token in cleaned.split() if token.strip()]


def execute_sql_file(target: ConnectionConfig, path: Path | None) -> None:
    if path is None:
        return
    if not path.exists():
        return
    statements = parse_sql_file(path)
    if not statements:
        return
    conn = target.connect()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
    finally:
        conn.close()


def run_case(
    suite: SuiteConfig,
    case: CaseDefinition,
) -> None:
    targets = suite.targets_for_case(case)
    setup_targets = _unique_setup_targets(targets)
    try:
        for cfg in setup_targets:
            if suite.global_setup:
                execute_sql_file(cfg, suite.global_setup)
            execute_sql_file(cfg, case.setup)

        results: Dict[str, List[StatementResult]] = {}
        for cfg in targets:
            results[cfg.name] = _collect_results(case, cfg)

        _assert_all_equal(case, targets, results)
    finally:
        for cfg in reversed(setup_targets):
            execute_sql_file(cfg, case.teardown)
            if suite.global_teardown:
                execute_sql_file(cfg, suite.global_teardown)


def _unique_setup_targets(targets: Sequence[ConnectionConfig]) -> List[ConnectionConfig]:
    unique: OrderedDict[str, ConnectionConfig] = OrderedDict()
    for cfg in targets:
        unique.setdefault(cfg.dsn, cfg)
    return list(unique.values())


def _collect_results(case: CaseDefinition, cfg: ConnectionConfig) -> List[StatementResult]:
    statements = parse_sql_file(case.path)
    conn = cfg.connect()
    registry = TypeRegistry()
    try:
        results: List[StatementResult] = []
        with conn.cursor(**cfg.cursor_kwargs()) as cur:
            for stmt in statements:
                stmt_clean = stmt.strip()
                if not stmt_clean:
                    continue
                cur.execute(stmt_clean)
                status = cur.statusmessage or "OK"
                rowcount = cur.rowcount
                columns: Sequence[str] = []
                type_names: Sequence[str] = []
                rows: Sequence[tuple] = []
                if cur.description:
                    columns = tuple(desc.name for desc in cur.description)
                    oids = [desc.type_code for desc in cur.description]
                    type_names = tuple(registry.resolve(conn, oids))
                    rows = tuple(tuple(row) for row in cur.fetchall())
                results.append(
                    StatementResult(
                        sql=stmt_clean,
                        status=status,
                        rowcount=rowcount,
                        columns=columns,
                        type_names=type_names,
                        rows=rows,
                    )
                )
        if case.transactional:
            conn.rollback()
        else:
            conn.commit()
        return results
    finally:
        conn.close()


def _assert_all_equal(
    case: CaseDefinition,
    ordered_targets: Sequence[ConnectionConfig],
    results: Dict[str, Sequence[StatementResult]],
) -> None:
    baseline_cfg = ordered_targets[0]
    baseline_results = results[baseline_cfg.name]
    for candidate_cfg in ordered_targets[1:]:
        candidate_results = results[candidate_cfg.name]
        _assert_pair_equal(case, baseline_cfg, candidate_cfg, baseline_results, candidate_results)


def _assert_pair_equal(
    case: CaseDefinition,
    baseline_cfg: ConnectionConfig,
    candidate_cfg: ConnectionConfig,
    baseline_results: Sequence[StatementResult],
    candidate_results: Sequence[StatementResult],
) -> None:
    if len(baseline_results) != len(candidate_results):
        raise AssertionError(
            f"Case '{case.id}' produced {len(baseline_results)} statements on baseline "
            f"but {len(candidate_results)} on {candidate_cfg.name}"
        )
    for idx, (baseline_stmt, candidate_stmt) in enumerate(zip(baseline_results, candidate_results)):
        context = (
            f"case={case.id} statement_index={idx} baseline={baseline_cfg.name} "
            f"candidate={candidate_cfg.name}"
        )
        if baseline_stmt.status != candidate_stmt.status:
            raise AssertionError(
                f"{context}: status mismatch -> '{baseline_stmt.status}' vs '{candidate_stmt.status}'"
            )
        if baseline_stmt.rowcount != candidate_stmt.rowcount:
            raise AssertionError(
                f"{context}: rowcount mismatch -> {baseline_stmt.rowcount} vs {candidate_stmt.rowcount}"
            )
        if tuple(baseline_stmt.columns) != tuple(candidate_stmt.columns):
            raise AssertionError(
                f"{context}: column names mismatch -> {baseline_stmt.columns} vs {candidate_stmt.columns}"
            )
        if tuple(baseline_stmt.type_names) != tuple(candidate_stmt.type_names):
            raise AssertionError(
                f"{context}: column type mismatch -> {baseline_stmt.type_names} vs {candidate_stmt.type_names}"
            )
        if tuple(baseline_stmt.rows) != tuple(candidate_stmt.rows):
            raise AssertionError(
                f"{context}: row payload mismatch\n"
                f"baseline={baseline_stmt.rows}\n"
                f"candidate={candidate_stmt.rows}"
            )


__all__ = [
    "CaseDefinition",
    "ConnectionConfig",
    "SuiteConfig",
    "load_suite",
    "run_case",
]
