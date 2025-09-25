from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import psycopg
import psycopg.rows
import sqlparse
import yaml


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
class PairConfig:
    name: str
    baseline: str
    candidate: str
    tags: frozenset[str]


@dataclass(frozen=True)
class CaseDefinition:
    id: str
    description: str
    path: Path
    setup: Path | None
    teardown: Path | None
    tags: frozenset[str]
    transactional: bool


@dataclass(frozen=True)
class CasePair:
    case: CaseDefinition
    pair: PairConfig


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
    targets: dict[str, ConnectionConfig]
    pairs: List[PairConfig]
    cases: List[CaseDefinition]
    global_setup: Path | None
    global_teardown: Path | None

    def lookup_target(self, name: str) -> ConnectionConfig:
        try:
            return self.targets[name]
        except KeyError as exc:
            raise KeyError(f"unknown connection target '{name}'") from exc


def load_suite(root: Path | None = None) -> SuiteConfig:
    root = root or Path(__file__).resolve().parent
    config = _load_config(root / "config.yaml")
    cases = discover_cases(root / "cases")
    global_setup = root / "global_setup.sql"
    if not global_setup.exists():
        global_setup = None
    global_teardown = root / "global_teardown.sql"
    if not global_teardown.exists():
        global_teardown = None
    return SuiteConfig(
        root=root,
        targets=config["targets"],
        pairs=config["pairs"],
        cases=cases,
        global_setup=global_setup,
        global_teardown=global_teardown,
    )


def _load_config(path: Path) -> dict:
    data = yaml.safe_load(path.read_text())
    targets = {
        name: ConnectionConfig(
            name=name,
            dsn=body["dsn"],
            format=body.get("format", "text"),
            tags=frozenset(body.get("tags", []) or []),
        )
        for name, body in (data.get("targets") or {}).items()
    }
    pairs = [
        PairConfig(
            name=item["name"],
            baseline=item["baseline"],
            candidate=item["candidate"],
            tags=frozenset(item.get("tags", []) or []),
        )
        for item in data.get("pairs", [])
    ]
    return {"targets": targets, "pairs": pairs}


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
            )
        )
    return cases


def build_matrix(suite: SuiteConfig) -> List[CasePair]:
    matrix: List[CasePair] = []
    for case in suite.cases:
        for pair in suite.pairs:
            if pair.tags and not pair.tags.issubset(case.tags):
                continue
            matrix.append(CasePair(case=case, pair=pair))
    return matrix


def parse_case_metadata(path: Path) -> CaseMetadata:
    tags: List[str] = []
    description = ""
    transactional = True
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
            cleaned = value.replace(",", " ")
            tokens = [token.strip().lower() for token in cleaned.split() if token.strip()]
            tags = tokens
        elif key_lower == "description":
            description = value
        elif key_lower == "transactional":
            transactional = value.lower() not in {"false", "0", "no"}
    if not tags:
        tags = ["standard"]
    return CaseMetadata(description=description, tags=frozenset(tags), transactional=transactional)


def strip_metadata_header(text: str) -> str:
    metadata_keys = {"tags", "description", "transactional"}
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


def execute_sql_file(target: ConnectionConfig, path: Path | None) -> None:
    if path is None:
        return
    if not path.exists():
        return
    statements = parse_sql_file(path)
    if not statements:
        return
    conn = target.connect()
    try:
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
        conn.commit()
    finally:
        conn.close()


def run_case(
    suite: SuiteConfig,
    case: CaseDefinition,
    baseline_cfg: ConnectionConfig,
    candidate_cfg: ConnectionConfig,
) -> None:
    if suite.global_setup:
        execute_sql_file(baseline_cfg, suite.global_setup)
    execute_sql_file(baseline_cfg, case.setup)
    baseline_results = _collect_results(case, baseline_cfg)
    candidate_results = _collect_results(case, candidate_cfg)
    try:
        _assert_equal(case, baseline_cfg, candidate_cfg, baseline_results, candidate_results)
    finally:
        execute_sql_file(baseline_cfg, case.teardown)
        if suite.global_teardown:
            execute_sql_file(baseline_cfg, suite.global_teardown)


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


def _assert_equal(
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
    "CasePair",
    "ConnectionConfig",
    "SuiteConfig",
    "build_matrix",
    "load_suite",
    "run_case",
]
