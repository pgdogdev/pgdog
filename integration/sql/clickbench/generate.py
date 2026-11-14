#!/usr/bin/env python3
"""Generate ClickBench Citus regression cases for the SQL acceptance suite.

Reads the upstream queries (one per line) from ClickBench/citus/queries.sql
and emits one regression case per query. Each case runs the query against
direct Postgres (baseline) and PgDog routed through the sharded pool, in
both text and binary wire formats. The point is to surface which queries
PgDog returns identical results for and which it does not.

The generated layout under integration/sql/cases/ is:

    clickbench_setup.sql      -- shared CREATE TABLE + seed INSERTs
    clickbench_teardown.sql   -- shared DROP TABLE
    9XX_clickbench_qNN_setup.sql      -> symlink to clickbench_setup.sql
    9XX_clickbench_qNN_case.sql       -- one ClickBench query
    9XX_clickbench_qNN_teardown.sql   -> symlink to clickbench_teardown.sql

The harness ignores files that lack a numeric prefix, so the templates
are not picked up directly.
"""
from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SQL_ROOT = REPO_ROOT / "integration" / "sql"
CASES_DIR = SQL_ROOT / "cases"
QUERIES_SRC = REPO_ROOT.parent / "ClickBench" / "citus" / "queries.sql"

SETUP_TEMPLATE = CASES_DIR / "clickbench_setup.sql"
TEARDOWN_TEMPLATE = CASES_DIR / "clickbench_teardown.sql"

START_INDEX = 940  # leaves room above the existing 9xx range

# (column_name, sql_type) — verbatim from ClickBench/citus/create.sql, minus
# the columnar storage clause and the composite primary key.
COLUMNS: list[tuple[str, str]] = [
    ("WatchID", "BIGINT"),
    ("JavaEnable", "SMALLINT"),
    ("Title", "TEXT"),
    ("GoodEvent", "SMALLINT"),
    ("EventTime", "TIMESTAMP"),
    ("EventDate", "DATE"),
    ("CounterID", "INTEGER"),
    ("ClientIP", "INTEGER"),
    ("RegionID", "INTEGER"),
    ("UserID", "BIGINT"),
    ("CounterClass", "SMALLINT"),
    ("OS", "SMALLINT"),
    ("UserAgent", "SMALLINT"),
    ("URL", "TEXT"),
    ("Referer", "TEXT"),
    ("IsRefresh", "SMALLINT"),
    ("RefererCategoryID", "SMALLINT"),
    ("RefererRegionID", "INTEGER"),
    ("URLCategoryID", "SMALLINT"),
    ("URLRegionID", "INTEGER"),
    ("ResolutionWidth", "SMALLINT"),
    ("ResolutionHeight", "SMALLINT"),
    ("ResolutionDepth", "SMALLINT"),
    ("FlashMajor", "SMALLINT"),
    ("FlashMinor", "SMALLINT"),
    ("FlashMinor2", "TEXT"),
    ("NetMajor", "SMALLINT"),
    ("NetMinor", "SMALLINT"),
    ("UserAgentMajor", "SMALLINT"),
    ("UserAgentMinor", "VARCHAR(255)"),
    ("CookieEnable", "SMALLINT"),
    ("JavascriptEnable", "SMALLINT"),
    ("IsMobile", "SMALLINT"),
    ("MobilePhone", "SMALLINT"),
    ("MobilePhoneModel", "TEXT"),
    ("Params", "TEXT"),
    ("IPNetworkID", "INTEGER"),
    ("TraficSourceID", "SMALLINT"),
    ("SearchEngineID", "SMALLINT"),
    ("SearchPhrase", "TEXT"),
    ("AdvEngineID", "SMALLINT"),
    ("IsArtifical", "SMALLINT"),
    ("WindowClientWidth", "SMALLINT"),
    ("WindowClientHeight", "SMALLINT"),
    ("ClientTimeZone", "SMALLINT"),
    ("ClientEventTime", "TIMESTAMP"),
    ("SilverlightVersion1", "SMALLINT"),
    ("SilverlightVersion2", "SMALLINT"),
    ("SilverlightVersion3", "INTEGER"),
    ("SilverlightVersion4", "SMALLINT"),
    ("PageCharset", "TEXT"),
    ("CodeVersion", "INTEGER"),
    ("IsLink", "SMALLINT"),
    ("IsDownload", "SMALLINT"),
    ("IsNotBounce", "SMALLINT"),
    ("FUniqID", "BIGINT"),
    ("OriginalURL", "TEXT"),
    ("HID", "INTEGER"),
    ("IsOldCounter", "SMALLINT"),
    ("IsEvent", "SMALLINT"),
    ("IsParameter", "SMALLINT"),
    ("DontCountHits", "SMALLINT"),
    ("WithHash", "SMALLINT"),
    ("HitColor", "CHAR"),
    ("LocalEventTime", "TIMESTAMP"),
    ("Age", "SMALLINT"),
    ("Sex", "SMALLINT"),
    ("Income", "SMALLINT"),
    ("Interests", "SMALLINT"),
    ("Robotness", "SMALLINT"),
    ("RemoteIP", "INTEGER"),
    ("WindowName", "INTEGER"),
    ("OpenerName", "INTEGER"),
    ("HistoryLength", "SMALLINT"),
    ("BrowserLanguage", "TEXT"),
    ("BrowserCountry", "TEXT"),
    ("SocialNetwork", "TEXT"),
    ("SocialAction", "TEXT"),
    ("HTTPError", "SMALLINT"),
    ("SendTiming", "INTEGER"),
    ("DNSTiming", "INTEGER"),
    ("ConnectTiming", "INTEGER"),
    ("ResponseStartTiming", "INTEGER"),
    ("ResponseEndTiming", "INTEGER"),
    ("FetchTiming", "INTEGER"),
    ("SocialSourceNetworkID", "SMALLINT"),
    ("SocialSourcePage", "TEXT"),
    ("ParamPrice", "BIGINT"),
    ("ParamOrderID", "TEXT"),
    ("ParamCurrency", "TEXT"),
    ("ParamCurrencyID", "SMALLINT"),
    ("OpenstatServiceName", "TEXT"),
    ("OpenstatCampaignID", "TEXT"),
    ("OpenstatAdID", "TEXT"),
    ("OpenstatSourceID", "TEXT"),
    ("UTMSource", "TEXT"),
    ("UTMMedium", "TEXT"),
    ("UTMCampaign", "TEXT"),
    ("UTMContent", "TEXT"),
    ("UTMTerm", "TEXT"),
    ("FromTag", "TEXT"),
    ("HasGCLID", "SMALLINT"),
    ("RefererHash", "BIGINT"),
    ("URLHash", "BIGINT"),
    ("CLID", "INTEGER"),
]

# Seed rows. Each row overrides only the columns relevant to the query
# filters; all other columns get a deterministic default for their type.
# UserIDs are distinct so the routing key spreads rows across shards.
ROWS: list[dict] = [
    # Q20: WHERE UserID = 435090932899640449
    dict(WatchID=100001, UserID=435090932899640449, CounterID=62,
         EventDate="2013-07-15", EventTime="2013-07-15 10:00:00",
         URL="http://example.com/", SearchPhrase="magic",
         ResolutionWidth=1920, RegionID=1),

    # Q2/Q8: AdvEngineID <> 0; Q21/Q22: URL LIKE '%google%'
    dict(WatchID=100002, UserID=100, CounterID=62,
         EventDate="2013-07-10", EventTime="2013-07-10 11:30:00",
         URL="http://google.com/search?q=test", SearchPhrase="hello world",
         AdvEngineID=1, RegionID=1, ResolutionWidth=1920,
         Title="Search results", IsRefresh=0, DontCountHits=0),

    # Q8: another AdvEngineID
    dict(WatchID=100003, UserID=200, CounterID=62,
         EventDate="2013-07-20", EventTime="2013-07-20 14:45:00",
         URL="http://example.com/page", SearchPhrase="",
         AdvEngineID=2, RegionID=2, ResolutionWidth=1366,
         IsRefresh=0, DontCountHits=0),

    # Q23: Title LIKE '%Google%' AND URL NOT LIKE '%.google.%'
    dict(WatchID=100004, UserID=300, CounterID=42,
         EventDate="2013-08-01", EventTime="2013-08-01 09:00:00",
         URL="http://maps.example.com/", Title="Google Maps directions",
         AdvEngineID=3, SearchPhrase="directions",
         RegionID=3, ResolutionWidth=1024),

    # Q41: RefererHash = 3594120000172545465 AND TraficSourceID IN (-1, 6)
    dict(WatchID=100005, UserID=400, CounterID=62,
         EventDate="2013-07-15", EventTime="2013-07-15 12:00:00",
         RefererHash=3594120000172545465, TraficSourceID=6,
         URLHash=11111111111111111, ResolutionWidth=1280,
         IsRefresh=0, DontCountHits=0,
         URL="http://example.com/r", Referer="https://www.foo.com/p"),

    # Q42: URLHash = 2868770270353813622
    dict(WatchID=100006, UserID=500, CounterID=62,
         EventDate="2013-07-15", EventTime="2013-07-15 13:00:00",
         URLHash=2868770270353813622,
         WindowClientWidth=1024, WindowClientHeight=768,
         ResolutionWidth=1024, IsRefresh=0, DontCountHits=0,
         URL="http://example.com/u"),

    # Q11/Q12: non-empty MobilePhoneModel
    dict(WatchID=100007, UserID=600, CounterID=62,
         EventDate="2013-07-12", EventTime="2013-07-12 16:00:00",
         MobilePhoneModel="iPhone", MobilePhone=1,
         RegionID=4, ResolutionWidth=375),
    dict(WatchID=100008, UserID=700, CounterID=62,
         EventDate="2013-07-13", EventTime="2013-07-13 16:30:00",
         MobilePhoneModel="Galaxy", MobilePhone=2,
         SearchEngineID=3, SearchPhrase="news",
         RegionID=4, ResolutionWidth=412),

    # Q13/Q14/Q15: shared SearchPhrase + SearchEngineID for GROUP BY tests
    dict(WatchID=100009, UserID=800, CounterID=62,
         EventDate="2013-07-16", EventTime="2013-07-16 08:00:00",
         SearchEngineID=2, SearchPhrase="weather",
         RegionID=10, ResolutionWidth=1920, ClientIP=12345, IsRefresh=0,
         DontCountHits=0),
    dict(WatchID=100010, UserID=900, CounterID=62,
         EventDate="2013-07-17", EventTime="2013-07-17 08:15:00",
         SearchEngineID=2, SearchPhrase="weather",
         RegionID=10, ResolutionWidth=1920, ClientIP=12345, IsRefresh=0,
         DontCountHits=0),

    # Q31/Q32/Q36: shared ClientIP + diverse IsRefresh
    dict(WatchID=100011, UserID=1000, CounterID=62,
         EventDate="2013-07-18", EventTime="2013-07-18 12:00:00",
         RegionID=20, ClientIP=67890, IsRefresh=0,
         ResolutionWidth=800, SearchEngineID=4, SearchPhrase="news",
         DontCountHits=0),
    dict(WatchID=100012, UserID=1100, CounterID=62,
         EventDate="2013-07-19", EventTime="2013-07-19 12:30:00",
         RegionID=20, ClientIP=67890, IsRefresh=1,
         ResolutionWidth=800, SearchEngineID=4, SearchPhrase="news",
         DontCountHits=0),

    # Q22/Q29: URL LIKE '%google%' + non-empty Referer
    dict(WatchID=100013, UserID=1200, CounterID=62,
         EventDate="2013-07-21", EventTime="2013-07-21 10:00:00",
         URL="http://google.com/maps",
         Referer="https://www.example.com/path/to/resource",
         SearchPhrase="maps", ResolutionWidth=1024,
         IsRefresh=0, DontCountHits=0),

    # Q39: IsLink <> 0 AND IsDownload = 0
    dict(WatchID=100014, UserID=1300, CounterID=62,
         EventDate="2013-07-25", EventTime="2013-07-25 11:00:00",
         URL="http://example.com/download",
         IsLink=1, IsDownload=0, IsRefresh=0,
         ResolutionWidth=1280, DontCountHits=0),

    # Q19/Q43: distinctive minute + DATE_TRUNC date in 2013-07-14..15
    dict(WatchID=100015, UserID=1400, CounterID=62,
         EventDate="2013-07-14", EventTime="2013-07-14 12:34:56",
         ClientEventTime="2013-07-14 12:34:56",
         URL="http://example.com/other", SearchPhrase="test",
         ResolutionWidth=1366, RegionID=30,
         IsRefresh=0, DontCountHits=0),
]


def default_value(sql_type: str):
    if sql_type in ("BIGINT", "SMALLINT", "INTEGER"):
        return 0
    if sql_type in ("TEXT", "VARCHAR(255)", "CHAR"):
        return ""
    if sql_type == "DATE":
        return "1970-01-01"
    if sql_type == "TIMESTAMP":
        return "1970-01-01 00:00:00"
    raise ValueError(f"unhandled type {sql_type}")


def sql_literal(value, sql_type: str) -> str:
    if sql_type in ("BIGINT", "SMALLINT", "INTEGER"):
        return str(int(value))
    if sql_type in ("TEXT", "VARCHAR(255)", "CHAR"):
        s = str(value).replace("'", "''")
        return f"'{s}'"
    if sql_type == "DATE":
        return f"DATE '{value}'"
    if sql_type == "TIMESTAMP":
        return f"TIMESTAMP '{value}'"
    raise ValueError(f"unhandled type {sql_type}")


def render_setup() -> str:
    out: list[str] = ["DROP TABLE IF EXISTS hits;\n", "CREATE TABLE hits (\n"]
    out.append(",\n".join(f"    {n} {t} NOT NULL" for n, t in COLUMNS))
    out.append("\n);\n")
    for row in ROWS:
        col_names = [n for n, _ in COLUMNS]
        values = [sql_literal(row.get(n, default_value(t)), t) for n, t in COLUMNS]
        out.append(
            "INSERT INTO hits ("
            + ", ".join(col_names)
            + ") VALUES ("
            + ", ".join(values)
            + ");\n"
        )
    return "".join(out)


def render_teardown() -> str:
    return "DROP TABLE IF EXISTS hits;\n"


def parse_queries(text: str) -> list[str]:
    queries: list[str] = []
    for raw in text.splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("--"):
            continue
        queries.append(stripped.rstrip(";"))
    return queries


def render_case(query_no: int, sql: str) -> str:
    header = (
        f"-- description: ClickBench Citus query {query_no}\n"
        "-- tags: sharded\n"
        "-- transactional: true\n"
        "-- only-targets: postgres_standard_text pgdog_sharded_text pgdog_sharded_binary\n"
        "\n"
    )
    return header + sql + ";\n"


def force_symlink(link: Path, target_name: str) -> None:
    if link.exists() or link.is_symlink():
        link.unlink()
    os.symlink(target_name, link)


def main() -> None:
    if not QUERIES_SRC.exists():
        raise SystemExit(f"queries.sql not found at {QUERIES_SRC}")
    queries = parse_queries(QUERIES_SRC.read_text())
    if len(queries) != 43:
        raise SystemExit(
            f"expected 43 ClickBench queries, found {len(queries)} in {QUERIES_SRC}"
        )

    SETUP_TEMPLATE.write_text(render_setup())
    TEARDOWN_TEMPLATE.write_text(render_teardown())

    for offset, sql in enumerate(queries):
        query_no = offset + 1
        idx = START_INDEX + offset
        slug = f"clickbench_q{query_no:02d}"
        case_path = CASES_DIR / f"{idx}_{slug}_case.sql"
        setup_link = CASES_DIR / f"{idx}_{slug}_setup.sql"
        teardown_link = CASES_DIR / f"{idx}_{slug}_teardown.sql"

        case_path.write_text(render_case(query_no, sql))
        force_symlink(setup_link, SETUP_TEMPLATE.name)
        force_symlink(teardown_link, TEARDOWN_TEMPLATE.name)

    print(f"wrote {len(queries)} ClickBench cases starting at {START_INDEX}")


if __name__ == "__main__":
    main()
