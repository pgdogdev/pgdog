-- Benchmark schema setup: bench_copy
--
-- Creates schema + tables on SOURCE databases (pgdog1/pgdog2/pgdog3).
-- Run once at benchmark start; safe to re-run (DROP IF EXISTS).
--
-- Tables exercise two distinct copy paths:
--   sessions   : UUID/TIMESTAMPTZ-heavy, fully inline -- shows binary format benefit.
--   documents  : body ~32 KB (STORAGE EXTERNAL) -- TOAST read path.
--   files      : omni (no tenant_id) -- 4 KB TOAST bytea, copied to all shards.
--   ledger     : notes ~16 KB (STORAGE EXTERNAL) + NUMERIC.
--
-- The destination schema is created by pgdog schema-sync, not here.

DROP SCHEMA IF EXISTS bench_copy CASCADE;
CREATE SCHEMA bench_copy;

-- ── 1. sessions ───────────────────────────────────────────────────────────────
-- UUID/TIMESTAMPTZ-heavy, zero TOAST. Best-case for binary format:
-- 8 UUID cols × 20 B + 4 TIMESTAMPTZ × 21 B = 244 B/row less on wire vs text.
CREATE TABLE bench_copy.sessions (
    id              UUID        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    tenant_id       BIGINT      NOT NULL,
    user_id         UUID        NOT NULL,
    device_id       UUID        NOT NULL,
    session_id      UUID        NOT NULL,
    correlation_id  UUID        NOT NULL,
    request_id      UUID        NOT NULL,
    trace_id        UUID        NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    ended_at        TIMESTAMPTZ NOT NULL,
    last_seen_at    TIMESTAMPTZ NOT NULL,
    expires_at      TIMESTAMPTZ NOT NULL,
    duration_ms     FLOAT8      NOT NULL DEFAULT 0.0,
    score           FLOAT8      NOT NULL DEFAULT 0.0,
    event_count     BIGINT      NOT NULL DEFAULT 0,
    byte_count      BIGINT      NOT NULL DEFAULT 0
);

-- ── 2. documents ──────────────────────────────────────────────────────────────
-- body ~32 KB forced out of the main heap (STORAGE EXTERNAL).
CREATE TABLE bench_copy.documents (
    id          UUID        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    tenant_id   BIGINT      NOT NULL,
    title       TEXT        NOT NULL,
    body        TEXT        NOT NULL,
    tags        TEXT[]      NOT NULL DEFAULT '{}',
    metadata    JSONB       NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE bench_copy.documents ALTER COLUMN body SET STORAGE EXTERNAL;

-- ── 3. files (omni) ─────────────────────────────────────────────────────────────
-- No tenant_id: copied to every destination shard. content ~4 KB bytea TOAST.
CREATE TABLE bench_copy.files (
    id           BIGINT      NOT NULL PRIMARY KEY,
    name         TEXT        NOT NULL,
    mime_type    TEXT        NOT NULL DEFAULT 'application/octet-stream',
    content      BYTEA       NOT NULL,
    size_bytes   BIGINT      NOT NULL DEFAULT 0,
    checksum     TEXT        NOT NULL DEFAULT '',
    uploaded_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── 4. ledger ─────────────────────────────────────────────────────────────────
-- notes ~16 KB (STORAGE EXTERNAL). NUMERIC columns for exact financials.
CREATE TABLE bench_copy.ledger (
    id            UUID          NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    tenant_id     BIGINT        NOT NULL,
    ref_code      TEXT          NOT NULL,
    amount        NUMERIC(18,6) NOT NULL DEFAULT 0,
    rate          FLOAT8        NOT NULL DEFAULT 1.0,
    currency      TEXT          NOT NULL DEFAULT 'USD',
    notes         TEXT          NOT NULL DEFAULT '',
    checkpoints   TIMESTAMPTZ[] NOT NULL DEFAULT '{}',
    posted_at     TIMESTAMPTZ   NOT NULL DEFAULT now()
);
ALTER TABLE bench_copy.ledger ALTER COLUMN notes SET STORAGE EXTERNAL;

-- ── Publication ───────────────────────────────────────────────────────────────
DROP PUBLICATION IF EXISTS bench_copy;
CREATE PUBLICATION bench_copy FOR TABLES IN SCHEMA bench_copy;
