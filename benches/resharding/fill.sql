-- Benchmark data generator: bench_copy
--
-- Four data shapes in one file:
--
--   sessions   : scale rows — UUID/TIMESTAMPTZ-heavy, fully inline.
--                Best-case for binary format: 8 UUID×20B + 4 TS×21B = 244 B/row
--                saved vs text, on both source routing and destination ingest.
--
--   documents  : scale/100 rows — body ≈ 32 KB (STORAGE EXTERNAL).
--   files      : scale/100 rows — content ≈ 4 KB bytea (TOAST).
--   ledger     : scale/100 rows — notes ≈ 16 KB (STORAGE EXTERNAL).
--
-- The TOAST tables share their row count so a single BENCH_SCALE knob controls
-- everything: inline rows = scale, TOAST rows = scale/100.
--
-- Override default scale: BENCH_SCALE=N psql ... -f fill.sql
-- Shard slice: pass -v num_shards=N -v shard_index=I to insert only the
--   rows that belong on shard I.  Defaults to a single shard (all rows).
\getenv scale BENCH_SCALE
\if :{?scale}
\else
\set scale 100000
\endif
\if :{?num_shards}
\else
\set num_shards 1
\endif
\if :{?shard_index}
\else
\set shard_index 0
\endif
-- ── sessions (inline, UUID + TIMESTAMPTZ heavy) ────────────────────────────────
INSERT INTO bench_copy.sessions (
    id, tenant_id, user_id, device_id, session_id, correlation_id,
    request_id, trace_id,
    started_at, ended_at, last_seen_at, expires_at,
    duration_ms, score, event_count, byte_count
)
SELECT
    gen_random_uuid(),
    gs.i,
    md5((gs.i % 100000)::text)::uuid,
    md5((gs.i % 10000)::text)::uuid,
    gen_random_uuid(),
    gen_random_uuid(),
    gen_random_uuid(),
    md5(gs.i::text)::uuid,
    now() - ((gs.i % 365) || ' days')::interval,
    now() - ((gs.i % 365) || ' days')::interval + '1 hour'::interval,
    now() - ((gs.i % 7)   || ' days')::interval,
    now() + ((gs.i % 30)  || ' days')::interval,
    ROUND((0.1 + (gs.i % 60000) * 0.001)::numeric, 3)::float8,
    ROUND((gs.i % 1000)::numeric / 1000.0, 6)::float8,
    (gs.i % 10000),
    (gs.i % 1000000)::bigint * 1024
FROM generate_series(:shard_index + 1, :scale, :num_shards) AS gs(i);

-- ── documents (body ≈ 32 KB, STORAGE EXTERNAL) ────────────────────────────────
INSERT INTO bench_copy.documents (id, tenant_id, title, body, tags, metadata, created_at, updated_at)
SELECT
    gen_random_uuid(),
    gs.i,
    format('doc_title_%s', gs.i),
    repeat(md5(gs.i::text), 1024),
    ARRAY[
        'tag_' || ((gs.i % 20) + 1)::text,
        'cat_' || ((gs.i % 8)  + 1)::text,
        CASE WHEN gs.i % 3 = 0 THEN 'featured' ELSE 'standard' END
    ],
    jsonb_build_object(
        'version',    (gs.i % 10) + 1,
        'published',  (gs.i % 2 = 0),
        'word_count', 500 + (gs.i % 2000)
    ),
    now() - ((gs.i % 730) || ' days')::interval,
    now() - ((gs.i % 365) || ' days')::interval
FROM generate_series(:shard_index + 1, :scale / 100, :num_shards) AS gs(i);

-- files (omni, no tenant_id) -- copied to every destination shard. content ~4 KB TOAST.
INSERT INTO bench_copy.files (id, name, mime_type, content, size_bytes, checksum, uploaded_at)
SELECT
    gs.i,
    format('file_%s.bin', gs.i),
    (ARRAY['application/octet-stream', 'image/png', 'application/pdf'])[((gs.i % 3) + 1)],
    decode(repeat(md5(gs.i::text), 256), 'hex'),
    4096,
    md5(gs.i::text),
    now() - ((gs.i % 365) || ' days')::interval
FROM generate_series(1, :scale / 100) AS gs(i);

-- ── ledger (notes ≈ 16 KB, STORAGE EXTERNAL) ─────────────────────────────────
INSERT INTO bench_copy.ledger (id, tenant_id, ref_code, amount, rate, currency, notes, checkpoints, posted_at)
SELECT
    gen_random_uuid(),
    gs.i,
    'REF-' || lpad(gs.i::text, 10, '0'),
    ROUND((1.0 + (gs.i % 1000000) * 0.01)::numeric, 6),
    ROUND((0.85 + (gs.i % 50) * 0.003)::numeric, 6)::float8,
    (ARRAY['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF'])[((gs.i % 7) + 1)],
    repeat(md5(('ledger_' || gs.i)::text), 512),
    ARRAY(
        SELECT now() - ((gs.i % 365) || ' days')::interval
                     - ((j * 30) || ' days')::interval
        FROM generate_series(0, (gs.i % 5)) AS j
    ),
    now() - ((gs.i % 365) || ' days')::interval
FROM generate_series(:shard_index + 1, :scale / 100, :num_shards) AS gs(i);
