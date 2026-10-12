-- Schema where we are placing all of our code.
CREATE SCHEMA IF NOT EXISTS pgdog;
CREATE SCHEMA IF NOT EXISTS pgdog_shadow;

GRANT USAGE ON SCHEMA pgdog TO PUBLIC;
GRANT USAGE ON SCHEMA pgdog_shadow TO PUBLIC;

-- Settings table.
CREATE TABLE IF NOT EXISTS pgdog.config (
    shard INTEGER NOT NULL,
    shards INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(shard, shards)
);

CREATE OR REPLACE FUNCTION pgdog.config_trigger() RETURNS trigger AS $body$
DECLARE count BIGINT;
BEGIN
    SELECT count(*) INTO count FROM pgdog.config;

    IF count::bigint = 1::bigint THEN
        RAISE EXCEPTION 'There can only be one pgdog.config row.';
    END IF;

    RETURN NEW;
END;
$body$
LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER config_trigger BEFORE INSERT ON pgdog.config
FOR EACH ROW
EXECUTE FUNCTION pgdog.config_trigger();

GRANT SELECT ON TABLE pgdog.config TO PUBLIC;

-- Table to use with "satisfies_hash_partition".
-- We just need the type to match; everything else
-- is passed as an argument to the function.
CREATE TABLE IF NOT EXISTS pgdog.validator_bigint (id BIGSERIAL NOT NULL PRIMARY KEY)
PARTITION BY
    HASH (id);

-- Table to use with "satisfies_hash_partition".
-- We just need the type to match; everything else
-- is passed as an argument to the function.
CREATE TABLE IF NOT EXISTS pgdog.validator_uuid (id UUID NOT NULL PRIMARY KEY)
PARTITION BY
    HASH (id);

-- Allow anyone to get next sequence value.
GRANT USAGE ON SEQUENCE pgdog.validator_bigint_id_seq TO PUBLIC;

-- Generate a primary key from a sequence that will
-- match the shard number this is ran on.
CREATE OR REPLACE FUNCTION pgdog.next_id_seq(
     sequence_name regclass,
     table_name regclass default 'pgdog.validator_bigint'::regclass
) RETURNS BIGINT AS $body$
DECLARE next_value BIGINT;
DECLARE seq_oid oid;
DECLARE table_oid oid;
DECLARE shards INTEGER;
DECLARE shard INTEGER;
BEGIN
    SELECT sequence_name INTO seq_oid;
    SELECT table_name INTO table_oid;
    SELECT
        pgdog.config.shard,
        pgdog.config.shards
    INTO shard, shards
    FROM pgdog.config;

    IF shards IS NULL OR shard IS NULL THEN
        RAISE EXCEPTION 'pgdog.config not set';
    END IF;

    LOOP
        -- This is atomic.
        SELECT nextval(seq_oid) INTO next_value;

        IF satisfies_hash_partition(table_oid, shards, shard, next_value) THEN
            RETURN next_value;
        END IF;
    END LOOP;
END;
$body$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgdog.next_id_auto() RETURNS BIGINT AS $body$
BEGIN
    RETURN pgdog.next_id_seq('pgdog.validator_bigint_id_seq'::regclass);
END;
$body$ LANGUAGE plpgsql;

-- Key the DDL written to the WAL is signed with. Only pgdog.log_ddl_to_wal() and
-- the owner of this table can read it, so no other role can write DDL to replay.
CREATE TABLE IF NOT EXISTS pgdog.ddl_replication_key (
    id BOOLEAN PRIMARY KEY DEFAULT true CHECK (id),
    key TEXT NOT NULL
);

REVOKE ALL ON TABLE pgdog.ddl_replication_key FROM PUBLIC;

INSERT INTO pgdog.ddl_replication_key (key)
VALUES (replace(gen_random_uuid()::text || gen_random_uuid()::text, '-', ''))
ON CONFLICT DO NOTHING;

-- Saves the session's search_path for pgdog.log_ddl_to_wal(), which runs with its own.
CREATE OR REPLACE FUNCTION pgdog.save_ddl_search_path() RETURNS event_trigger AS $body$
BEGIN
    PERFORM pg_catalog.set_config('pgdog.ddl_search_path', pg_catalog.current_setting('search_path'), true);
END;
$body$ LANGUAGE plpgsql;

-- Writes DDL to the WAL, signed, for PgDog to replay where it replicates to.
--
-- Runs as its owner to read the signing key, with a search_path
-- the calling role can't use to replace what it calls.
CREATE OR REPLACE FUNCTION pgdog.log_ddl_to_wal() RETURNS event_trigger
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $body$
DECLARE
    query TEXT := current_query();
    rebuilt TEXT;
    -- What the triggers of the events before this one found out about the command.
    temp_drop BOOLEAN := coalesce(current_setting('pgdog.temp_drop', true), '') = 'on';
    drop_index TEXT := coalesce(current_setting('pgdog.ddl_drop_index', true), '');
    rewritten BOOLEAN := coalesce(current_setting('pgdog.ddl_rewrite', true), '') = 'on';
    defaults JSON := '[]';
    row_defaults JSON := '[]';
    key BYTEA;
    payload BYTEA;
BEGIN
    -- It's about this command only.
    PERFORM set_config('pgdog.temp_drop', 'off', true),
            set_config('pgdog.ddl_drop_index', '', true),
            set_config('pgdog.ddl_rewrite', 'off', true);

    -- Maintenance of the source's own indexes, and temporary
    -- objects, which only exist in the session that made them.
    IF TG_TAG = 'REINDEX' OR temp_drop THEN
        RETURN;
    END IF;

    -- Nothing reads the message unless the database is being replicated.
    IF NOT EXISTS (
        SELECT 1 FROM pg_replication_slots
        WHERE plugin = 'pgoutput' AND database = current_database()
    ) THEN
        RETURN;
    END IF;

    IF (SELECT bool_and(schema_name = 'pg_temp') FROM pg_event_trigger_ddl_commands()) THEN
        RETURN;
    END IF;

    SELECT convert_to(k.key, 'UTF8') INTO key FROM pgdog.ddl_replication_key k;
    IF key IS NULL THEN
        RETURN;
    END IF;

    -- Indexes are made and dropped by what the catalog says, not by the query:
    -- CONCURRENTLY can't run in the replicated transaction, and the
    -- destination has no traffic to stay out of the way of.
    --
    -- TODO: ALTER TABLE ... DETACH PARTITION ... CONCURRENTLY can't run there either.
    IF TG_TAG = 'CREATE INDEX' THEN
        -- IF NOT EXISTS, and it did.
        IF NOT EXISTS (SELECT 1 FROM pg_event_trigger_ddl_commands()) THEN
            RETURN;
        END IF;

        -- The index of a partitioned table can't be made CONCURRENTLY,
        -- and its definition leaves the partitions out.
        SELECT pg_get_indexdef(c.objid) INTO rebuilt
        FROM pg_event_trigger_ddl_commands() c
        JOIN pg_index i ON i.indexrelid = c.objid
        JOIN pg_class t ON t.oid = i.indrelid AND t.relkind <> 'p';

        query := coalesce(rebuilt, query);
    ELSIF TG_TAG = 'DROP INDEX' THEN
        -- IF EXISTS, and it didn't.
        IF drop_index = '' THEN
            RETURN;
        END IF;

        query := drop_index;
    END IF;

    -- Columns added with a default that isn't the same when the destination computes
    -- it again, for the rows it has: one computed once for all of them, which the
    -- catalog keeps, or for each of them, which rewrites the table.
    --
    -- TODO: a column added in a subtransaction isn't found by the transaction's ID.
    IF TG_TAG = 'ALTER TABLE' THEN
        SELECT
            coalesce(json_agg(c.name) FILTER (WHERE a.atthasmissing), '[]'),
            coalesce(json_agg(c.name) FILTER (WHERE NOT a.atthasmissing), '[]')
        INTO defaults, row_defaults
        FROM pg_attribute a
        LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum,
        LATERAL (
            SELECT json_build_object('table', a.attrelid::regclass::text, 'column', a.attname)
        ) c (name)
        WHERE a.attrelid IN (
                SELECT objid FROM pg_event_trigger_ddl_commands() WHERE object_type = 'table'
            )
            AND a.attgenerated = ''
            AND a.xmin = pg_current_xact_id()::xid
            AND (
                -- Numbered by a sequence, in the order the rows are stored in.
                (rewritten AND a.attidentity <> '')
                OR (
                    (a.atthasmissing OR (rewritten AND d.xmin = pg_current_xact_id()::xid))
                    AND (
                        d.adbin::text ~ 'SQLVALUEFUNCTION|NEXTVALUEEXPR'
                        OR EXISTS (
                            SELECT 1
                            FROM regexp_matches(d.adbin::text, ':(?:funcid|opfuncid) (\d+)', 'g') f
                            JOIN pg_proc p ON p.oid = f[1]::oid AND p.provolatile <> 'i'
                        )
                    )
                )
            );
    END IF;

    payload := convert_to(json_build_object(
        -- The role the DDL ran as: this function runs as its owner.
        'role', coalesce(nullif(current_setting('role'), 'none'), session_user),
        'search_path', coalesce(current_setting('pgdog.ddl_search_path', true), '"$user", public'),
        'query', query,
        'defaults', defaults,
        'row_defaults', row_defaults
    )::text, 'UTF8');

    -- The signature, in hex, comes before the DDL it signs.
    PERFORM pg_logical_emit_message(true, 'ddl_replication',
        convert_to(encode(sha256(key || sha256(key || payload)), 'hex'), 'UTF8') || payload);
END;
$body$ LANGUAGE plpgsql;

-- Dropped objects aren't listed at the end of the command, so pass
-- what pgdog.log_ddl_to_wal() needs to know about them.
CREATE OR REPLACE FUNCTION pgdog.mark_temp_drop() RETURNS event_trigger AS $body$
BEGIN
    -- Temporary objects, for it to skip.
    IF (SELECT bool_and(is_temporary) FROM pg_event_trigger_dropped_objects() WHERE original) THEN
        PERFORM set_config('pgdog.temp_drop', 'on', true);
    END IF;

    -- Indexes, for it to drop by name. With CASCADE, other objects go with them.
    IF TG_TAG = 'DROP INDEX' THEN
        PERFORM set_config('pgdog.ddl_drop_index', (
            SELECT CASE WHEN bool_and(object_type = 'index')
                THEN 'DROP INDEX ' || string_agg(object_identity, ', ') FILTER (WHERE original)
                ELSE current_query()
            END
            FROM pg_event_trigger_dropped_objects()
        ), true);
    END IF;
END;
$body$ LANGUAGE plpgsql;

-- A table is rewritten when a column is added with a default computed for each
-- of its rows, like random(). Flag it for pgdog.log_ddl_to_wal().
CREATE OR REPLACE FUNCTION pgdog.mark_default_rewrite() RETURNS event_trigger AS $body$
BEGIN
    -- 2: the default value of a column changed.
    IF pg_event_trigger_table_rewrite_reason() & 2 <> 0 THEN
        PERFORM set_config('pgdog.ddl_rewrite', 'on', true);
    END IF;
END;
$body$ LANGUAGE plpgsql;

-- Replicate DDL during resharding. Event triggers require superuser;
-- without it, setup still succeeds and DDL is not replicated.
DO $body$
BEGIN
    -- Triggers on the same event fire in the order of their names,
    -- so the search_path is saved before the DDL is logged.
    IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'pgdog_ddl_context') THEN
        CREATE EVENT TRIGGER pgdog_ddl_context ON ddl_command_end
        EXECUTE FUNCTION pgdog.save_ddl_search_path();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'pgdog_ddl_replication') THEN
        CREATE EVENT TRIGGER pgdog_ddl_replication ON ddl_command_end
        EXECUTE FUNCTION pgdog.log_ddl_to_wal();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'pgdog_ddl_rewrite') THEN
        CREATE EVENT TRIGGER pgdog_ddl_rewrite ON table_rewrite
        EXECUTE FUNCTION pgdog.mark_default_rewrite();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'pgdog_ddl_temp_drop') THEN
        CREATE EVENT TRIGGER pgdog_ddl_temp_drop ON sql_drop
        EXECUTE FUNCTION pgdog.mark_temp_drop();
    END IF;
EXCEPTION WHEN insufficient_privilege THEN
    NULL;
END;
$body$;

-- Generate a primary key from a sequence that will
-- match the shard number this is ran on.
CREATE OR REPLACE FUNCTION pgdog.next_uuid_auto() RETURNS UUID AS $body$
DECLARE next_value UUID;
DECLARE table_oid OID;
DECLARE shard INTEGER;
DECLARE shards INTEGER;
BEGIN
    SELECT 'pgdog.validator_uuid'::regclass INTO table_oid;
    SELECT
        pgdog.config.shard,
        pgdog.config.shards
    INTO shard, shards
    FROM pgdog.config;

    LOOP
        SELECT gen_random_uuid() INTO next_value;

        IF satisfies_hash_partition(table_oid, shards, shard, next_value) THEN
            RETURN next_value;
        END IF;
    END LOOP;
END;
$body$ LANGUAGE plpgsql;

-- Generate a primary key from a sequence that will
-- match the shard number this is ran on.
CREATE OR REPLACE FUNCTION pgdog.next_id(shards INTEGER, shard INTEGER) RETURNS BIGINT AS $body$
DECLARE next_value BIGINT;
DECLARE seq_oid oid;
DECLARE table_oid oid;
BEGIN
    SELECT 'pgdog.validator_bigint_id_seq'::regclass INTO seq_oid;
    SELECT 'pgdog.validator_bigint'::regclass INTO table_oid;

    LOOP
        -- This is atomic.
        SELECT nextval(seq_oid) INTO next_value;

        IF satisfies_hash_partition(table_oid, shards, shard, next_value) THEN
            RETURN next_value;
        END IF;
    END LOOP;
END;
$body$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgdog.check_table(schema_name text, table_name text, lock_timeout TEXT DEFAULT '1s')
RETURNS TEXT AS $body$
    BEGIN
        PERFORM format('SET LOCAL lock_timeout TO ''%s''', lock_timeout);
        EXECUTE format('LOCK TABLE "%s"."%s" IN ACCESS EXCLUSIVE MODE', schema_name, table_name);

        RETURN format('"%s"."%s" OK', schema_name, table_name);
    END;
$body$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgdog.check_column(schema_name text, table_name text, column_name text)
RETURNS BOOL AS $body$
DECLARE has_index BOOL;
BEGIN
    EXECUTE format('SELECT COUNT(*) > 0 FROM (
        SELECT
            t.relname AS table_name,
            i.relname AS index_name,
            a.attname AS column_name
        FROM
            pg_class t,
            pg_class i,
            pg_index ix,
            pg_attribute a
        WHERE
            t.oid = ix.indrelid
            AND i.oid = ix.indexrelid
            AND a.attrelid = t.oid
            AND a.attnum = ANY(ix.indkey)
            AND t.relkind = ''r''
            AND t.relname like ''%s''
            AND a.attname = ''%s''
            AND i.relnamespace = ''%s''::regnamespace
        )',
            table_name,
            column_name,
            schema_name
        ) INTO has_index;

    RETURN has_index;
END;
$body$
LANGUAGE plpgsql;

-- Install the sharded sequence on a table and column.
CREATE OR REPLACE FUNCTION pgdog.install_next_id(
    schema_name TEXT,
    table_name TEXT,
    column_name TEXT,
    shards INTEGER,
    shard INTEGER,
    lock_timeout TEXT DEFAULT '1s'
) RETURNS TEXT AS $body$
DECLARE max_id BIGINT;
DECLARE current_id BIGINT;
BEGIN
    -- Check inputs
    EXECUTE format('SELECT "%s" FROM "%s"."%s"  LIMIT 1', column_name, schema_name, table_name);

    IF shards < shard OR shards < 1 OR shard < 0 THEN
        RAISE EXCEPTION 'shards=%, shard=% is an invalid sharding configuration', shards, shard;
    END IF;

    PERFORM pgdog.check_table(schema_name, table_name);

    IF NOT pgdog.check_column(schema_name, table_name, column_name) THEN
        RAISE WARNING 'column is not indexed, this can be very slow';
    END IF;

    -- Lock table to prevent more writes.
    EXECUTE format('LOCK TABLE "%s"."%s" IN ACCESS EXCLUSIVE MODE', schema_name, table_name);

    -- Get the max column value.
    EXECUTE format('SELECT MAX("%s") FROM "%s"."%s"', column_name, schema_name, table_name) INTO max_id;

    -- Get current sequence value.
    SELECT last_value FROM pgdog.validator_bigint_id_seq INTO current_id;

    -- Install the function as the source of IDs.
    EXECUTE format(
        'ALTER TABLE "%s"."%s" ALTER COLUMN "%s" SET DEFAULT pgdog.next_id(%s, %s)',
            schema_name,
            table_name,
            column_name,
            shards::text,
            shard::text
        );

    -- Update the sequence value if it's too low.
    IF current_id < max_id THEN
        PERFORM setval('pgdog.validator_bigint_id_seq'::regclass, max_id);
    END IF;

    RETURN format('pgdog.next_id(%s, %s) installed on table "%s"."%s"',
        shards::text,
        shard::text,
        schema_name,
        table_name
    );
END;
$body$ LANGUAGE plpgsql;

--
-- Create "shadow" table used for primary key generation using the internal sequence.
--
-- This will create the table and the sequence.
--
CREATE OR REPLACE FUNCTION pgdog.install_sharded_sequence(
    schema_name TEXT,
    table_name TEXT,
    column_name TEXT,
    lock_timeout TEXT DEFAULT '1s'
) RETURNS text AS $body$
DECLARE shadow_table_name TEXT;
DECLARE shadow_seq_name TEXT;
BEGIN
    SELECT schema_name || '_' || table_name INTO shadow_table_name;
    SELECT schema_name || '_' || table_name || '_' || column_name || '_seq' INTO shadow_seq_name;

    PERFORM format('SET LOCAL lock_timeout TO ''%s''', lock_timeout);

    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS pgdog_shadow."%s" (LIKE "%s"."%s") PARTITION BY HASH("%s")',
        shadow_table_name,
        schema_name,
        table_name,
        column_name
    );

    -- Create sequence.
    EXECUTE format('CREATE SEQUENCE IF NOT EXISTS pgdog_shadow."%s" CACHE 100', shadow_seq_name);

    -- Make the sequence owned by the shadow table.
    EXECUTE format('ALTER SEQUENCE pgdog_shadow."%s" OWNED BY pgdog_shadow.%s.%s', shadow_seq_name, shadow_table_name, column_name);

    -- Drop identity constraint if one exists, since we're replacing it with a custom default.
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns c
        WHERE c.table_schema = install_sharded_sequence.schema_name
        AND c.table_name = install_sharded_sequence.table_name
        AND c.column_name = install_sharded_sequence.column_name
        AND c.is_identity = 'YES'
    ) THEN
        EXECUTE format('ALTER TABLE "%s"."%s" ALTER COLUMN "%s" DROP IDENTITY', schema_name, table_name, column_name);
    END IF;

    -- Set it as the default for the target table, allowing automatic ID generation.
    EXECUTE format('ALTER TABLE "%s"."%s" ALTER COLUMN "%s" SET DEFAULT pgdog.next_id_seq(''pgdog_shadow.%s''::regclass, ''pgdog_shadow.%s'')',
        schema_name,
        table_name,
        column_name,
        shadow_seq_name,
        shadow_table_name
    );

    RETURN format('"pgdog_shadow"."%s"', shadow_table_name);
END;
$body$ LANGUAGE plpgsql;

-- Install the sharded sequence on a table and column,
-- automatically determining the sequence from the column's default value.
CREATE OR REPLACE FUNCTION pgdog.install_next_id_seq(
    schema_name TEXT,
    table_name TEXT,
    column_name TEXT,
    lock_timeout TEXT DEFAULT '1s'
) RETURNS TEXT AS $body$
DECLARE max_id BIGINT;
DECLARE current_id BIGINT;
DECLARE seq_name TEXT;
DECLARE col_default TEXT;
DECLARE shard INTEGER;
DECLARE shards INTEGER;
BEGIN
    -- Check inputs
    EXECUTE format('SELECT "%s" FROM "%s"."%s" LIMIT 1', column_name, schema_name, table_name);

    -- Get shard configuration.
    SELECT
        pgdog.config.shard,
        pgdog.config.shards
    INTO shard, shards
    FROM pgdog.config;

    IF shards IS NULL OR shard IS NULL THEN
        RAISE EXCEPTION 'pgdog.config not set';
    END IF;

    -- Extract the sequence name from the column's default value.
    SELECT pg_get_expr(d.adbin, d.adrelid)
    INTO col_default
    FROM pg_attrdef d
    JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
    WHERE a.attrelid = format('"%s"."%s"', schema_name, table_name)::regclass
      AND a.attname = column_name;

    IF col_default IS NULL THEN
        RAISE EXCEPTION 'column "%" on table "%"."%" has no default value', column_name, schema_name, table_name;
    END IF;

    -- Extract sequence name from nextval('sequence_name'::regclass).
    SELECT substring(col_default FROM 'nextval\(''([^'']+)''')
    INTO seq_name;

    IF seq_name IS NULL THEN
        RAISE EXCEPTION 'could not extract sequence name from default: %', col_default;
    END IF;

    PERFORM pgdog.check_table(schema_name, table_name);

    IF NOT pgdog.check_column(schema_name, table_name, column_name) THEN
        RAISE WARNING 'column is not indexed, this can be very slow';
    END IF;

    -- Lock table to prevent more writes.
    EXECUTE format('LOCK TABLE "%s"."%s" IN ACCESS EXCLUSIVE MODE', schema_name, table_name);

    -- Get the max column value.
    EXECUTE format('SELECT MAX("%s") FROM "%s"."%s"', column_name, schema_name, table_name) INTO max_id;

    -- Get current sequence value.
    EXECUTE format('SELECT last_value FROM %s', seq_name) INTO current_id;

    -- Install the function as the source of IDs.
    EXECUTE format(
        'ALTER TABLE "%s"."%s" ALTER COLUMN "%s" SET DEFAULT pgdog.next_id_seq(''%s''::regclass)',
            schema_name,
            table_name,
            column_name,
            seq_name
        );

    -- Update the sequence value if it's too low.
    IF current_id < max_id THEN
        PERFORM setval(seq_name::regclass, max_id);
    END IF;

    RETURN format('pgdog.next_id_seq(''%s'') installed on table "%s"."%s"',
        seq_name,
        schema_name,
        table_name
    );
END;
$body$ LANGUAGE plpgsql;

-- Install trigger protecting the sharded column from bad inserts/updates.
CREATE OR REPLACE FUNCTION pgdog.install_trigger(
    schema_name text,
    table_name text,
    column_name text,
    shards INTEGER,
    shard INTEGER
) RETURNS TEXT AS $body$
DECLARE trigger_name TEXT;
DECLARE function_name TEXT;
DECLARE fq_table_name TEXT;
BEGIN
    SELECT format('"pgdog_%s"', table_name) INTO trigger_name;
    SELECT format('"pgdog"."tr_%s_%s"', schema_name, table_name) INTO function_name;
    SELECT format('"%s"."%s"', schema_name, table_name) INTO fq_table_name;

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION %s() RETURNS trigger AS $body2$
            BEGIN
                IF satisfies_hash_partition(''pgdog.validator_bigint''::regclass, %s, %s, NEW."%s") THEN
                    RETURN NEW;
                END IF;

                RETURN NULL;
            END;
        $body2$ LANGUAGE plpgsql',
        function_name,
        shards::text,
        shard::text,
        column_name
    );

    EXECUTE format('CREATE OR REPLACE TRIGGER
        %s BEFORE INSERT OR UPDATE ON %s
        FOR EACH ROW EXECUTE FUNCTION %s()',
            trigger_name,
            fq_table_name,
            function_name
        );

    EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER %s', fq_table_name, trigger_name);

    RETURN format('%s installed on table %s', trigger_name, fq_table_name);
END;
$body$ LANGUAGE plpgsql;

-- Debugging information.
CREATE OR REPLACE FUNCTION pgdog.debug() RETURNS TEXT
AS $body$
DECLARE result TEXT;
DECLARE i TEXT;
DECLARE tmp TEXT;
BEGIN
    SELECT CONCAT('PgDog Debugging', E'\n----------------\n\n') INTO result;
    FOREACH i IN ARRAY '{''next_id'', ''install_next_id'', ''check_column'', ''check_table''}'::text[] LOOP
        EXECUTE format('
            SELECT prosrc
            FROM pg_proc
            WHERE proname = %s
            AND pronamespace = ''pgdog''::regnamespace
        ', i) INTO tmp;
        SELECT CONCAT(result, format('-- Function: pgdog.%s', i), E'\n', tmp, E'\n--\n\n') INTO result;
    END LOOP;
    RETURN result;
END;
$body$ LANGUAGE plpgsql;

--- Shard identifier.
CREATE OR REPLACE FUNCTION pgdog.install_shard_id(shard INTEGER) RETURNS TEXT
AS $body$
BEGIN
    EXECUTE format('CREATE OR REPLACE FUNCTION pgdog.shard_id() RETURNS INTEGER AS
    $body2$
    BEGIN
        RETURN %s::integer;
    END;
    $body2$
    LANGUAGE plpgsql', shard);

    RETURN format('installed on shard %s', shard);
END;
$body$ LANGUAGE plpgsql;
