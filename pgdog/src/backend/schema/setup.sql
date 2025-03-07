-- Schema where we are placing all of our code.
CREATE SCHEMA IF NOT EXISTS pgdog;

GRANT USAGE ON SCHEMA pgdog TO PUBLIC;

-- Table to use with "satisfies_hash_partition".
-- We just need the type to match; everything else
-- is passed as an argument to the function.
CREATE TABLE IF NOT EXISTS pgdog.validator_bigint (id BIGSERIAL NOT NULL PRIMARY KEY)
PARTITION BY
    HASH (id);

-- Allow anyone to get next sequence value.
GRANT USAGE ON SEQUENCE pgdog.validator_bigint_id_seq TO PUBLIC;

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

-- Allow anyone to get IDs.
GRANT EXECUTE ON FUNCTION pgdog.next_id(INTEGER, INTEGER) TO PUBLIC;

-- Install the sharded sequence on a table and column.
CREATE OR REPLACE FUNCTION pgdog.install_next_id(
    schema_name text,
    table_name text,
    column_name text,
    shards INTEGER,
    shard INTEGER
) RETURNS TEXT AS $body$
DECLARE max_id BIGINT;
DECLARE current_id BIGINT;
BEGIN
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
