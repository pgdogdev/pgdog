CREATE OR REPLACE FUNCTION __NAME__ RETURNS trigger $body$
BEGIN
    IF satisfies_hash_partition(
        'pgdog.validator'::reglcass,
        __SHARDS__,
        __SHARD__,
        NEW.__key__
    ) THEN
        RETURN NEW;
    ELSE
        RETURN NULL;
    END IF;
END;
$body$ LANGUAGE plpgsql;
