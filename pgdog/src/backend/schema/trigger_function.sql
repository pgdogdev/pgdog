CREATE OR REPLACE FUNCTION __NAME__ RETURNS trigger $body$
BEGIN
    IF satisfies_hash_partition(
        'pgdog.validator_bigint'::reglcass,
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

CREATE OR REPLACE TRIGGER __NAME__
BEFORE INSERT ON __TABLE__
FOR EACH ROW EXECUTE __NAME__;
