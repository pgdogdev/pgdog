SELECT
    n.nspname::text AS schema_name,
    c.relname::text AS table_name,
    a.attname::text AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod)::text AS column_type,
    a.attnotnull::text AS is_not_null,
    pg_catalog.pg_get_expr(ad.adbin, ad.adrelid)::text AS column_default,
    a.attgenerated::text AS generated,
    coll.collname::text AS collation_name,
    collnsp.nspname::text AS collation_schema
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON
    c.relnamespace = n.oid
LEFT JOIN pg_catalog.pg_attribute a ON
    a.attrelid = c.oid
    AND a.attnum > 0
    AND NOT a.attisdropped
LEFT JOIN pg_catalog.pg_attrdef ad ON
    ad.adrelid = c.oid
    AND ad.adnum = a.attnum
LEFT JOIN pg_catalog.pg_collation coll ON
    coll.oid = a.attcollation
LEFT JOIN pg_catalog.pg_namespace collnsp ON
    collnsp.oid = coll.collnamespace
WHERE
    c.relkind IN ('r', 'v', 'f', 'm', 'p')
    AND n.nspname <> 'pg_catalog'
    AND n.nspname !~ '^pg_toast'
    AND n.nspname <> 'information_schema'
    AND NOT c.relispartition
ORDER BY
    n.nspname,
    c.relname,
    a.attnum
