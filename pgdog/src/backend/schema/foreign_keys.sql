SELECT DISTINCT
    kcu.table_schema::text AS source_schema,
    kcu.table_name::text AS source_table,
    kcu.column_name::text AS source_column,
    ref_kcu.table_schema::text AS ref_schema,
    ref_kcu.table_name::text AS ref_table,
    ref_kcu.column_name::text AS ref_column,
    rc.delete_rule::text AS on_delete,
    rc.update_rule::text AS on_update
FROM
    information_schema.table_constraints tc
JOIN
    information_schema.key_column_usage kcu
    ON tc.constraint_catalog = kcu.constraint_catalog
    AND tc.constraint_schema = kcu.constraint_schema
    AND tc.constraint_name = kcu.constraint_name
JOIN
    information_schema.referential_constraints rc
    ON tc.constraint_catalog = rc.constraint_catalog
    AND tc.constraint_schema = rc.constraint_schema
    AND tc.constraint_name = rc.constraint_name
JOIN
    information_schema.key_column_usage ref_kcu
    ON rc.unique_constraint_catalog = ref_kcu.constraint_catalog
    AND rc.unique_constraint_schema = ref_kcu.constraint_schema
    AND rc.unique_constraint_name = ref_kcu.constraint_name
    AND kcu.position_in_unique_constraint = ref_kcu.ordinal_position
WHERE
    tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY
    source_schema, source_table, source_column;
