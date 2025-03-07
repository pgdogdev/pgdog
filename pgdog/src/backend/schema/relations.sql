 SELECT n.nspname                                                 AS "schema",
       c.relname                                                  AS "name",
       CASE c.relkind
         WHEN 'r' THEN 'table'
         WHEN 'v' THEN 'view'
         WHEN 'm' THEN 'materialized view'
         WHEN 'i' THEN 'index'
         WHEN 'S' THEN 'sequence'
         WHEN 't' THEN 'TOAST table'
         WHEN 'f' THEN 'foreign table'
         WHEN 'p' THEN 'partitioned table'
         WHEN 'I' THEN 'partitioned index'
       end                                                        AS "type",
       pg_catalog.pg_get_userbyid(c.relowner)                     AS "owner",
       CASE c.relpersistence
         WHEN 'p' THEN 'permanent'
         WHEN 't' THEN 'temporary'
         WHEN 'u' THEN 'unlogged'
       end                                                        AS "persistence",
       am.amname                                                  AS "access_method",
       pg_catalog.pg_table_size(c.oid)                            AS "size",
       pg_catalog.obj_description(c.oid, 'pg_class')              AS "description",
       c.oid::integer                                             AS "oid"
FROM   pg_catalog.pg_class c
       LEFT JOIN pg_catalog.pg_namespace n
              ON n.oid = c.relnamespace
       LEFT JOIN pg_catalog.pg_am am
              ON am.oid = c.relam
WHERE  c.relkind IN ( 'r', 'p', 'v', 'm',
                      'S', 'f', '' )
       AND n.nspname <> 'pg_catalog'
       AND n.nspname !~ '^pg_toast'
       AND n.nspname <> 'information_schema'
       -- AND pg_catalog.pg_table_is_visible(c.oid)
ORDER  BY 1,
          2;
