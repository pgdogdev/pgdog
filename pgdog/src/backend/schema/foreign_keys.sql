SELECT DISTINCT
      n.nspname::text AS source_schema,
      c.relname::text AS source_table,
      a.attname::text AS source_column,
      rn.nspname::text AS ref_schema,
      rc.relname::text AS ref_table,
      ra.attname::text AS ref_column,
      CASE con.confdeltype
          WHEN 'a' THEN 'NO ACTION'
          WHEN 'r' THEN 'RESTRICT'
          WHEN 'c' THEN 'CASCADE'
          WHEN 'n' THEN 'SET NULL'
          WHEN 'd' THEN 'SET DEFAULT'
      END AS on_delete,
      CASE con.confupdtype
          WHEN 'a' THEN 'NO ACTION'
          WHEN 'r' THEN 'RESTRICT'
          WHEN 'c' THEN 'CASCADE'
          WHEN 'n' THEN 'SET NULL'
          WHEN 'd' THEN 'SET DEFAULT'
      END AS on_update
  FROM pg_constraint con
  JOIN pg_class c ON c.oid = con.conrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_class rc ON rc.oid = con.confrelid
  JOIN pg_namespace rn ON rn.oid = rc.relnamespace
  JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS src(attnum, ord) ON true
  JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS dst(attnum, ord) ON src.ord = dst.ord
  JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = src.attnum
  JOIN pg_attribute ra ON ra.attrelid = con.confrelid AND ra.attnum = dst.attnum
  WHERE con.contype = 'f'
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  ORDER BY source_schema, source_table, source_column;
