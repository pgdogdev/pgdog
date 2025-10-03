SELECT
      n.nspname AS schema_name,
      c.relname AS table_name,
      i.relname AS index_name,
      idx.indisvalid
  FROM
      pg_index idx
      JOIN pg_class i ON i.oid = idx.indexrelid
      JOIN pg_class c ON c.oid = idx.indrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE
      i.relname = $1;

  Or if you want to include the schema name:

  SELECT
      n.nspname AS schema_name,
      c.relname AS table_name,
      i.relname AS index_name,
      idx.indisvalid
  FROM
      pg_index idx
      JOIN pg_class i ON i.oid = idx.indexrelid
      JOIN pg_class c ON c.oid = idx.indrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE
      i.relname = $1
      AND n.nspname = $2;
